import os

from aws_cdk import (
    core,
    aws_sns as sns,
    aws_iam,
    aws_stepfunctions as sfn,
    aws_s3_deployment as s3d,
    aws_dynamodb as dynamo
)


from aws_emr_launch.constructs.emr_constructs import emr_code
from aws_emr_launch.constructs.step_functions import emr_chains
from aws_emr_launch.constructs.step_functions import emr_launch_function
from aws_emr_launch.constructs.step_functions import emr_tasks


class StepFunctionStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str,
                 emr_launch_stack,
                 artifact_bucket,
                 output_bucket,
                 **kwargs):
        super().__init__(scope, id, **kwargs)

        launch_function = emr_launch_stack.launch_function

        # Create DynamoDB table for tracking
        dynamo_table = dynamo.Table(
            self,
            "dynamotable",
            partition_key=dynamo.Attribute(name="BatchId", type=dynamo.AttributeType.STRING),
            sort_key=dynamo.Attribute(name="Name", type=dynamo.AttributeType.STRING),
            billing_mode=dynamo.BillingMode.PAY_PER_REQUEST
        )

        emr_role = aws_iam.Role.from_role_arn(
            self,
            "emr_role_iam",
            role_arn=emr_launch_stack.instance_role_arn
        )

        emr_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["dynamodb:*"],
                resources=[dynamo_table.table_arn]
            )
        )

        emr_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["logs:CreateLogStream","logs:DescribeLogStreams","logs:CreateLogGroup","logs:PutLogEvents", "ec2:DescribeTags"],
                resources=["*"]
            )
        )

        # SNS Topics for Success/Failures messages from our Pipeline
        success_topic = sns.Topic(self, 'SuccessTopic')
        failure_topic = sns.Topic(self, 'FailureTopic')

        # Upload artifacts to S3
        step_code = s3d.BucketDeployment(
            self,
            id='sparkscript',
            destination_bucket=artifact_bucket,
            destination_key_prefix='steps',
            sources=[s3d.Source.asset('infrastructure/emr_orchestration/steps/')]
        )

        # Create a Chain to receive Failure messages
        fail = emr_chains.Fail(
            self, 'FailChain',
            message=sfn.TaskInput.from_data_at('$.Error'),
            subject='Pipeline Failure',
            topic=failure_topic
        )

        # # Define a Task to Terminate the Cluster on failure
        terminate_failed_cluster = emr_tasks.TerminateClusterBuilder.build(
            self, 'TerminateFailedCluster',
            name='Terminate Failed Cluster',
            cluster_id=sfn.TaskInput.from_data_at('$.LaunchClusterResult.ClusterId').value,
            result_path='$.TerminateResult').add_catch(fail, errors=['States.ALL'], result_path='$.Error')

        terminate_failed_cluster.next(fail)

        # Use a NestedStateMachine to launch the cluster
        launch_cluster = emr_chains.NestedStateMachine(
            self, 'NestedStateMachine',
            name='Launch Cluster StateMachine',
            state_machine=launch_function.state_machine,
            fail_chain=fail
        )

        pyspark_step = emr_chains.AddStepWithArgumentOverrides(
            self, 'PySparkDataIngestion',
            emr_step=emr_code.EMRStep(
                name=f'Data Ingestion - PySpark Job',
                jar='command-runner.jar',
                args=[
                    'spark-submit',
                    '--master', 'yarn',
                    '--deploy-mode', 'client',
                    '--packages', 'com.audienceproject:spark-dynamodb_2.12:1.1.2',
                    os.path.join(f's3://{artifact_bucket.bucket_name}', 'steps', 'data_ingestion.py'),
                    '--batch-id', 'DynamoDB.BatchId',
                    '--batch-metadata-table-name', dynamo_table.table_name,
                    '--output-bucket', output_bucket.bucket_name,
                    '--region', os.environ["CDK_DEFAULT_REGION"]
                ]
            ),
            cluster_id=sfn.TaskInput.from_data_at('$.LaunchClusterResult.ClusterId').value,
            result_path='$.PySparkResult',
            fail_chain=terminate_failed_cluster
        )


        pyspark_example_step = emr_chains.AddStepWithArgumentOverrides(
            self, 'PySparkDataPreparation',
            emr_step=emr_code.EMRStep(
                name=f'Data Preparation - PySpark Job',
                jar='command-runner.jar',
                args=[
                    'spark-submit',
                    '--master', 'yarn',
                    '--deploy-mode', 'client',
                    os.path.join(f's3://{artifact_bucket.bucket_name}', 'steps', 'data_preparation.py'),
                    '--batch-id', 'DynamoDB.BatchId',
                    '--batch-metadata-table-name', dynamo_table.table_name,
                    '--input-bucket', output_bucket.bucket_name,
                    '--region', os.environ["CDK_DEFAULT_REGION"]
                ]
            ),
            cluster_id=sfn.TaskInput.from_data_at('$.LaunchClusterResult.ClusterId').value,
            result_path='$.PySparkResult',
            fail_chain=terminate_failed_cluster
        )

        # Define a Task to Terminate the Cluster
        terminate_cluster = emr_tasks.TerminateClusterBuilder.build(
            self, 'TerminateCluster',
            name='Terminate Cluster',
            cluster_id=sfn.TaskInput.from_data_at('$.LaunchClusterResult.ClusterId').value,
            result_path='$.TerminateResult'
        ).add_catch(fail, errors=['States.ALL'], result_path='$.Error')

        # A Chain for Success notification when the pipeline completes
        success = emr_chains.Success(
            self, 'SuccessChain',
            message=sfn.TaskInput.from_data_at('$.TerminateResult'),
            subject='Pipeline Succeeded',
            topic=success_topic
        )

        # Assemble the Pipeline
        definition = sfn.Chain \
            .start(launch_cluster) \
            .next(pyspark_step) \
            .next(pyspark_example_step) \
            .next(terminate_cluster) \
            .next(success)

        # Create the State Machine
        self.state_machine = sfn.StateMachine(
            self, 'PySparkExampleStateMachine',
            state_machine_name='pyspark-example-pipeline', definition=definition
        )
        self.dynamo_table = dynamo_table