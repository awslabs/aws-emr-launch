#!/usr/bin/env python3

import os

from aws_cdk import (
    aws_lambda,
    aws_lambda_event_sources as sources,
    aws_iam as iam,
    aws_sns as sns,
    aws_stepfunctions as sfn,
    core
)

from aws_emr_launch.constructs.emr_constructs import (
    emr_code
)
from aws_emr_launch.constructs.step_functions import (
    emr_launch_function,
    emr_chains,
    emr_tasks
)

app = core.App()
stack = core.Stack(app, 'SNSTriggeredPipelineStack', env=core.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"],
    region=os.environ["CDK_DEFAULT_REGION"]))

# SNS Topics for Success/Failures messages from our Pipeline
success_topic = sns.Topic(stack, 'SuccessTopic')
failure_topic = sns.Topic(stack, 'FailureTopic')

# Use the Launch Cluster State Machine we created in the emr_launch_function example
launch_function = emr_launch_function.EMRLaunchFunction.from_stored_function(
    stack, 'BasicLaunchFunction', 'launch-basic-cluster')

# Prepare the scripts executed by our Steps for deployment
# This uses the Artifacts bucket defined in Cluster Configuration used by our
# Launch Function
step_code = emr_code.Code.from_path(
    path='./step_sources',
    deployment_bucket=launch_function.cluster_configuration.emr_profile.artifacts_bucket,
    deployment_prefix='sns_triggered_pipeline/step_sources')

# Create a Chain to receive Failure messages
fail = emr_chains.Fail(
    stack, 'FailChain',
    message=sfn.TaskInput.from_data_at('$.Error'),
    subject='Pipeline Failure',
    topic=failure_topic)

# Use the State Machine defined earlier to launch the Cluster
# The ClusterConfigurationOverrides and Tags will be passed through for
# runtime overrides
launch_cluster = emr_chains.NestedStateMachine(
    stack, 'NestedStateMachine',
    name='Launch Cluster StateMachine',
    state_machine=launch_function.state_machine,
    input={
        'ClusterConfigurationOverrides': sfn.TaskInput.from_data_at('$.ClusterConfigurationOverrides').value,
        'Tags': sfn.TaskInput.from_data_at('$.Tags').value
    },
    fail_chain=fail)

# Create a Parallel Task for the Steps
steps = sfn.Parallel(stack, 'Steps', result_path='$.Result.Steps')

# Add a Failure catch to our Parallel phase
steps.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

# Create 5 Phase 1 Parallel Steps. The number of concurrently running Steps is
# defined in the Cluster Configuration
for i in range(5):
    # Define the EMR Step Using S3 Paths created by our Code deployment
    emr_step = emr_code.EMRStep(
        name=f'Step {i}',
        jar='command-runner.jar',
        args=[
            'spark-submit',
            f'{step_code.s3_path}/test_step_{i}.py'
        ],
        code=step_code
    )
    # Define an AddStep Task for Each Step
    step_task = emr_tasks.AddStepBuilder.build(
        stack, f'Step{i}',
        name=f'Step {i}',
        emr_step=emr_step,
        cluster_id=sfn.TaskInput.from_data_at('$.LaunchClusterResult.ClusterId').value)
    steps.branch(step_task)

# Define a Task to Terminate the Cluster
terminate_cluster = emr_tasks.TerminateClusterBuilder.build(
    stack, 'TerminateCluster',
    name='Terminate Cluster',
    cluster_id=sfn.TaskInput.from_data_at('$.LaunchClusterResult.ClusterId').value,
    result_path='$.TerminateResult').add_catch(fail, errors=['States.ALL'], result_path='$.Error')

# A Chain for Success notification when the pipeline completes
success = emr_chains.Success(
    stack, 'SuccessChain',
    message=sfn.TaskInput.from_data_at('$.TerminateResult'),
    subject='Pipeline Succeeded',
    topic=success_topic)

# Assemble the Pipeline
definition = sfn.Chain \
    .start(launch_cluster) \
    .next(steps) \
    .next(terminate_cluster) \
    .next(success)

# Create the State Machine
state_machine = sfn.StateMachine(
    stack, 'SNSTriggeredPipeline',
    state_machine_name='sns-triggered-pipeline', definition=definition)

# Create a Lambda to receive SNS Events from another Pipeline and execute our
# SNS Triggered Pipeline
lambda_code = aws_lambda.Code.from_asset('./lambda_sources')
sns_lambda = aws_lambda.Function(
    stack, 'SNSTriggeredLambda',
    code=lambda_code,
    handler='execute_pipeline.handler',
    runtime=aws_lambda.Runtime.PYTHON_3_7,
    timeout=core.Duration.minutes(1),
    environment={
        'PIPELINE_ARN': state_machine.state_machine_arn
    },
    initial_policy=[
        iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                'states:StartExecution'
            ],
            resources=[state_machine.state_machine_arn]
        )
    ],
    events=[
        sources.SnsEventSource(
            sns.Topic.from_topic_arn(
                stack, 'TransientPipelineSuccessTopic',
                    core.Fn.import_value('TransientPipeline-SuccessTopicArn')))
    ]
)

app.synth()
