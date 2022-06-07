#!/usr/bin/env python3

import os

import aws_cdk
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda
from aws_cdk import aws_lambda_event_sources as sources
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_stepfunctions as sfn

from aws_emr_launch.constructs.emr_constructs import cluster_configuration, emr_code, emr_profile
from aws_emr_launch.constructs.step_functions import emr_chains, emr_launch_function, emr_tasks

NAMING_PREFIX = f"emr-launch-{aws_cdk.Aws.ACCOUNT_ID}-{aws_cdk.Aws.REGION}"

app = aws_cdk.App()
stack = aws_cdk.Stack(
    app,
    "SNSTriggeredPipelineStack",
    env=aws_cdk.Environment(account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"]),
)

# SNS Topics for Success/Failures messages from our Pipeline
success_topic = sns.Topic(stack, "SuccessTopic")
failure_topic = sns.Topic(stack, "FailureTopic")

# Load our SSE-KMS EMR Profile created in the emr_profiles example
sse_s3_profile = emr_profile.EMRProfile.from_stored_profile(stack, "EMRProfile", "sse-s3-profile")

# Load our Basic Cluster Configuration created in the cluster_configurations example
cluster_config = cluster_configuration.ClusterConfiguration.from_stored_configuration(
    stack, "ClusterConfiguration", "high-mem-instance-group-cluster"
)

# Create a new State Machine to launch a cluster with the Basic configuration
# Don't allow only ClusterName to be overwritten at launch time.
# Unless specifically indicated, fail to start if a cluster
# of the same name is already running.
launch_function = emr_launch_function.EMRLaunchFunction(
    stack,
    "EMRLaunchFunction",
    launch_function_name="launch-sns-triggered-pipeline-cluster",
    cluster_configuration=cluster_config,
    emr_profile=sse_s3_profile,
    cluster_name="sns-triggered-pipeline",
    success_topic=success_topic,
    failure_topic=failure_topic,
    allowed_cluster_config_overrides={"ClusterName": cluster_config.override_interfaces["default"]["ClusterName"]},
    default_fail_if_cluster_running=True,
)

deployment_bucket = (
    s3.Bucket.from_bucket_name(stack, "ArtifactsBucket", f"{NAMING_PREFIX}-artifacts")
    if launch_function.emr_profile.artifacts_bucket is None
    else launch_function.emr_profile.artifacts_bucket
)

# Prepare the scripts executed by our Steps for deployment
# This uses the Artifacts bucket defined in Cluster Configuration used by our
# Launch Function
step_code = emr_code.Code.from_path(
    path="./step_sources", deployment_bucket=deployment_bucket, deployment_prefix="sns_triggered_pipeline/step_sources"
)

# Create a Chain to receive Failure messages
fail = emr_chains.Fail(
    stack,
    "FailChain",
    message=sfn.TaskInput.from_json_path_at("$.Error"),
    subject="Pipeline Failure",
    topic=failure_topic,
)

# Use the State Machine defined earlier to launch the Cluster
# The ClusterConfigurationOverrides and Tags will be passed through for
# runtime overrides
launch_cluster = emr_chains.NestedStateMachine(
    stack,
    "NestedStateMachine",
    name="Launch SNS Pipeline Cluster StateMachine",
    state_machine=launch_function.state_machine,
    input={
        "ClusterConfigurationOverrides": sfn.TaskInput.from_json_path_at("$.ClusterConfigurationOverrides").value,
        "Tags": sfn.TaskInput.from_json_path_at("$.Tags").value,
    },
    fail_chain=fail,
)

# Create a Parallel Task for the Steps
steps = sfn.Parallel(stack, "Steps", result_path="$.Result.Steps")

# Add a Failure catch to our Parallel phase
steps.add_catch(fail, errors=["States.ALL"], result_path="$.Error")

# Create 5 Phase 1 Parallel Steps. The number of concurrently running Steps is
# defined in the Cluster Configuration
for file in emr_code.Code.files_in_path("./step_sources", "test_step_*.py"):
    # Define an AddStep Task for Each Step
    step_task = emr_tasks.AddStepBuilder.build(
        stack,
        f"Step_{file}",
        emr_step=emr_code.EMRStep(
            name=f"Step - {file}",
            jar="command-runner.jar",
            args=["spark-submit", f"{step_code.s3_path}/{file}", "Arg1"],
            code=step_code,
        ),
        cluster_id=sfn.TaskInput.from_json_path_at("$.LaunchClusterResult.ClusterId").value,
    )
    steps.branch(step_task)

# Define a Task to Terminate the Cluster
terminate_cluster = emr_tasks.TerminateClusterBuilder.build(
    stack,
    "TerminateCluster",
    name="Terminate Cluster",
    cluster_id=sfn.TaskInput.from_json_path_at("$.LaunchClusterResult.ClusterId").value,
    result_path="$.TerminateResult",
).add_catch(fail, errors=["States.ALL"], result_path="$.Error")

# A Chain for Success notification when the pipeline completes
success = emr_chains.Success(
    stack,
    "SuccessChain",
    message=sfn.TaskInput.from_json_path_at("$.TerminateResult"),
    subject="Pipeline Succeeded",
    topic=success_topic,
)

# Assemble the Pipeline
definition = sfn.Chain.start(launch_cluster).next(steps).next(terminate_cluster).next(success)

# Create the State Machine
state_machine = sfn.StateMachine(
    stack, "SNSTriggeredPipeline", state_machine_name="sns-triggered-pipeline", definition=definition
)

# Create a Lambda to receive SNS Events from another Pipeline and execute our
# SNS Triggered Pipeline
lambda_code = aws_lambda.Code.from_asset("./lambda_sources")
sns_lambda = aws_lambda.Function(
    stack,
    "SNSTriggeredLambda",
    code=lambda_code,
    handler="execute_pipeline.handler",
    runtime=aws_lambda.Runtime.PYTHON_3_7,
    timeout=aws_cdk.Duration.minutes(1),
    environment={"PIPELINE_ARN": state_machine.state_machine_arn},
    initial_policy=[
        iam.PolicyStatement(
            effect=iam.Effect.ALLOW, actions=["states:StartExecution"], resources=[state_machine.state_machine_arn]
        )
    ],
    events=[
        sources.SnsEventSource(
            sns.Topic.from_topic_arn(
                stack, "TransientPipelineSuccessTopic", aws_cdk.Fn.import_value("TransientPipeline-SuccessTopicArn")
            )
        )
    ],
)

app.synth()
