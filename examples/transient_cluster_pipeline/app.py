#!/usr/bin/env python3

import os

from aws_cdk import (
    aws_sns as sns,
    aws_stepfunctions as sfn,
    core
)

from aws_emr_launch.constructs.emr_constructs import (
    cluster_configuration,
    emr_code
)
from aws_emr_launch.constructs.step_functions import (
    emr_launch_function,
    emr_chains,
    emr_tasks
)

app = core.App()
stack = core.Stack(app, 'TransientPipelineStack', env=core.Environment(
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
    deployment_bucket=launch_function.emr_profile.artifacts_bucket,
    deployment_prefix='transient_pipeline/step_sources')

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

# Create a Parallel Task for the Phase 1 Steps
phase_1 = sfn.Parallel(stack, 'Phase1', result_path='$.Result.Phase1')

# Add a Failure catch to our Parallel phase
phase_1.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

# Create 5 Phase 1 Parallel Steps. The number of concurrently running Steps is
# defined in the Cluster Configuration
for i in range(5):
    # Define the EMR Step Using S3 Paths created by our Code deployment
    emr_step = emr_code.EMRStep(
        name=f'Phase 1 - Step {i}',
        jar='s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar',
        args=[
            f'{step_code.s3_path}/phase_1/test_step_{i}.sh',
            'Arg1',
            'Arg2'
        ],
        code=step_code
    )
    # Define an AddStep Task for Each Step
    step_task = emr_tasks.AddStepBuilder.build(
        stack, f'Phase1Step{i}',
        name=f'Phase 1 - Step {i}',
        emr_step=emr_step,
        cluster_id=sfn.TaskInput.from_data_at('$.LaunchClusterResult.ClusterId').value)
    phase_1.branch(step_task)

# Define an example Validation Step
emr_step = emr_code.EMRStep(
    name='Validate Phase 1',
    jar='s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar',
    args=[
        f'{step_code.s3_path}/phase_1/test_validation.sh'
    ],
    code=step_code
)
# Define an AddStep Task for the Validation Step
validate_phase_1 = emr_tasks.AddStepBuilder.build(
    stack, 'ValidatePhase1',
    name='Validate Phase 1',
    emr_step=emr_step,
    cluster_id=sfn.TaskInput.from_data_at('$.LaunchClusterResult.ClusterId').value,
    result_path='$.ValidatePhase1Result').add_catch(fail, errors=['States.ALL'], result_path='$.Error')


# Create a Parallel Task for the Phase 2 Steps
phase_2 = sfn.Parallel(stack, 'Phase2', result_path='$.Result.Phase2')

# Add a Failure catch to our Parallel phase
phase_2.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

# Create 5 Phase 2 Parallel Steps.
for i in range(5):
    emr_step = emr_code.EMRStep(
        name=f'Phase 2 - Step {i}',
        jar='command-runner.jar',
        args=[
            'hive-script',
            '--run-hive-script',
            '--args',
            '-f',
            f'{step_code.s3_path}/phase_2/test_step_{i}.hql',
            '-d'
            'ARG1=Arg1',
            '-d',
            'ARG2=Arg2'
        ],
        code=step_code
    )
    # Define an AddStep Task for Each Step
    step_task = emr_tasks.AddStepBuilder.build(
        stack, f'Phase2Step{i}',
        name=f'Phase 2 - Step {i}',
        emr_step=emr_step,
        cluster_id=sfn.TaskInput.from_data_at('$.LaunchClusterResult.ClusterId').value)
    phase_2.branch(step_task)

# Define an example Validation Step
emr_step = emr_code.EMRStep(
    name='Validate Phase 2',
    jar='command-runner.jar',
    args=[
        'hive-script',
        '--run-hive-script',
        '--args',
        '-f',
        f'{step_code.s3_path}/phase_2/test_validation.hql'
    ],
    code=step_code
)
# Define an AddStep Task for the Validation Step
validate_phase_2 = emr_tasks.AddStepBuilder.build(
    stack, 'ValidatePhase2',
    name='Validate Phase 2',
    emr_step=emr_step,
    cluster_id=sfn.TaskInput.from_data_at('$.LaunchClusterResult.ClusterId').value,
    result_path='$.ValidatePhase2Result').add_catch(fail, errors=['States.ALL'], result_path='$.Error')


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
    .next(phase_1) \
    .next(validate_phase_1) \
    .next(phase_2) \
    .next(validate_phase_2) \
    .next(terminate_cluster) \
    .next(success)

state_machine = sfn.StateMachine(
    stack, 'TransientPipeline',
    state_machine_name='transient-multi-phase-pipeline', definition=definition)

core.CfnOutput(stack, 'SuccessTopicArn', value=success_topic.topic_arn, export_name='TransientPipeline-SuccessTopicArn')

app.synth()
