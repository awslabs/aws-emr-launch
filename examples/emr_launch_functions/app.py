#!/usr/bin/env python3

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
stack = core.Stack(app, 'EmrLaunchFunctionsStack', env=core.Environment(account='876929970656', region='us-west-2'))

success_topic = sns.Topic(stack, 'SuccessTopic')
failure_topic = sns.Topic(stack, 'FailureTopic')

cluster_config = cluster_configuration.ClusterConfiguration.from_stored_configuration(
    stack, 'ClusterConfiguration', 'test-cluster')

launch_function = emr_launch_function.EMRLaunchFunction(
    stack, 'EMRLaunchFunction-1',
    launch_function_name='test-cluster-launch',
    cluster_config=cluster_config,
    success_topic=success_topic,
    failure_topic=failure_topic,
    allowed_cluster_config_overrides={
        'Name': 'Name',
        'CoreInstanceCount': 'Instances.InstanceGroups.1.InstanceCount',
        'CoreInstanceType': 'Instances.InstanceGroups.1.InstanceType'
    })

step_code = emr_code.Code.from_path(
    path='./step_source',
    deployment_bucket=cluster_config.profile_components.artifacts_bucket,
    deployment_prefix='emr_launch_testing/step_source')

emr_step = emr_code.EMRStep(
    name='Test Step',
    jar='s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar',
    args=[
        f'{step_code.s3_path}/test_step.sh',
        'Arg1',
        'Arg2'
    ],
    code=step_code
)

fail = emr_chains.Fail(
    stack, 'FailChain',
    message=sfn.TaskInput.from_data_at('$.Error'),
    subject='Pipeline Failure',
    topic=failure_topic).chain

launch_cluster = emr_chains.NestedStateMachine(
    stack, 'NestedStateMachine',
    name='Launch Cluster StateMachine',
    state_machine=launch_function.state_machine,
    input={
        'LaunchParameters': sfn.TaskInput.from_data_at('$.LaunchParameters').value
    },
    fail_chain=fail).chain

add_step = emr_tasks.AddStep(
    stack, 'AddStep',
    name='Add Step',
    emr_step=emr_step,
    cluster_id=sfn.TaskInput.from_data_at('$.Result.ClusterId').value,
    result_path='$.StepResult').task.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

success = emr_chains.Success(
    stack, 'SuccessChain',
    message=sfn.TaskInput.from_data_at('$.StepResult'),
    subject='Pipeline Succeeded',
    topic=success_topic).chain

definition = sfn.Chain \
    .start(launch_cluster) \
    .next(add_step) \
    .next(success)

state_machine = sfn.StateMachine(
    stack, 'AddStepTest',
    state_machine_name='test-add-step-pipeline', definition=definition)

app.synth()
