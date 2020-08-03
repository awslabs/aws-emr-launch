from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_sns as sns
from aws_cdk import core

from aws_emr_launch import __product__, __version__
from aws_emr_launch.constructs.emr_constructs import emr_profile, cluster_configuration
from aws_emr_launch.constructs.step_functions import emr_launch_function


default_function = {
    'AllowedClusterConfigOverrides': {
        'ClusterName': {
            'Default': 'test-configuration',
            'JsonPath': 'Name'
        },
        'ReleaseLabel': {
            'Default': 'emr-5.29.0',
            'JsonPath': 'ReleaseLabel'
        },
        'StepConcurrencyLevel': {
            'Default': 1,
            'JsonPath': 'StepConcurrencyLevel'
        }
    },
    'ClusterConfiguration': 'default/test-configuration',
    'ClusterName': 'test-cluster',
    'ClusterTags': [
        {'Key': 'deployment:product:name', 'Value': __product__},
        {'Key': 'deployment:product:version', 'Value': __version__}
    ],
    'DefaultFailIfClusterRunning': False,
    'EMRProfile': 'default/test-profile',
    'FailureTopic': {'Ref': 'FailureTopic74C6EA16'},
    'LaunchFunctionName': 'test-function',
    'Namespace': 'default',
    'StateMachine': {'Ref': 'testfunctionStateMachineF50AE8F9'},
    'SuccessTopic': {'Ref': 'SuccessTopic495EEDDD'},
    'WaitForClusterStart': False
}


def test_emr_launch_function():
    app = core.App()
    stack = core.Stack(app, 'test-stack')
    vpc = ec2.Vpc(stack, 'Vpc')
    success_topic = sns.Topic(stack, 'SuccessTopic')
    failure_topic = sns.Topic(stack, 'FailureTopic')

    profile = emr_profile.EMRProfile(
        stack, 'test-profile',
        profile_name='test-profile',
        vpc=vpc)
    configuration = cluster_configuration.ClusterConfiguration(
        stack, 'test-configuration', configuration_name='test-configuration')

    function = emr_launch_function.EMRLaunchFunction(
        stack, 'test-function',
        launch_function_name='test-function',
        emr_profile=profile,
        cluster_configuration=configuration,
        cluster_name='test-cluster',
        success_topic=success_topic,
        failure_topic=failure_topic,
        allowed_cluster_config_overrides=configuration.override_interfaces['default'],
        wait_for_cluster_start=False
    )

    resolved_function = stack.resolve(function.to_json())
    print(default_function)
    print(resolved_function)
    assert default_function == resolved_function


def test_emr_secure_launch_function():
    app = core.App()
    stack = core.Stack(app, 'test-stack')
    vpc = ec2.Vpc(stack, 'Vpc')
    success_topic = sns.Topic(stack, 'SuccessTopic')
    failure_topic = sns.Topic(stack, 'FailureTopic')

    profile = emr_profile.EMRProfile(
        stack, 'test-profile',
        profile_name='test-profile',
        vpc=vpc,)
    configuration = cluster_configuration.ClusterConfiguration(
        stack, 'test-configuration',
        configuration_name='test-configuration',
        secret_configurations={
            'SecretConfiguration': secretsmanager.Secret(stack, 'Secret')
        })

    function = emr_launch_function.EMRLaunchFunction(
        stack, 'test-function',
        launch_function_name='test-function',
        emr_profile=profile,
        cluster_configuration=configuration,
        cluster_name='test-cluster',
        success_topic=success_topic,
        failure_topic=failure_topic,
        allowed_cluster_config_overrides=configuration.override_interfaces['default'],
        wait_for_cluster_start=False
    )

    resolved_function = stack.resolve(function.to_json())
    print(default_function)
    print(resolved_function)
    assert default_function == resolved_function
