#!/usr/bin/env python3

from aws_cdk import (
    aws_sns as sns,
    core
)

from aws_emr_launch.constructs.emr_constructs import (
    cluster_configuration,
    emr_code
)
from aws_emr_launch.constructs.step_functions import emr_launch_function

app = core.App()
stack = core.Stack(app, 'EmrLaunchFunctionsStack', env=core.Environment(account='876929970656', region='us-west-2'))

success_topic = sns.Topic(stack, 'SuccessTopic')
failure_topic = sns.Topic(stack, 'FailureTopic')

cluster_config = cluster_configuration.ClusterConfiguration.from_stored_configuration(
    stack, 'ClusterConfiguration', 'test-cluster')

launch_config = emr_launch_function.EMRLaunchFunction(
    stack, 'EMRLaunchFunction-1',
    launch_function_name='test-cluster-launch',
    cluster_config=cluster_config,
    success_topic=success_topic,
    failure_topic=failure_topic,
    allowed_cluster_config_overrides={
        'Name': 'Name',
        'CoreInstanceCount': 'Instances.InstanceGroups.1.InstanceCount'
    })

launch_config2 = emr_launch_function.EMRLaunchFunction(
    stack, 'EMRLaunchFunction-2',
    launch_function_name='test-cluster-launch-2',
    cluster_config=cluster_config,
    success_topic=success_topic,
    failure_topic=failure_topic,
    allowed_cluster_config_overrides={
        'Name': 'Name',
        'CoreInstanceCount': 'Instances.InstanceGroups.1.InstanceCount',
        'CoreInstanceType': 'Instances.InstanceGroups.1.InstanceType'
    })

app.synth()
