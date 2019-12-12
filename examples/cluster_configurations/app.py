#!/usr/bin/env python3

from aws_cdk import (
    aws_sns as sns,
    core
)

from aws_emr_launch.constructs.emr_constructs import EMRProfile, InstanceGroupConfiguration
from aws_emr_launch.constructs.step_functions.emr_launch_function import EMRLaunchFunction

app = core.App()
stack = core.Stack(app, 'test-configs-stack', env=core.Environment(account='876929970656', region='us-west-2'))

success_topic = sns.Topic(stack, 'SuccessTopic')
failure_topic = sns.Topic(stack, 'FailureTopic')

emr_profile = EMRProfile.from_stored_profile(
    stack, 'test-emr-profile',
    profile_name='test-emr-profile')

subnet = emr_profile.vpc.private_subnets[0]

cluster_config = InstanceGroupConfiguration(
    stack, 'test-instance-group-config',
    cluster_name='test-cluster',
    profile_components=emr_profile,
    subnet=subnet)

launch_config = EMRLaunchFunction(
    stack, 'test-launch-function',
    launch_function_name='test-cluster-launch',
    cluster_config=cluster_config,
    success_topic=success_topic,
    failure_topic=failure_topic,
    allowed_cluster_config_overrides={
        'Name': 'Name',
        'Instances.InstanceGroups.1.InstanceCount': 'CoreInstanceCount'
    })

launch_config2 = EMRLaunchFunction(
    stack, 'test-launch-function-2',
    launch_function_name='test-cluster-launch-2',
    cluster_config=cluster_config,
    success_topic=success_topic,
    failure_topic=failure_topic,
    allowed_cluster_config_overrides={
        'Name': 'Name',
        'Instances.InstanceGroups.1.InstanceCount': 'CoreInstanceCount',
        'Instances.InstanceGroups.1.InstanceType': 'CoreInstanceType'
    })

app.synth()
