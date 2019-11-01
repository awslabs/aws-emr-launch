#!/usr/bin/env python3

from aws_cdk import (
    aws_sns as sns,
    aws_ec2 as ec2,
    aws_iam as iam,
    core
)

from aws_emr_launch.constructs.emr_constructs import EMRProfile, InstanceGroupConfiguration
from aws_emr_launch.constructs.step_functions.launch_emr_config import LaunchEMRConfig

app = core.App()
stack = core.Stack(app, 'test-configs-stack', env=core.Environment(account='876929970656', region='us-west-2'))

success_topic = sns.Topic(stack, 'SuccessTopic')
failure_topic = sns.Topic(stack, 'FailureTopic')

emr_profile = EMRProfile.from_stored_profile(
    stack, 'test-emr-components',
    profile_name='TestCluster')

emr_profile.roles.instance_role.add_to_policy(
    iam.PolicyStatement(
        effect=iam.Effect.ALLOW,
        actions=['s3:*'],
        resources=['*']
    )
)

subnet = emr_profile.vpc.private_subnets[0]

cluster_config = InstanceGroupConfiguration(
    stack, 'test-instance-group-config',
    cluster_name='test-cluster',
    profile_components=emr_profile,
    subnet=subnet)

launch_config = LaunchEMRConfig(
    stack, 'test-step-functions-stack',
    launch_config_name='test-cluster-launch',
    cluster_config=cluster_config,
    success_topic=success_topic,
    failure_topic=failure_topic)

# launch_role - iam.Role()

app.synth()
