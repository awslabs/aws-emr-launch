#!/usr/bin/env python3

import os

from aws_cdk import (
    aws_sns as sns,
    core
)

from aws_emr_launch.constructs.emr_constructs import (
    cluster_configuration,
    emr_profile
)
from aws_emr_launch.constructs.step_functions import (
    emr_launch_function
)

app = core.App()
stack = core.Stack(app, 'EmrLaunchFunctionStack', env=core.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"],
    region=os.environ["CDK_DEFAULT_REGION"]))

# Create SNS Topics to send Success/Failure updates when the Cluster is Launched
success_topic = sns.Topic(stack, 'SuccessTopic')
failure_topic = sns.Topic(stack, 'FailureTopic')

# Load our SSE-KMS EMR Profile created in the emr_profiles example
sse_kms_profile = emr_profile.EMRProfile.from_stored_profile(
    stack, 'EMRProfile', 'sse-kms-profile')

# Load our Basic Cluster Configuration created in the cluster_configurations example
cluster_config = cluster_configuration.ClusterConfiguration.from_stored_configuration(
    stack, 'ClusterConfiguration', 'basic-instance-group-cluster')

# Create a new State Machine to launch a cluster with the Basic configuration
# Unless specifically indicated, fail to start if a cluster of the same name
# is already running. Allow any parameter in the default override_interface to
# be overwritten.
launch_function = emr_launch_function.EMRLaunchFunction(
    stack, 'EMRLaunchFunction',
    launch_function_name='launch-basic-cluster',
    cluster_configuration=cluster_config,
    emr_profile=sse_kms_profile,
    cluster_name='basic-cluster',
    success_topic=success_topic,
    failure_topic=failure_topic,
    default_fail_if_cluster_running=True,
    allowed_cluster_config_overrides=cluster_config.override_interfaces['default'],
    cluster_tags=[core.Tag('Key1', 'Value1'), core.Tag('Key2', 'Value2')])

app.synth()
