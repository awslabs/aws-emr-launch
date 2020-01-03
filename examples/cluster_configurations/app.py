#!/usr/bin/env python3

import os

from aws_cdk import (
    core
)

from aws_emr_launch.constructs.emr_constructs import (
    emr_profile,
    emr_code,
    cluster_configuration
)

app = core.App()
stack = core.Stack(app, 'ClusterConfigurationsStack', env=core.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"],
    region=os.environ["CDK_DEFAULT_REGION"]))

# Load EMR Profile by profile_name
sse_kms_profile = emr_profile.EMRProfile.from_stored_profile(
    stack, 'SSEKMSProfile',
    profile_name='sse-kms-profile')


# This prepares the project's bootstrap_source/ folder for deployment
# We use the Artifacts bucket configured and authorized on the EMR Profile
bootstrap_code = emr_code.Code.from_path(
    path='./bootstrap_source',
    deployment_bucket=sse_kms_profile.artifacts_bucket,
    deployment_prefix='emr_launch_testing/bootstrap_source')

# Define a Bootstrap Action using the bootstrap_source/ folder's deployment location
bootstrap = emr_code.EMRBootstrapAction(
    name='bootstrap-1',
    path=f'{bootstrap_code.s3_path}/test_bootstrap.sh',
    args=['Arg1', 'Arg2'],
    code=bootstrap_code)

# Cluster Configurations that use InstanceGroups are deployed to a Private subnet
subnet = sse_kms_profile.vpc.private_subnets[0]

# Create a basic Cluster Configuration using InstanceGroups, the Subnet and Bootstrap
# Action defined above, the EMR Profile we loaded, and defaults defined in
# the InstanceGroupConfiguration
basic_cluster_config = cluster_configuration.InstanceGroupConfiguration(
    stack, 'BasicClusterConfiguration',
    configuration_name='basic-instance-group-cluster',
    emr_profile=sse_kms_profile,
    subnet=subnet,
    bootstrap_actions=[bootstrap],
    step_concurrency_level=2)

# Here we create another Cluster Configuration using the same subnet, bootstrap, and
# EMR Profile while customizing the default Instance Type and Instance Count
high_mem_cluster_config = cluster_configuration.InstanceGroupConfiguration(
    stack, 'HighMemClusterConfiguration',
    configuration_name='high-mem-instance-group-cluster',
    emr_profile=sse_kms_profile,
    subnet=subnet,
    bootstrap_actions=[bootstrap],
    step_concurrency_level=4,
    core_instance_type='r5.2xlarge',
    core_instance_count=5)

app.synth()
