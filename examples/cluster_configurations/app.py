#!/usr/bin/env python3

from aws_cdk import (
    core
)

from aws_emr_launch.constructs.emr_constructs import (
    emr_profile,
    emr_code,
    cluster_configuration
)

app = core.App()
stack = core.Stack(app, 'ClusterConfigurationsStack', env=core.Environment(account='876929970656', region='us-west-2'))

emr_profile = emr_profile.EMRProfile.from_stored_profile(
    stack, 'EMRProfile',
    profile_name='test-emr-profile')

subnet = emr_profile.vpc.private_subnets[0]

bootstrap_code = emr_code.Code.from_path(
    path='./bootstrap_source',
    deployment_bucket=emr_profile.artifacts_bucket,
    deployment_prefix='emr_launch_testing/bootstrap_source')

bootstrap = emr_code.EMRBootstrapAction(
    name='bootstrap-1',
    path=f'{bootstrap_code.s3_path}/test_bootstrap.sh',
    args=['Arg1', 'Arg2'],
    code=bootstrap_code)

cluster_config = cluster_configuration.InstanceGroupConfiguration(
    stack, 'ClusterConfiguration',
    configuration_name='test-cluster',
    emr_profile=emr_profile,
    subnet=subnet,
    bootstrap_actions=[bootstrap],
    step_concurrency_level=2)

app.synth()
