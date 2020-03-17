#!/usr/bin/env python3

from aws_cdk import (
    core
)

from aws_emr_launch.constructs.emr_constructs import (
    emr_profile,
    cluster_configuration
)
from aws_emr_launch.constructs.step_functions import emr_launch_function

app = core.App()

print(emr_profile.EMRProfile.get_profiles(namespace='chauncysteam'))
print(emr_profile.EMRProfile.get_profile(profile_name='test-emr-profile', namespace='chauncysteam'))

print(cluster_configuration.ClusterConfiguration.get_configurations())
print(cluster_configuration.ClusterConfiguration.get_configuration('test-cluster'))

print(emr_launch_function.EMRLaunchFunction.get_functions())
print(emr_launch_function.EMRLaunchFunction.get_function('test-cluster-launch'))

app.synth()
