#!/usr/bin/env python3

from aws_cdk import (
    core
)

from aws_emr_launch.constructs.emr_constructs.emr_profile import EMRProfile
from aws_emr_launch.constructs.emr_constructs.cluster_configuration import BaseConfiguration
from aws_emr_launch.constructs.step_functions.emr_launch_function import EMRLaunchFunction

app = core.App()

print(EMRProfile.get_profiles())
print(EMRProfile.get_profile('test-emr-profile'))

print(BaseConfiguration.get_configurations())
print(BaseConfiguration.get_configuration('test-cluster'))

print(EMRLaunchFunction.get_functions())
print(EMRLaunchFunction.get_function('test-cluster-launch'))

app.synth()
