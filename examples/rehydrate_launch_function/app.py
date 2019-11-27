#!/usr/bin/env python3

from aws_cdk import (
    core
)

from aws_emr_launch.constructs.step_functions.emr_launch_function import EMRLaunchFunction

app = core.App()
stack = core.Stack(app, 'test-rehydrate-function-stack', env=core.Environment(account='876929970656', region='us-west-2'))

launch_config = EMRLaunchFunction.from_stored_config(
    stack, 'test-step-functions-stack', launch_function_name='test-cluster-launch')

print(launch_config.allowed_cluster_config_overrides)

app.synth()
