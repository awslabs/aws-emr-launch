#!/usr/bin/env python3

from aws_cdk import (
    core
)

from aws_emr_launch.constructs.step_functions.emr_launch_function import EMRLaunchFunction

app = core.App()

print(EMRLaunchFunction.list_functions())

app.synth()
