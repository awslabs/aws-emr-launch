#!/usr/bin/env python3

from aws_cdk import core

from constructs.control_plane_stack import ControlPlaneStack

app = core.App()
ControlPlaneStack(app, "aws-emr-launch-control-plane")

app.synth()
