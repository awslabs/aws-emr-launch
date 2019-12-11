#!/usr/bin/env python3

from aws_cdk import core

from aws_emr_launch.control_plane import ControlPlaneStack

app = core.App()
ControlPlaneStack(app)

app.synth()
