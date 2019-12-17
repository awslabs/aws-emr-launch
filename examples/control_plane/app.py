#!/usr/bin/env python3

from aws_cdk import core

from aws_emr_launch import control_plane

app = core.App()
control_plane.ControlPlaneStack(app)

app.synth()
