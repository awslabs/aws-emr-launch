#!/usr/bin/env python3

from aws_cdk import core

from control_plane.control_plane_stack import ControlPlaneStack


app = core.App()
ControlPlaneStack(app, "control-plane")

app.synth()
