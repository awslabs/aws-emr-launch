#!/usr/bin/env python3

import aws_cdk

from aws_emr_launch import control_plane

app = aws_cdk.App()
control_plane.ControlPlaneStack(app, "EMRLaunchControlPlaneStack")

app.synth()
