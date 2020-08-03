from aws_cdk import core

from aws_emr_launch.control_plane import ControlPlaneStack


def test_control_plane_stack():
    stack = ControlPlaneStack(core.App())

    assert stack.apis
