import aws_cdk

from aws_emr_launch.control_plane import ControlPlaneStack


def test_control_plane_stack() -> None:
    stack = ControlPlaneStack(aws_cdk.App())

    assert stack.apis
