from aws_cdk import (
    core
)

from aws_emr_launch.constructs.step_functions.emr_launch_function import EMRLaunchFunction


def test_emr_lambdas():
    app = core.App()
    stack = core.Stack(app, 'test-step-functions-stack')

    assert stack
