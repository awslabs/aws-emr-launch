from aws_cdk import (
    core
)

from aws_emr_launch.constructs.lambdas.emr_lambdas import (
    EMRLambdas
)


def test_emr_lambdas():
    app = core.App()
    stack = core.Stack(app, 'test-stack')
    emr_lambdas = EMRLambdas(stack, 'test-lambdas')

    assert emr_lambdas.run_job_flow
    assert emr_lambdas.add_job_flow_steps
    assert emr_lambdas.check_step_status
