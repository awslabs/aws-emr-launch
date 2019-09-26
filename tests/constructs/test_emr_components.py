from aws_cdk import (
    aws_ec2 as ec2,
    core
)

from aws_emr_launch.constructs.emr_components import (
    TransientEMRComponents
)


def test_emr_security_groups():
    app = core.App()
    stack = core.Stack(app, 'test-stack')
    vpc = ec2.Vpc(stack, 'test-vpc')
    emr_componets = TransientEMRComponents(stack, 'test-emr-components', vpc=vpc)

    assert emr_componets.security_groups
