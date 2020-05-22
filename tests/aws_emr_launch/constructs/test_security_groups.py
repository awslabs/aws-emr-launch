from aws_cdk import aws_ec2 as ec2
from aws_cdk import core

from aws_emr_launch.constructs.security_groups.emr import EMRSecurityGroups


def test_emr_security_groups():
    app = core.App()
    stack = core.Stack(app, 'test-stack')
    vpc = ec2.Vpc(stack, 'test-vpc')
    emr_security_groups = EMRSecurityGroups(stack, 'test-security-groups', vpc=vpc)

    assert emr_security_groups.service_group
    assert emr_security_groups.master_group
    assert emr_security_groups.workers_group
