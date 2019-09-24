from aws_cdk import (
    aws_ec2 as ec2,
    core
)

from emrlaunch.security_groups import (
    EMRSecurityGroups
)

def test_emr_security_groups():
    app = core.App()
    stack = core.Stack(app, 'test-stack')
    vpc = ec2.Vpc(stack, 'test-vpc')
    emr_security_groups = EMRSecurityGroups(stack, 'test-security-groups', vpc)

    assert emr_security_groups.service_group
    assert emr_security_groups.master_group
    assert emr_security_groups.workers_group