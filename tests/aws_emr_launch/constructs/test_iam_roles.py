from aws_cdk import aws_s3 as s3
from aws_cdk import core

from aws_emr_launch.constructs.iam_roles.emr_roles import EMRRoles


def test_emr_security_groups():
    app = core.App()
    stack = core.Stack(app, 'test-stack')
    artifacts_bucket = s3.Bucket(stack, 'test-artifacts-bucket')
    logs_bucket = s3.Bucket(stack, 'test-logs-bucket')

    emr_roles = EMRRoles(
        stack, 'test-emr-components',
        role_name_prefix='TestCluster',
        artifacts_bucket=artifacts_bucket, logs_bucket=logs_bucket)

    assert emr_roles.service_role
    assert emr_roles.instance_role
    assert emr_roles.autoscaling_role
