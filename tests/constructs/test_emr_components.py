from aws_cdk import (
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_kms as kms,
    core
)

from aws_emr_launch.constructs.emr_components import (
    TransientEMRComponents
)


def test_emr_security_groups():
    app = core.App()
    stack = core.Stack(app, 'test-stack')
    vpc = ec2.Vpc(stack, 'test-vpc')
    artifacts_bucket = s3.Bucket(stack, 'test-artifacts-bucket')
    logs_bucket = s3.Bucket(stack, 'test-logs-bucket')
    read_bucket = s3.Bucket(stack, 'test-read-bucket')
    read_write_bucket = s3.Bucket(stack, 'test-read-write-bucket')
    read_key = kms.Key(stack, 'test-read-key')
    write_key = kms.Key(stack, 'test-write-key')
    ebs_key = kms.Key(stack, 'test-ebs-key')

    emr_components = TransientEMRComponents(
        stack, 'test-emr-components',
        cluster_name='TestCluster', environment='test',
        vpc=vpc, artifacts_bucket=artifacts_bucket, logs_bucket=logs_bucket,
        read_buckets=[read_bucket], read_write_buckets=[read_write_bucket],
        read_kms_keys=[read_key], write_kms_key=write_key, ebs_kms_key=ebs_key)

    assert emr_components.security_groups
