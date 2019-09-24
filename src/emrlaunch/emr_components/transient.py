from typing import List
from aws_cdk import (
    aws_s3 as s3,
    aws_iam as iam,
    aws_kms as kms,
    aws_ec2 as ec2,
    core
)
from ..security_groups.emr import EMRSecurityGroups


class TransientEMRComponents(core.Construct):

    def __init__(self, scope: core.Construct, id: str,
                 cluster_name: str, environment: str, vpc: ec2.Vpc,
                 read_kms_keys: List[kms.Key], write_kms_key: kms.Key, ebs_kms_key: kms.Key,
                 artifacts_bucket: s3.Bucket, logs_bucket: s3.Bucket,
                 read_buckets: List[s3.Bucket], read_write_bucket: s3.Bucket) -> None:
        super().__init__(scope, id)

