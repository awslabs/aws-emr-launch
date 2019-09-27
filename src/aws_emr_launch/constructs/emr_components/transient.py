from typing import Optional, List
from aws_cdk import (
    aws_s3 as s3,
    aws_kms as kms,
    aws_ec2 as ec2,
    aws_emr as emr,
    core
)

from ..security_groups.emr import EMRSecurityGroups
from ..iam_roles.emr import EMRRoles


class TransientEMRComponents(core.Construct):

    def __init__(self, scope: core.Construct, id: str, *,
                 cluster_name: str, environment: str, vpc: ec2.Vpc,
                 artifacts_bucket: s3.Bucket, logs_bucket: s3.Bucket,
                 read_buckets: List[s3.Bucket] = None, read_write_buckets: List[s3.Bucket] = None,
                 read_kms_keys: Optional[List[kms.Key]] = None, write_kms_key: Optional[kms.Key] = None,
                 ebs_kms_key: Optional[kms.Key] = None) -> None:
        super().__init__(scope, id)

        self._security_groups = EMRSecurityGroups(self, 'TransientEMRSecurityGroups', vpc=vpc)
        self._roles = EMRRoles(self, 'TransientEMRRoles', role_name_prefix='{}-{}'.format(cluster_name, environment),
                               artifacts_bucket=artifacts_bucket, logs_bucket=logs_bucket,
                               read_buckets=read_buckets, read_write_buckets=read_write_buckets,
                               read_kms_keys=read_kms_keys, write_kms_key=write_kms_key,
                               ebs_kms_key=ebs_kms_key)
        # self._security_configuration = emr.

    @property
    def security_groups(self):
        return self._security_groups

    @property
    def roles(self):
        return self._roles
