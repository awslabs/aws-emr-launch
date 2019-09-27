from typing import Optional, List
from aws_cdk import (
    aws_iam as iam,
    aws_s3 as s3,
    aws_kms as kms,
    core
)


def _emr_artifacts_policy() -> iam.PolicyDocument:
    return iam.PolicyDocument(
        statements=[
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    's3:GetObject*',
                    's3:List*'
                ],
                resources=[
                    'arn:aws:s3:::elasticmapreduce',
                    'arn:aws:s3:::elasticmapreduce/*',
                    'arn:aws:s3:::elasticmapreduce.samples',
                    'arn:aws:s3:::elasticmapreduce.samples/*',
                    'arn:aws:s3:::*.elasticmapreduce',
                    'arn:aws:s3:::*.elasticmapreduce/*',
                    'arn:aws:s3:::*.elasticmapreduce.samples',
                    'arn:aws:s3:::*.elasticmapreduce.samples/*'
                ]
            )
        ]
    )


class EMRServiceRole(iam.Role):

    def __init__(self, scope: core.Construct, id: str, role_name: Optional[str] = None):
        super().__init__(scope, id, role_name=role_name,
                         assumed_by=iam.ServicePrincipal('elasticmapreduce.amazonaws.com'),
                         inline_policies={
                             'emr-artifacts-policy': _emr_artifacts_policy()
                         },
                         managed_policies=[
                             iam.ManagedPolicy.from_aws_managed_policy_name('AmazonElasticMapReduceRole')
                         ])


class EMRAutoScalingRole(iam.Role):

    def __init__(self, scope: core.Construct, id: str, role_name: Optional[str] = None):
        super().__init__(scope, id, role_name=role_name,
                         assumed_by=iam.ServicePrincipal('elasticmapreduce.amazonaws.com'),
                         managed_policies=[
                             iam.ManagedPolicy.from_aws_managed_policy_name('AmazonElasticMapReduceforAutoScalingRole')
                         ])

        self.assume_role_policy.add_statements(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                principals=[
                    iam.ServicePrincipal('application-autoscaling.amazonaws.com')
                ],
                actions=[
                    'sts:AssumeRole'
                ]
            )
        )


class EMREC2InstanceRole(iam.Role):

    def __init__(self, scope: core.Construct, id: str, role_name: Optional[str] = None):
        super().__init__(scope, id, role_name=role_name,
                         assumed_by=iam.ServicePrincipal('ec2.amazonaws.com'),
                         inline_policies={
                             'emr-artifacts-policy': _emr_artifacts_policy()
                         })


class EMRRoles(core.Construct):

    def __init__(self, scope: core.Construct, id: str, cluster_name: str, environment: str,
                 artifacts_bucket: s3.Bucket, logs_bucket: s3.Bucket,
                 read_buckets: List[s3.Bucket] = None, read_write_buckets: List[s3.Bucket] = None,
                 read_kms_keys: Optional[List[kms.Key]] = None, write_kms_key: Optional[kms.Key] = None,
                 ebs_kms_key: Optional[kms.Key] = None) -> None:
        super().__init__(scope, id)

        self._service_role = EMRServiceRole(
            self, 'TransientEMRServiceRole', role_name='{}-ServiceRole-{}'.format(cluster_name, environment))
        self._instance_role = EMREC2InstanceRole(
            self, 'TransientEMRInstanceRole', role_name='{}-InstanceRole-{}'.format(cluster_name, environment))
        self._autoscaling_role = EMREC2InstanceRole(
            self, 'TransientEMRAutoScalingRole', role_name='{}-AutoScalingRole-{}'.format(cluster_name, environment))

        artifacts_bucket.grant_read(self._service_role)
        artifacts_bucket.grant_read(self._instance_role)

        logs_bucket.grant_read_write(self._service_role)
        logs_bucket.grant_read_write(self._instance_role)

        for bucket in read_buckets if read_buckets else []:
            bucket.grant_read(self._instance_role)
        for bucket in read_write_buckets if read_write_buckets else []:
            bucket.grant_read_write(self._instance_role)
        for key in read_kms_keys if read_kms_keys else []:
            key.grant_decrypt(self._instance_role)
        if write_kms_key:
            write_kms_key.grant_encrypt(self._instance_role)
        if ebs_kms_key:
            ebs_kms_key.grant_encrypt_decrypt(self._instance_role)

    @property
    def service_role(self):
        return self._service_role

    @property
    def instance_role(self):
        return self._instance_role

    @property
    def autoscaling_role(self):
        return self._autoscaling_role
