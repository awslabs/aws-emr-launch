# Copyright 2019 Amazon.com, Inc. and its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the 'License').
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#   http://aws.amazon.com/asl/
#
# or in the 'license' file accompanying this file. This file is distributed
# on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

from typing import Optional
from aws_cdk import (
    aws_iam as iam,
    aws_s3 as s3,
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

    def __init__(self, scope: core.Construct, id: str, role_name_prefix: str,
                 artifacts_bucket: s3.Bucket, logs_bucket: s3.Bucket) -> None:
        super().__init__(scope, id)

        self._service_role = EMRServiceRole(
            self, 'TransientEMRServiceRole', role_name='{}-ServiceRole'.format(role_name_prefix))
        self._instance_role = EMREC2InstanceRole(
            self, 'TransientEMRInstanceRole', role_name='{}-InstanceRole'.format(role_name_prefix))
        self._autoscaling_role = EMREC2InstanceRole(
            self, 'TransientEMRAutoScalingRole', role_name='{}-AutoScalingRole'.format(role_name_prefix))

        artifacts_bucket.grant_read(self._service_role)
        artifacts_bucket.grant_read(self._instance_role)

        logs_bucket.grant_read_write(self._service_role)
        logs_bucket.grant_read_write(self._instance_role)

    @property
    def service_role(self):
        return self._service_role

    @property
    def instance_role(self):
        return self._instance_role

    @property
    def autoscaling_role(self):
        return self._autoscaling_role
