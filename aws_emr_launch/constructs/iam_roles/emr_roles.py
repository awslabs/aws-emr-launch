import os
from typing import Optional

from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import core

from aws_emr_launch.constructs.base import BaseConstruct


class EMRRoles(BaseConstruct):
    def __init__(self, scope: core.Construct, id: str, *,
                 role_name_prefix: Optional[str] = None,
                 artifacts_bucket: Optional[s3.Bucket] = None,
                 artifacts_path: Optional[str] = None,
                 logs_bucket: Optional[s3.Bucket] = None,
                 logs_path: Optional[str] = None) -> None:
        super().__init__(scope, id)

        if role_name_prefix:
            self._service_role = EMRRoles._create_service_role(
                self, 'EMRServiceRole', role_name='{}-ServiceRole'.format(role_name_prefix))
            self._instance_role = EMRRoles._create_instance_role(
                self, 'EMRInstanceRole', role_name='{}-InstanceRole'.format(role_name_prefix))
            self._autoscaling_role = EMRRoles._create_autoscaling_role(
                self, 'EMRAutoScalingRole', role_name='{}-AutoScalingRole'.format(role_name_prefix))

            self._instance_profile = iam.CfnInstanceProfile(
                self, '{}_InstanceProfile'.format(id),
                roles=[self._instance_role.role_name],
                instance_profile_name=self._instance_role.role_name)
            self._instance_profile_arn = self._instance_profile.attr_arn

        if artifacts_bucket:
            artifacts_bucket.grant_read(
                self._instance_role,
                os.path.join(artifacts_path, '*') if artifacts_path else artifacts_path).assert_success()

        if logs_bucket:
            logs_bucket.grant_read_write(
                self._service_role,
                os.path.join(logs_path, '*') if logs_path else logs_path).assert_success()
            logs_bucket.grant_read_write(
                self._instance_role,
                os.path.join(logs_path, '*') if logs_path else logs_path).assert_success()

    @staticmethod
    def _glue_catalog_policy(scope: core.Construct) -> iam.PolicyDocument:
        stack = core.Stack.of(scope)
        return iam.PolicyDocument(statements=[
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    'glue:GetDatabase',
                    'glue:GetDatabases'
                ],
                resources=[
                    stack.format_arn(
                        partition=stack.partition,
                        service='glue',
                        resource='catalog'
                    ),
                    stack.format_arn(
                        partition=stack.partition,
                        service='glue',
                        resource='database/default'
                    )
                ]
            )
        ])

    @staticmethod
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

    @staticmethod
    def _create_service_role(scope: core.Construct, id: str, *, role_name: Optional[str] = None):
        role = iam.Role(
            scope, id, role_name=role_name,
            assumed_by=iam.ServicePrincipal('elasticmapreduce.amazonaws.com'),
            inline_policies={
                'emr-artifacts-policy': EMRRoles._emr_artifacts_policy()
            },
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonElasticMapReduceRole')
            ])
        return role

    @staticmethod
    def _create_autoscaling_role(scope: core.Construct, id: str, *, role_name: Optional[str] = None):
        role = iam.Role(
            scope, id, role_name=role_name,
            assumed_by=iam.ServicePrincipal('elasticmapreduce.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AmazonElasticMapReduceforAutoScalingRole')
            ])

        role.assume_role_policy.add_statements(
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
        return role

    @staticmethod
    def _create_instance_role(scope: core.Construct, id: str, *, role_name: Optional[str] = None):
        role = iam.Role(
            scope, id,
            role_name=role_name,
            assumed_by=iam.ServicePrincipal('ec2.amazonaws.com'),
            inline_policies={
                'emr-artifacts-policy': EMRRoles._emr_artifacts_policy(),
                'glue-catalog-policy': EMRRoles._glue_catalog_policy(scope)
            })
        return role

    @staticmethod
    def from_role_arns(scope: core.Construct, id: str, service_role_arn: str,
                       instance_role_arn: str, autoscaling_role_arn: str, mutable: Optional[bool] = None):
        roles = EMRRoles(scope, id)
        roles._service_role = iam.Role.from_role_arn(roles, 'EMRServiceRole', service_role_arn, mutable=False)
        roles._instance_role = iam.Role.from_role_arn(roles, 'EMRInstanceRole', instance_role_arn, mutable=mutable)
        roles._autoscaling_role = iam.Role.from_role_arn(
            roles, 'EMRAutoScalingRole', autoscaling_role_arn, mutable=False)
        roles._instance_profile_arn = roles._instance_role.role_arn.replace(':role/', ':instance-profile/')
        return roles

    @property
    def service_role(self) -> iam.Role:
        return self._service_role

    @property
    def instance_role(self) -> iam.Role:
        return self._instance_role

    @property
    def autoscaling_role(self) -> iam.Role:
        return self._autoscaling_role

    @property
    def instance_profile_arn(self) -> str:
        return self._instance_profile_arn
