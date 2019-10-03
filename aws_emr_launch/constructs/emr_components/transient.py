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

from typing import Optional, List
from aws_cdk import (
    aws_s3 as s3,
    aws_kms as kms,
    aws_ec2 as ec2,
    aws_emr as emr,
    aws_sns as sns,
    core
)

from ..security_groups.emr import EMRSecurityGroups
from ..iam_roles.emr import EMRRoles


class TransientEMRComponents(core.Construct):

    def __init__(self, scope: core.Construct, id: str, *,
                 cluster_name: str, environment: str, vpc: ec2.Vpc,
                 artifacts_bucket: s3.Bucket, logs_bucket: s3.Bucket) -> None:
        super().__init__(scope, id)

        self._cluster_name = cluster_name
        self._environment = environment
        self._vpc = vpc
        self._artifacts_bucket = artifacts_bucket
        self._logs_bucket = logs_bucket

        self._security_groups = EMRSecurityGroups(self, 'TransientEMRComponents_SecurityGroups', vpc=vpc)

        self._roles = EMRRoles(self, 'TransientEMRComponents_Roles',
                               role_name_prefix='{}-{}'.format(cluster_name, environment),
                               artifacts_bucket=artifacts_bucket, logs_bucket=logs_bucket)

        self._success_topic = sns.Topic(
            self, 'TransientEMRComponents_SuccessTopic',
            display_name='{}-{}-SuccessTopic'.format(cluster_name, environment),
            topic_name='{}-{}-SuccessTopic'. format(cluster_name, environment))
        self._failure_topic = sns.Topic(
            self, 'TransientEMRComponents_FailureTopic',
            display_name='{}-{}-FailureTopic'.format(cluster_name, environment),
            topic_name='{}-{}-FailureTopic'. format(cluster_name, environment))

        self._s3_encryption_mode = None
        self._s3_encryption_key = None
        self._local_disk_encryption_key = None
        self._ebs_encryption = False
        self._tls_certificate_location = None
        self._security_configuration = None

    def _construct_security_configuration(self, custom_security_configuration=None) -> None:
        if (not custom_security_configuration
                and not self._s3_encryption_mode
                and not self._local_disk_encryption_key
                and not self._tls_certificate_location):
            self._security_configuration = None
            return

        if self._security_configuration is None:
            name = '{}-{}-SecurityConfiguration'.format(self._cluster_name, self._environment)
            self._security_configuration = emr.CfnSecurityConfiguration(
                self, 'TransientEMRComponents_SecurityConfiguration',
                security_configuration={}, name=name
            )

        if custom_security_configuration is not None:
            self._security_configuration.security_configuration = self._custom_security_configuration
            return

        encryption_config = {}

        if self._tls_certificate_location:
            encryption_config['EnableInTransitEncryption'] = True
            encryption_config['InTransitEncryptionConfiguration'] = {
                'TLSCertificateConfiguration': {
                    'CertificateProviderType': 'PEM',
                    'S3Object': self._tls_certificate_location
                }
            }
        else:
            encryption_config['EnableInTransitEncryption'] = False

        if self._s3_encryption_mode or self._local_disk_encryption_key:
            encryption_config['EnableAtRestEncryption'] = True
            atrest_config = {}

            if self._s3_encryption_mode:
                atrest_config['S3EncryptionConfiguration'] = {
                    'EncryptionMode': self._s3_encryption_mode
                }
                if self._s3_encryption_key:
                    atrest_config['S3EncryptionConfiguration']['AwsKmsKey'] = self._s3_encryption_key.key_arn

            if self._local_disk_encryption_key:
                atrest_config['LocalDiskEncryptionConfiguration'] = {
                    'EncryptionProviderType': 'AwsKms',
                    'AwsKmsKey': self._local_disk_encryption_key.key_arn
                }
                if self._ebs_encryption:
                    atrest_config['LocalDiskEncryptionConfiguration']['EnableEbsEncryption'] = True

            encryption_config['AtRestEncryptionConfiguration'] = atrest_config
        else:
            encryption_config['EnableAtRestEncryption'] = False

        self._security_configuration.security_configuration = {
            'EncryptionConfiguration': encryption_config
        }

    @property
    def cluster_name(self) -> str:
        return self._cluster_name

    @property
    def environment(self) -> str:
        return self._environment

    @property
    def vpc(self) -> ec2.Vpc:
        return self._vpc

    @property
    def artifacts_bucket(self) -> s3.Bucket:
        return self._artifacts_bucket

    @property
    def logs_bucket(self) -> s3.Bucket:
        return self._logs_bucket

    @property
    def security_groups(self) -> EMRSecurityGroups:
        return self._security_groups

    @property
    def roles(self) -> EMRRoles:
        return self._roles

    @property
    def success_topic(self) -> sns.Topic:
        return self._success_topic

    @property
    def failure_topic(self) -> sns.Topic:
        return self._failure_topic

    @property
    def s3_encryption_mode(self) -> str:
        return self._s3_encryption_mode

    @property
    def s3_encryption_key(self) -> kms.Key:
        return self._s3_encryption_key

    @property
    def local_disk_encryption_key(self) -> kms.Key:
        return self._local_disk_encryption_key

    @property
    def ebs_encryption(self) -> bool:
        return self._ebs_encryption

    @property
    def tls_certificate_location(self) -> str:
        return self._tls_certificate_location

    @property
    def security_configuration(self) -> emr.CfnSecurityConfiguration:
        return self._security_configuration

    def set_s3_encryption(self, mode: str, encryption_key: Optional[kms.Key] = None):
        if encryption_key:
            encryption_key.grant_encrypt(self._roles.instance_role)
        self._s3_encryption_mode = mode
        self._s3_encryption_key = encryption_key
        self._construct_security_configuration()
        return self

    def set_local_disk_encryption_key(self, encryption_key: kms.Key, ebs_encryption: bool = True):
        encryption_key.grant_encrypt_decrypt(self._roles.instance_role)
        if ebs_encryption:
            encryption_key.grant_encrypt_decrypt(self._roles.service_role)
        self._local_disk_encryption_key = encryption_key
        self._ebs_encryption = ebs_encryption
        self._construct_security_configuration()
        return self

    def set_tls_certificate_location(self, certificate_location: str):
        self._tls_certificate_location = certificate_location
        self._construct_security_configuration()
        return self

    def set_custom_security_configuration(self, security_configuration):
        self._construct_security_configuration(security_configuration)
        return self

    def authorize_input_buckets(self, input_buckets: List[s3.Bucket]):
        for bucket in input_buckets:
            bucket.grant_read(self._roles.instance_role)
        return self

    def authorize_output_buckets(self, output_buckets: List[s3.Bucket]):
        for bucket in output_buckets:
            bucket.grant_write(self._roles.instance_role)
        return self

    def authorize_input_keys(self, input_keys: List[kms.Key]):
        for key in input_keys:
            key.grant_decrypt(self._roles.instance_role)
        return self

    def authorize_output_keys(self, output_keys: List[kms.Key]):
        for key in output_keys:
            key.grant_encrypt(self._roles.instance_role)
        return self
