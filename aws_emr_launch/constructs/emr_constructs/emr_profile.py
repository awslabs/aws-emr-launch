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

import json
import boto3

from botocore.exceptions import ClientError

from typing import Optional, List
from aws_cdk import (
    aws_s3 as s3,
    aws_kms as kms,
    aws_ec2 as ec2,
    aws_emr as emr,
    aws_ssm as ssm,
    core
)

from ..security_groups.emr import EMRSecurityGroups
from ..iam_roles.emr import EMRRoles


class ReadOnlyEMRProfileError(Exception):
    pass


class EMRProfileNotFoundError(Exception):
    pass


class EMRProfile(core.Construct):

    def __init__(self, scope: core.Construct, id: str, *, profile_name: Optional[str] = None,
                 vpc: Optional[ec2.Vpc] = None, artifacts_bucket: Optional[s3.Bucket] = None,
                 logs_bucket: Optional[s3.Bucket] = None, mutable_instance_role: bool = False,
                 mutable_security_groups: bool = False) -> None:
        super().__init__(scope, id)

        if not profile_name:
            return

        self._profile_name = profile_name
        self._mutable_instance_role = mutable_instance_role
        self._mutable_security_groups = mutable_security_groups
        self._vpc = vpc
        self._security_groups = EMRSecurityGroups(self, 'SecurityGroups', vpc=vpc)
        self._roles = EMRRoles(self, 'Roles',
                               role_name_prefix=profile_name,
                               artifacts_bucket=artifacts_bucket, logs_bucket=logs_bucket)
        self._artifacts_bucket = artifacts_bucket
        self._logs_bucket = logs_bucket

        self._s3_encryption_mode = None
        self._s3_encryption_key = None
        self._local_disk_encryption_key = None
        self._ebs_encryption = False
        self._tls_certificate_location = None

        self._security_configuration = None
        self._security_configuration_name = None

        self._ssm_parameter = ssm.CfnParameter(
            self, 'SSMParameter',
            type='String',
            value=self._property_values_to_json(),
            name='/emr_launch/control_plane/emr_profiles/{}'.format(profile_name))

        self._rehydrated = False

    def _property_values_to_json(self):
        property_values = {
            'ProfileName': self._profile_name,
            'VpcId': self._vpc.vpc_id if self._vpc is not None else None,
            'MutableInstanceRole': self._mutable_instance_role,
            'MutableSecurityGroups': self._mutable_security_groups,
            'SecurityGroupIds': {
                'MasterGroup': self._security_groups.master_group.security_group_id,
                'WorkersGroup': self._security_groups.workers_group.security_group_id,
                'ServiceGroup': self._security_groups.service_group.security_group_id
            },
            'RoleArns': {
                'ServiceRole': self._roles.service_role.role_arn,
                'InstanceRole': self._roles.instance_role.role_arn,
                'AutoScalingRole': self._roles.autoscaling_role.role_arn
            },
            'ArtifactsBucket': self._artifacts_bucket.bucket_name if self._artifacts_bucket else None,
            'LogsBucket': self._logs_bucket.bucket_name if self._logs_bucket else None,
            'S3EncryptionMode': self._s3_encryption_mode,
            'S3EncryptionKeyArn': self._s3_encryption_key.key_arn if self._s3_encryption_key else None,
            'LocalDiskEncryptionKeyArn':
                self._local_disk_encryption_key.key_arn if self._local_disk_encryption_key else None,
            'EBSEncryption': self._ebs_encryption,
            'TLSCertificateLocation': self._tls_certificate_location,
            'SecurityConfigurationName': self._security_configuration_name
        }
        return json.dumps(property_values)

    def _property_values_from_json(self, property_values_json):
        property_values = json.loads(property_values_json)
        self._profile_name = property_values['ProfileName']
        self._mutable_instance_role = property_values['MutableInstanceRole']
        self._mutable_security_groups = property_values['MutableSecurityGroups']

        vpc_id = property_values.get('VpcId', None)
        self._vpc = ec2.Vpc.from_lookup(self, 'Vpc', vpc_id=vpc_id) \
            if vpc_id \
            else None

        security_groups_ids = property_values['SecurityGroupIds']
        self._security_groups = EMRSecurityGroups.from_security_group_ids(
            self, 'SecurityGroups', security_groups_ids['MasterGroup'],
            security_groups_ids['WorkersGroup'], security_groups_ids['ServiceGroup'],
            mutable=self._mutable_security_groups
        )

        role_arns = property_values['RoleArns']
        self._roles = EMRRoles.from_role_arns(
            self, 'Roles', role_arns['ServiceRole'], role_arns['InstanceRole'],
            role_arns['AutoScalingRole'], mutable=self._mutable_instance_role)

        artifacts_bucket = property_values.get('ArtifactsBucket', None)
        self._artifacts_bucket = s3.Bucket.from_bucket_name(self, 'ArtifactsBucket', artifacts_bucket)\
            if artifacts_bucket \
            else None

        logs_bucket = property_values.get('LogsBucket', None)
        self._logs_bucket = s3.Bucket.from_bucket_name(self, 'LogsBucket', logs_bucket) \
            if logs_bucket \
            else None

        self._s3_encryption_mode = property_values.get('S3EncryptionMode', None)

        s3_encryption_key = property_values.get('S3EncryptionKeyArn', None)
        self._s3_encryption_key = kms.Key.from_key_arn(self, 'S3EncryptionKey', s3_encryption_key) \
            if s3_encryption_key \
            else None

        local_disk_encryption_key = property_values.get('LocalDiskEncryptionKeyArn', None)
        self._local_disk_encryption_key = kms.Key.from_key_arn(
            self, 'LocalDiskEncryptionKey', local_disk_encryption_key) \
            if local_disk_encryption_key \
            else None

        self._ebs_encryption = property_values.get('EBSEncryption', None)
        self._tls_certificate_location = property_values.get('TLSCertificateLocation', None)
        self._security_configuration_name = property_values.get('SecurityConfigurationName', None)
        self._rehydrated = True
        return self

    def _construct_security_configuration(self, custom_security_configuration=None) -> None:
        if (not custom_security_configuration
                and not self._s3_encryption_mode
                and not self._local_disk_encryption_key
                and not self._tls_certificate_location):
            self._security_configuration = None
            self._security_configuration_name = None
            self._ssm_parameter.value = self._property_values_to_json()
            return

        if self._security_configuration is None:
            name = '{}-SecurityConfiguration'.format(self._profile_name)
            self._security_configuration = emr.CfnSecurityConfiguration(
                self, 'SecurityConfiguration',
                security_configuration={}, name=name
            )
            self._security_configuration_name = name

        self._ssm_parameter.value = self._property_values_to_json()

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
                    'EncryptionKeyProviderType': 'AwsKms',
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
    def profile_name(self) -> str:
        return self._profile_name

    @property
    def mutable_instance_role(self) -> bool:
        return self._mutable_instance_role

    @property
    def mutable_security_groups(self) -> bool:
        return self._mutable_security_groups
    
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
    def security_configuration_name(self) -> str:
        return self._security_configuration_name

    def set_s3_encryption(self, mode: str, encryption_key: Optional[kms.Key] = None):
        if self._rehydrated:
            raise ReadOnlyEMRProfileError()

        if encryption_key:
            encryption_key.grant_encrypt(self._roles.instance_role)
        self._s3_encryption_mode = mode
        self._s3_encryption_key = encryption_key
        self._construct_security_configuration()
        return self

    def set_local_disk_encryption_key(self, encryption_key: kms.Key, ebs_encryption: bool = True):
        if self._rehydrated:
            raise ReadOnlyEMRProfileError()

        encryption_key.grant_encrypt_decrypt(self._roles.instance_role)
        if ebs_encryption:
            encryption_key.grant_encrypt_decrypt(self._roles.service_role)
        self._local_disk_encryption_key = encryption_key
        self._ebs_encryption = ebs_encryption
        self._construct_security_configuration()
        return self

    def set_tls_certificate_location(self, certificate_location: str):
        if self._rehydrated:
            raise ReadOnlyEMRProfileError()

        self._tls_certificate_location = certificate_location
        self._construct_security_configuration()
        return self

    def set_custom_security_configuration(self, security_configuration):
        if self._rehydrated:
            raise ReadOnlyEMRProfileError()

        self._construct_security_configuration(security_configuration)
        return self

    def authorize_input_buckets(self, input_buckets: List[s3.Bucket]):
        if self._rehydrated:
            raise ReadOnlyEMRProfileError()

        for bucket in input_buckets:
            bucket.grant_read(self._roles.instance_role)
        return self

    def authorize_output_buckets(self, output_buckets: List[s3.Bucket]):
        if self._rehydrated:
            raise ReadOnlyEMRProfileError()

        for bucket in output_buckets:
            bucket.grant_write(self._roles.instance_role)
        return self

    def authorize_input_keys(self, input_keys: List[kms.Key]):
        if self._rehydrated:
            raise ReadOnlyEMRProfileError()

        for key in input_keys:
            key.grant_decrypt(self._roles.instance_role)
        return self

    def authorize_output_keys(self, output_keys: List[kms.Key]):
        if self._rehydrated:
            raise ReadOnlyEMRProfileError()

        for key in output_keys:
            key.grant_encrypt(self._roles.instance_role)
        return self

    @staticmethod
    def from_stored_profile(scope: core.Construct, id: str, profile_name: str):
        try:
            profile_json = boto3.client('ssm').get_parameter(
                Name='/emr_launch/control_plane/emr_profiles/{}'.format(profile_name))['Parameter']['Value']
            profile = EMRProfile(scope, id)
            return profile._property_values_from_json(profile_json)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ParameterNotFound':
                raise EMRProfileNotFoundError()
