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

from typing import Dict
from enum import Enum
from logzero import logger
from botocore.exceptions import ClientError

from typing import Optional
from aws_cdk import (
    aws_s3 as s3,
    aws_kms as kms,
    aws_ec2 as ec2,
    aws_emr as emr,
    aws_secretsmanager as secretsmanager,
    aws_ssm as ssm,
    core
)

from aws_emr_launch.constructs.security_groups.emr import EMRSecurityGroups
from aws_emr_launch.constructs.iam_roles.emr_roles import EMRRoles

SSM_PARAMETER_PREFIX = '/emr_launch/emr_profiles'


class ReadOnlyEMRProfileError(Exception):
    pass


class EMRProfileNotFoundError(Exception):
    pass


class S3EncryptionMode(Enum):
    SSE_S3 = 'SSE-S3'
    SSE_KMS = 'SSE-KMS'
    CSE_KMS = 'CSE-KMS'
    CSE_Custom = 'CSE-Custom'


class EMRProfile(core.Construct):

    def __init__(self, scope: core.Construct, id: str, *,
                 profile_name: Optional[str] = None,
                 namespace: str = 'default',
                 vpc: Optional[ec2.Vpc] = None,
                 artifacts_bucket: Optional[s3.Bucket] = None,
                 artifacts_path: Optional[str] = None,
                 logs_bucket: Optional[s3.Bucket] = None,
                 logs_path: Optional[str] = 'elasticmapreduce/',
                 mutable_instance_role: bool = True,
                 mutable_security_groups: bool = True,
                 description: Optional[str] = None) -> None:
        super().__init__(scope, id)

        if not profile_name:
            return

        self._profile_name = profile_name
        self._namespace = namespace
        self._mutable_instance_role = mutable_instance_role
        self._mutable_security_groups = mutable_security_groups
        self._vpc = vpc
        self._security_groups = EMRSecurityGroups(self, 'SecurityGroups', vpc=vpc)
        self._roles = EMRRoles(
            self, 'Roles',
            role_name_prefix=profile_name,
            artifacts_bucket=artifacts_bucket,
            artifacts_path=artifacts_path,
            logs_bucket=logs_bucket,
            logs_path=logs_path)
        self._artifacts_bucket = artifacts_bucket
        self._artifacts_path = artifacts_path
        self._logs_bucket = logs_bucket
        self._logs_path = logs_path
        self._description = description

        self._s3_encryption_configuration = {
            'EncryptionMode': S3EncryptionMode.SSE_S3.value
        }
        self._local_disk_encryption_configuration = None
        self._tls_certificate_configuration = None
        self._kerberos_configuration = None
        self._kerberos_attributes_secret = None
        self._emrfs_configuration = None

        self._security_configuration = None
        self._security_configuration_name = None

        self._ssm_parameter = ssm.CfnParameter(
            self, 'SSMParameter',
            type='String',
            value=json.dumps(self.to_json()),
            tier='Intelligent-Tiering',
            name=f'{SSM_PARAMETER_PREFIX}/{namespace}/{profile_name}')

        self._construct_security_configuration()

        self._rehydrated = False

    def to_json(self):
        property_values = {
            'ProfileName': self._profile_name,
            'Namespace': self._namespace,
            'Vpc': self._vpc.vpc_id if self._vpc is not None else None,
            'MutableInstanceRole': self._mutable_instance_role,
            'MutableSecurityGroups': self._mutable_security_groups,
            'SecurityGroups': {
                'MasterGroup': self._security_groups.master_group.security_group_id,
                'WorkersGroup': self._security_groups.workers_group.security_group_id,
                'ServiceGroup': self._security_groups.service_group.security_group_id
            },
            'Roles': {
                'ServiceRole': self._roles.service_role.role_arn,
                'InstanceRole': self._roles.instance_role.role_arn,
                'InstanceProfile': self._roles.instance_profile_arn,
                'AutoScalingRole': self._roles.autoscaling_role.role_arn
            },
            'ArtifactsBucket': self._artifacts_bucket.bucket_name if self._artifacts_bucket else None,
            'ArtifactsPath': self._artifacts_path,
            'LogsBucket': self._logs_bucket.bucket_name if self._logs_bucket else None,
            'LogsPath': self._logs_path,
            'S3EncryptionConfiguration': self._s3_encryption_configuration,
            'LocalDiskEncryptionConfiguration': self._local_disk_encryption_configuration,
            'TLSCertificateConfiguration': self._tls_certificate_configuration,
            'KerberosConfiguration': self._kerberos_configuration,
            'KerberosAttributesSecret': self._kerberos_attributes_secret.secret_arn
            if self._kerberos_attributes_secret else None,
            'EmrFsConfiguration': self._emrfs_configuration,
            'SecurityConfigurationName': self._security_configuration_name,
            'Description': self._description
        }
        return property_values

    def from_json(self, property_values):
        self._profile_name = property_values['ProfileName']
        self._namespace = property_values['Namespace']
        self._mutable_instance_role = property_values['MutableInstanceRole']
        self._mutable_security_groups = property_values['MutableSecurityGroups']

        vpc_id = property_values.get('Vpc', None)
        self._vpc = ec2.Vpc.from_lookup(self, 'Vpc', vpc_id=vpc_id) \
            if vpc_id \
            else None

        security_groups_ids = property_values['SecurityGroups']
        self._security_groups = EMRSecurityGroups.from_security_group_ids(
            self, 'SecurityGroups', security_groups_ids['MasterGroup'],
            security_groups_ids['WorkersGroup'], security_groups_ids['ServiceGroup'],
            mutable=self._mutable_security_groups
        )

        role_arns = property_values['Roles']
        self._roles = EMRRoles.from_role_arns(
            self, 'Roles', role_arns['ServiceRole'], role_arns['InstanceRole'],
            role_arns['AutoScalingRole'], mutable=self._mutable_instance_role)

        artifacts_bucket = property_values.get('ArtifactsBucket', None)
        self._artifacts_bucket = s3.Bucket.from_bucket_name(self, 'ArtifactsBucket', artifacts_bucket)\
            if artifacts_bucket \
            else None
        self._artifacts_path = property_values.get('ArtifactsPath', None)

        logs_bucket = property_values.get('LogsBucket', None)
        self._logs_bucket = s3.Bucket.from_bucket_name(self, 'LogsBucket', logs_bucket) \
            if logs_bucket \
            else None
        self._logs_path = property_values.get('LogsPath', None)

        self._s3_encryption_configuration = property_values.get('S3EncryptionConfiguration', None)
        self._local_disk_encryption_configuration = property_values.get('LocalDiskEncryptionConfiguration', None)
        self._tls_certificate_configuration = property_values.get('TLSCertificateConfiguration', None)
        self._kerberos_configuration = property_values.get('KerberosConfiguration', None)

        kerberos_attributes_secret = property_values.get('KerberosAttributesSecret', None)
        self._kerberos_attributes_secret = \
            secretsmanager.Secret.from_secret_arn(self, 'KerberosAttributesSecret', kerberos_attributes_secret) \
            if kerberos_attributes_secret else None

        self._emrfs_configuration = property_values.get('EmrFsConfiguration', None)
        self._security_configuration_name = property_values.get('SecurityConfigurationName', None)
        self._description = property_values.get('Description', None)
        self._rehydrated = True
        return self

    def _construct_security_configuration(self, custom_security_configuration=None) -> None:
        # Initialize the CfnSecurityConfiguration
        if self._security_configuration is None:
            name = f'{self._profile_name}-SecurityConfiguration'
            self._security_configuration = emr.CfnSecurityConfiguration(
                self, 'SecurityConfiguration',
                security_configuration={}, name=name
            )
            self._security_configuration_name = name

        self._ssm_parameter.value = json.dumps(self.to_json())

        if custom_security_configuration is not None:
            self._security_configuration.security_configuration = self._custom_security_configuration
            return

        encryption_config = {}
        # Set In-Transit Encryption
        if self._tls_certificate_configuration:
            encryption_config['EnableInTransitEncryption'] = True
            encryption_config['InTransitEncryptionConfiguration'] = self._tls_certificate_configuration
        else:
            encryption_config['EnableInTransitEncryption'] = False

        # Set At-Rest Encryption
        if self._s3_encryption_configuration or self._local_disk_encryption_configuration:
            encryption_config['EnableAtRestEncryption'] = True

            at_rest_config = {}

            if self._s3_encryption_configuration:
                at_rest_config['S3EncryptionConfiguration'] = self._s3_encryption_configuration

            if self._local_disk_encryption_configuration:
                at_rest_config['LocalDiskEncryptionConfiguration'] = self._local_disk_encryption_configuration

            encryption_config['AtRestEncryptionConfiguration'] = at_rest_config
        else:
            encryption_config['EnableAtRestEncryption'] = False

        # Set Authorization
        authorization_configuration = None
        if self._kerberos_configuration or self._emrfs_configuration:
            authorization_configuration = {}

            if self._kerberos_configuration:
                authorization_configuration['KerberosConfiguration'] = self._kerberos_configuration

            if self._emrfs_configuration:
                authorization_configuration['EmrFsConfiguration'] = self._emrfs_configuration

        self._security_configuration.security_configuration = {
            'EncryptionConfiguration': encryption_config
        }
        if authorization_configuration:
            self._security_configuration.security_configuration['AuthorizationConfiguration'] = \
                authorization_configuration

    @property
    def profile_name(self) -> str:
        return self._profile_name

    @property
    def namespace(self) -> str:
        return self._namespace

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
    def security_configuration_name(self) -> str:
        return self._security_configuration_name

    @property
    def description(self) -> str:
        return self._description

    def set_s3_encryption(self, mode: Optional[S3EncryptionMode], encryption_key: Optional[kms.Key] = None):
        if self._rehydrated:
            raise ReadOnlyEMRProfileError()

        if mode and mode == S3EncryptionMode.CSE_Custom:
            raise NotImplementedError('Use of CSE-Custom currently requires setting a custom security '
                                      'configuration with `set_custom_security_configuration()`')

        self._s3_encryption_configuration = {
            'EncryptionMode': mode.value
        }

        if mode in [S3EncryptionMode.SSE_KMS, S3EncryptionMode.CSE_KMS]:
            if encryption_key:
                self._s3_encryption_configuration['AwsKmsKey'] = encryption_key.key_arn
                encryption_key.grant_encrypt(self._roles.instance_role)
            else:
                raise ValueError(f'Parameter "encryption_key" cannot be None when "mode" is of type {mode.value}')

        self._construct_security_configuration()
        return self

    def set_local_disk_encryption(self, encryption_key: kms.Key, ebs_encryption: bool = True):
        if self._rehydrated:
            raise ReadOnlyEMRProfileError()

        self._local_disk_encryption_configuration = {
            'EncryptionKeyProviderType': 'AwsKms',
            'AwsKmsKey': encryption_key.key_arn
        }
        encryption_key.grant_encrypt_decrypt(self._roles.instance_role)

        if ebs_encryption:
            self._local_disk_encryption_configuration['EnableEbsEncryption'] = True
            encryption_key.grant_encrypt_decrypt(self._roles.service_role)
            encryption_key.grant(self._roles.service_role, 'kms:CreateGrant', 'kms:ListGrants', 'kms:RevokeGrant')

        self._construct_security_configuration()
        return self

    def set_tls_certificate(self, certificate_location: str):
        if self._rehydrated:
            raise ReadOnlyEMRProfileError()

        self._tls_certificate_configuration = {
            'TLSCertificateConfiguration': {
                'CertificateProviderType': 'PEM',
                'S3Object': certificate_location
            }
        }
        self._construct_security_configuration()
        return self

    def set_local_kdc(self, kerberos_attributes_secret: secretsmanager.Secret,
                      ticket_lifetime_in_hours: Optional[int] = 24):
        self._kerberos_attributes_secret = kerberos_attributes_secret
        self._kerberos_configuration = {
            'Provider': 'ClusterDedicatedKdc',
            'ClusterDedicatedKdcConfiguration': {
                'TicketLifetimeInHours': ticket_lifetime_in_hours
            }
        }
        logger.warn('------------------------------------------------------------------------------')
        logger.warn(f'SecretsManager Secret: {kerberos_attributes_secret.secret_arn}')
        logger.warn('Must contain Key/Values: Realm, KdcAdminPassword')
        logger.warn('------------------------------------------------------------------------------')
        self._construct_security_configuration()
        return self

    def set_local_kdc_with_cross_realm_trust(self, kerberos_attributes_secret: secretsmanager.Secret,
                                             realm: str, domain: str, admin_server: str, kdc_server: str,
                                             ticket_lifetime_in_hours: Optional[int] = 24):
        self._kerberos_attributes_secret = kerberos_attributes_secret
        self._kerberos_configuration = {
            'Provider': 'ClusterDedicatedKdc',
            'ClusterDedicatedKdcConfiguration': {
                'TicketLifetimeInHours': ticket_lifetime_in_hours,
                'CrossRealmTrustConfiguration': {
                    'Realm': realm,
                    'Domain': domain,
                    'AdminServer': admin_server,
                    'KdcServer': kdc_server
                }
            }
        }
        logger.warn('------------------------------------------------------------------------------')
        logger.warn(f'SecretsManager Secret: {kerberos_attributes_secret.secret_arn}')
        logger.warn('Must contain Key/Values: Realm, KdcAdminPassword, ADDomainJoinUser, ')
        logger.warn('ADDomainJoinPassword, CrossRealmTrustPrincipalPassword')
        logger.warn('------------------------------------------------------------------------------')
        self._construct_security_configuration()
        return self

    def set_external_kdc(self, kerberos_attributes_secret: secretsmanager.Secret,
                         admin_server: str, kdc_server: str):
        self._kerberos_attributes_secret = kerberos_attributes_secret
        self._kerberos_configuration = {
            'Provider': 'ExternalKdc',
            'ExternalKdcConfiguration': {
                'KdcServerType': 'Single',
                'AdminServer': admin_server,
                'KdcServer': kdc_server
            }
        }
        logger.warn('------------------------------------------------------------------------------')
        logger.warn(f'SecretsManager Secret: {kerberos_attributes_secret.secret_arn}')
        logger.warn('Must contain Key/Values: Realm, KdcAdminPassword')
        logger.warn('------------------------------------------------------------------------------')
        self._construct_security_configuration()
        return self

    def set_external_kdc_with_cross_realm_trust(self, kerberos_attributes_secret: secretsmanager.Secret,
                                                admin_server: str, kdc_server: str, ad_realm: str, ad_domain: str):
        self._kerberos_attributes_secret = kerberos_attributes_secret
        self._kerberos_configuration = {
            'Provider': 'ExternalKdc',
            'ExternalKdcConfiguration': {
                'KdcServerType': 'Single',
                'AdminServer': admin_server,
                'KdcServer': kdc_server,
                'AdIntegrationConfiguration': {
                    'AdRealm': ad_realm,
                    'AdDomain': ad_domain
                }
            }
        }
        logger.warn('------------------------------------------------------------------------------')
        logger.warn(f'SecretsManager Secret: {kerberos_attributes_secret.secret_arn}')
        logger.warn('Must contain Key/Values: Realm, KdcAdminPassword, ADDomainJoinUser, ')
        logger.warn('ADDomainJoinPassword')
        logger.warn('------------------------------------------------------------------------------')
        self._construct_security_configuration()
        return self

    def set_custom_security_configuration(self, security_configuration):
        if self._rehydrated:
            raise ReadOnlyEMRProfileError()

        logger.warn('------------------------------------------------------------------------------')
        logger.warn('Setting a Custom SecurityConfiguration will override all')
        logger.warn('SecurityConfiguration settings')
        logger.warn('------------------------------------------------------------------------------')
        self._construct_security_configuration(security_configuration)
        return self

    def authorize_input_bucket(self, bucket: s3.Bucket, objects_key_pattern: Optional[str] = None):
        if self._rehydrated and not self._mutable_instance_role:
            raise ReadOnlyEMRProfileError()

        bucket.grant_read(self._roles.instance_role, objects_key_pattern).assert_success()
        return self

    def authorize_output_bucket(self, bucket: s3.Bucket, objects_key_pattern: Optional[str] = None):
        if self._rehydrated and not self._mutable_instance_role:
            raise ReadOnlyEMRProfileError()

        bucket.grant_write(self._roles.instance_role, objects_key_pattern).assert_success()
        return self

    def authorize_input_key(self, key: kms.Key):
        if self._rehydrated and not self._mutable_instance_role:
            raise ReadOnlyEMRProfileError()

        key.grant_decrypt(self._roles.instance_role).assert_success()
        return self

    def authorize_output_key(self, key: kms.Key):
        if self._rehydrated and not self._mutable_instance_role:
            raise ReadOnlyEMRProfileError()

        key.grant_encrypt(self._roles.instance_role).assert_success()
        return self

    @staticmethod
    def get_profiles(namespace: str = 'default', next_token: Optional[str] = None,
                     ssm_client=None) -> Dict[str, any]:
        ssm_client = boto3.client('ssm') if ssm_client is None else ssm_client
        params = {
            'Path': f'{SSM_PARAMETER_PREFIX}/{namespace}/'
        }
        if next_token:
            params['NextToken'] = next_token
        result = ssm_client.get_parameters_by_path(**params)

        profiles = {
            'EMRProfiles': [json.loads(p['Value']) for p in result['Parameters']]
        }
        if 'NextToken' in result:
            profiles['NextToken'] = result['NextToken']
        return profiles

    @staticmethod
    def get_profile(profile_name: str, namespace: str = 'default',
                    ssm_client=None) -> Dict[str, any]:
        ssm_client = boto3.client('ssm') if ssm_client is None else ssm_client
        try:
            profile_json = ssm_client.get_parameter(
                Name=f'{SSM_PARAMETER_PREFIX}/{namespace}/{profile_name}')['Parameter']['Value']
            return json.loads(profile_json)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ParameterNotFound':
                raise EMRProfileNotFoundError()
            else:
                raise e

    @staticmethod
    def from_stored_profile(scope: core.Construct, id: str, profile_name: str, namespace: str = 'default'):
        stored_profile = EMRProfile.get_profile(profile_name, namespace)
        profile = EMRProfile(scope, id)
        return profile.from_json(stored_profile)
