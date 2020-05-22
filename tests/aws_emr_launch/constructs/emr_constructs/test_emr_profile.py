from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import core

from aws_emr_launch.constructs.emr_constructs import emr_profile

app = core.App()
stack = core.Stack(app, 'test-stack')
vpc = ec2.Vpc(stack, 'test-vpc')
artifacts_bucket = s3.Bucket(stack, 'test-artifacts-bucket')
logs_bucket = s3.Bucket(stack, 'test-logs-bucket')
input_bucket = s3.Bucket(stack, 'test-input-bucket')
output_bucket = s3.Bucket(stack, 'test-output-bucket')
input_key = kms.Key(stack, 'test-input-key')
s3_key = kms.Key(stack, 'test-s3-key')
local_disk_key = kms.Key(stack, 'test-local-disk-key')
secret = secretsmanager.Secret(stack, 'test-secret')
emrfs_mappings_role = iam.Role(stack, 'test-emrfs-role', assumed_by=iam.ServicePrincipal('ec2.amazonaws.com'))
lake_formation_role = iam.Role(stack, 'test-lake-formation-role', assumed_by=iam.ServicePrincipal('ec2.amazonaws.com'))
services_role = iam.Role(stack, 'test-services-role', assumed_by=iam.ServicePrincipal('ec2.amazonaws.com'))


default_profile = {
    'ProfileName': 'TestCluster',
    'Namespace': 'default',
    'Vpc': {'Ref': 'testvpc8985080E'},
    'MutableInstanceRole': True,
    'MutableSecurityGroups': True,
    'SecurityGroups': {
        'MasterGroup': {'Fn::GetAtt': ['testprofileSecurityGroupsMasterGroup31D25CD2', 'GroupId']},
        'WorkersGroup': {'Fn::GetAtt': ['testprofileSecurityGroupsWorkersGroup3AA4AA50', 'GroupId']},
        'ServiceGroup': {'Fn::GetAtt': ['testprofileSecurityGroupsServiceGroup594B671E', 'GroupId']}
    },
    'Roles': {
        'ServiceRole': {'Fn::GetAtt': ['testprofileRolesEMRServiceRoleFED85131', 'Arn']},
        'InstanceRole': {'Fn::GetAtt': ['testprofileRolesEMRInstanceRole5B4B46D3', 'Arn']},
        'InstanceProfile': {'Fn::GetAtt': ['testprofileRolesRolesInstanceProfileBEFAB63E', 'Arn']},
        'AutoScalingRole': {'Fn::GetAtt': ['testprofileRolesEMRAutoScalingRoleE8A59E0D', 'Arn']}
    },
    'ArtifactsBucket': {'Ref': 'testartifactsbucketB0C25ABE'},
    'LogsBucket': {'Ref': 'testlogsbucket11454DEF'},
    'LogsPath': 'elasticmapreduce/',
    'S3EncryptionConfiguration': {
        'EncryptionMode': 'SSE-S3',
    },
    'SecurityConfiguration': {'Ref': 'testprofileSecurityConfigurationCAAF9611'}
}

profile = emr_profile.EMRProfile(
    stack, 'test-profile',
    profile_name='TestCluster',
    vpc=vpc,
    artifacts_bucket=artifacts_bucket,
    logs_bucket=logs_bucket)


def test_profile_authorizations():
    profile \
        .authorize_input_bucket(input_bucket) \
        .authorize_output_bucket(output_bucket) \
        .authorize_input_key(input_key)

    resolved_profile = stack.resolve(profile.to_json())
    print(default_profile)
    print(resolved_profile)
    assert resolved_profile == default_profile


def test_profile_s3_encryption():
    profile \
        .set_s3_encryption(emr_profile.S3EncryptionMode.SSE_KMS, s3_key)

    default_profile['S3EncryptionConfiguration'] = {
        'EncryptionMode': 'SSE-KMS',
        'AwsKmsKey': {'Fn::GetAtt': ['tests3key4EE82721', 'Arn']}
    }

    resolved_profile = stack.resolve(profile.to_json())
    print(default_profile)
    print(resolved_profile)
    assert resolved_profile == default_profile


def test_profile_local_disk_encryption():
    profile \
        .set_local_disk_encryption(local_disk_key, ebs_encryption=True)

    default_profile['LocalDiskEncryptionConfiguration'] = {
        'EncryptionKeyProviderType': 'AwsKms',
        'AwsKmsKey': {'Fn::GetAtt': ['testlocaldiskkey48AE1C85', 'Arn']},
        'EnableEbsEncryption': True
    }

    resolved_profile = stack.resolve(profile.to_json())
    print(default_profile)
    print(resolved_profile)
    assert resolved_profile == default_profile


def test_profile_tls():
    profile \
        .set_tls_certificate('s3://null_bucket/cert')

    default_profile['TLSCertificateConfiguration'] = {
        'TLSCertificateConfiguration': {
            'CertificateProviderType': 'PEM',
            'S3Object': 's3://null_bucket/cert'
        }
    }

    resolved_profile = stack.resolve(profile.to_json())
    print(default_profile)
    print(resolved_profile)
    assert resolved_profile == default_profile


def test_profile_local_kdc():
    profile \
        .set_local_kdc(secret)

    default_profile['KerberosAttributesSecret'] = {'Ref': 'testsecretF8BBC644'}
    default_profile['KerberosConfiguration'] = {
        'Provider': 'ClusterDedicatedKdc',
        'ClusterDedicatedKdcConfiguration': {
            'TicketLifetimeInHours': 24
        }
    }

    resolved_profile = stack.resolve(profile.to_json())
    print(default_profile)
    print(resolved_profile)
    assert resolved_profile == default_profile


def test_profile_local_kdc_with_cross_realm_trust():
    profile \
        .set_local_kdc_with_cross_realm_trust(
            secret, 'realm', 'domain', 'admin_server', 'kdc_server')

    default_profile['KerberosAttributesSecret'] = {'Ref': 'testsecretF8BBC644'}
    default_profile['KerberosConfiguration'] = {
        'Provider': 'ClusterDedicatedKdc',
        'ClusterDedicatedKdcConfiguration': {
            'TicketLifetimeInHours': 24,
            'CrossRealmTrustConfiguration': {
                'Realm': 'realm',
                'Domain': 'domain',
                'AdminServer': 'admin_server',
                'KdcServer': 'kdc_server'
            }
        }
    }

    resolved_profile = stack.resolve(profile.to_json())
    print(default_profile)
    print(resolved_profile)
    assert resolved_profile == default_profile


def test_profile_external_kdc():
    profile \
        .set_external_kdc(secret, 'admin_server', 'kdc_server')

    default_profile['KerberosAttributesSecret'] = {'Ref': 'testsecretF8BBC644'}
    default_profile['KerberosConfiguration'] = {
        'Provider': 'ExternalKdc',
        'ExternalKdcConfiguration': {
            'KdcServerType': 'Single',
            'AdminServer': 'admin_server',
            'KdcServer': 'kdc_server'
        }
    }

    resolved_profile = stack.resolve(profile.to_json())
    print(default_profile)
    print(resolved_profile)
    assert resolved_profile == default_profile


def test_profile_external_kdc_with_cross_realm_trust():
    profile \
        .set_external_kdc_with_cross_realm_trust(
            secret, 'admin_server', 'kdc_server', 'ad_realm', 'ad_domain')

    default_profile['KerberosAttributesSecret'] = {'Ref': 'testsecretF8BBC644'}
    default_profile['KerberosConfiguration'] = {
        'Provider': 'ExternalKdc',
        'ExternalKdcConfiguration': {
            'KdcServerType': 'Single',
            'AdminServer': 'admin_server',
            'KdcServer': 'kdc_server',
            'AdIntegrationConfiguration': {
                'AdRealm': 'ad_realm',
                'AdDomain': 'ad_domain'
            }
        }
    }

    resolved_profile = stack.resolve(profile.to_json())
    print(default_profile)
    print(resolved_profile)
    assert resolved_profile == default_profile


def test_profile_emrfs_prefix_role_mapping():
    profile \
        .add_emrfs_role_mapping_for_s3_prefixes(emrfs_mappings_role, ['s3://bucket/prefix'])

    default_profile['EmrFsConfiguration'] = {
        'RoleMappings': [{
            'Role': {'Fn::GetAtt': ['testemrfsrole229C26D4', 'Arn']},
            'IdentifierType': 'Prefix',
            'Identifiers': ['s3://bucket/prefix']
        }]
    }

    resolved_profile = stack.resolve(profile.to_json())
    print(default_profile)
    print(resolved_profile)
    assert resolved_profile == default_profile
    default_profile.pop('EmrFsConfiguration')
    profile._emrfs_configuration = None


def test_profile_emrfs_user_role_mapping():
    profile \
        .add_emrfs_role_mapping_for_users(emrfs_mappings_role, ['user'])

    default_profile['EmrFsConfiguration'] = {
        'RoleMappings': [{
            'Role': {'Fn::GetAtt': ['testemrfsrole229C26D4', 'Arn']},
            'IdentifierType': 'User',
            'Identifiers': ['user']
        }]
    }

    resolved_profile = stack.resolve(profile.to_json())
    print(default_profile)
    print(resolved_profile)
    assert resolved_profile == default_profile
    default_profile.pop('EmrFsConfiguration')
    profile._emrfs_configuration = None


def test_profile_emrfs_group_role_mapping():
    profile \
        .add_emrfs_role_mapping_for_groups(emrfs_mappings_role, ['group'])

    default_profile['EmrFsConfiguration'] = {
        'RoleMappings': [{
            'Role': {'Fn::GetAtt': ['testemrfsrole229C26D4', 'Arn']},
            'IdentifierType': 'Group',
            'Identifiers': ['group']
        }]
    }

    resolved_profile = stack.resolve(profile.to_json())
    print(default_profile)
    print(resolved_profile)
    assert resolved_profile == default_profile
    default_profile.pop('EmrFsConfiguration')
    profile._emrfs_configuration = None


def test_profile_lake_formation_enablement():
    profile \
        .enable_lake_formation(secret, 's3://bucket/idp.xml', lake_formation_role, services_role)

    default_profile['KerberosAttributesSecret'] = {'Ref': 'testsecretF8BBC644'}
    default_profile['KerberosConfiguration'] = {
        'Provider': 'ClusterDedicatedKdc',
        'ClusterDedicatedKdcConfiguration': {
            'TicketLifetimeInHours': 24
        }
    }
    default_profile['LakeFormationConfiguration'] = {
        'IdpMetadataS3Path': 's3://bucket/idp.xml',
        'EmrRoleForUsersARN': {'Fn::GetAtt': ['testservicesrole14912EA6', 'Arn']},
        'LakeFormationRoleForSAMLPrincipalARN': {'Fn::GetAtt': ['testlakeformationroleBFC53CBB', 'Arn']}
    }

    resolved_profile = stack.resolve(profile.to_json())
    print(default_profile)
    print(resolved_profile)
    assert resolved_profile == default_profile
