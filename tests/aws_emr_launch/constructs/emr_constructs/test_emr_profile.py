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

from aws_cdk import (
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_kms as kms,
    core
)

from aws_emr_launch.constructs.emr_constructs import (
    emr_profile
)


def test_profile_components():
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

    profile = emr_profile.EMRProfile(
        stack, 'test-emr-components',
        profile_name='TestCluster',
        vpc=vpc,
        artifacts_bucket=artifacts_bucket,
        logs_bucket=logs_bucket)

    profile \
        .authorize_input_bucket(input_bucket) \
        .authorize_output_bucket(output_bucket) \
        .authorize_input_key(input_key) \
        .set_s3_encryption(emr_profile.S3EncryptionMode.SSE_KMS, s3_key) \
        .set_local_disk_encryption_key(local_disk_key, ebs_encryption=True) \
        .set_tls_certificate_location('s3://null_bucket/cert')

    resolved_profile = stack.resolve(profile.to_json())
    test_profile = {
        'ProfileName': 'TestCluster',
        'Namespace': 'default',
        'Vpc': {'Ref': 'testvpc8985080E'},
        'MutableInstanceRole': True,
        'MutableSecurityGroups': True,
        'SecurityGroups': {
            'MasterGroup': {'Fn::GetAtt': ['testemrcomponentsSecurityGroupsMasterGroupDAE98884', 'GroupId']},
            'WorkersGroup': {'Fn::GetAtt': ['testemrcomponentsSecurityGroupsWorkersGroup15225BED', 'GroupId']},
            'ServiceGroup': {'Fn::GetAtt': ['testemrcomponentsSecurityGroupsServiceGroup743CE6A3', 'GroupId']}
        },
        'Roles': {
            'ServiceRole': {'Fn::GetAtt': ['testemrcomponentsRolesEMRServiceRoleD1532152', 'Arn']},
            'InstanceRole': {'Fn::GetAtt': ['testemrcomponentsRolesEMRInstanceRoleA27892B5', 'Arn']},
            'InstanceProfile': {'Fn::GetAtt': ['testemrcomponentsRolesRolesInstanceProfile3CF026DE', 'Arn']},
            'AutoScalingRole': {'Fn::GetAtt': ['testemrcomponentsRolesEMRAutoScalingRoleD02C581D', 'Arn']}
        },
        'ArtifactsBucket': {'Ref': 'testartifactsbucketB0C25ABE'},
        'LogsBucket': {'Ref': 'testlogsbucket11454DEF'},
        'LogsPath': 'elasticmapreduce/',
        'S3EncryptionMode': 'SSE_KMS',
        'S3EncryptionKey': {'Fn::GetAtt': ['tests3key4EE82721', 'Arn']},
        'LocalDiskEncryptionKey': {'Fn::GetAtt': ['testlocaldiskkey48AE1C85', 'Arn']},
        'EBSEncryption': True,
        'TLSCertificateLocation': 's3://null_bucket/cert',
        'SecurityConfigurationName': 'TestCluster-SecurityConfiguration'
    }
    print(test_profile)
    print(resolved_profile)
    assert resolved_profile == test_profile