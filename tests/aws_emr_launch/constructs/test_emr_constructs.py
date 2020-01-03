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
    emr_profile,
    cluster_configuration
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

    emr_components = emr_profile.EMRProfile(
        stack, 'test-emr-components',
        profile_name='TestCluster',
        vpc=vpc,
        artifacts_bucket=artifacts_bucket,
        logs_bucket=logs_bucket)

    emr_components \
        .authorize_input_buckets([input_bucket]) \
        .authorize_output_buckets([output_bucket]) \
        .authorize_input_keys([input_key]) \
        .set_s3_encryption('SSE-KMS', s3_key) \
        .set_local_disk_encryption_key(local_disk_key, ebs_encryption=True) \
        .set_tls_certificate_location('s3://null_bucket/cert')

    assert emr_components.security_groups
    assert emr_components.roles
    assert emr_components.s3_encryption_key
    assert emr_components.local_disk_encryption_key
    assert emr_components.ebs_encryption
    assert emr_components.tls_certificate_location


def test_cluster_configurations():
    app = core.App()
    stack = core.Stack(app, 'test-stack')
    vpc = ec2.Vpc(stack, 'test-vpc')
    artifacts_bucket = s3.Bucket(stack, 'test-artifacts-bucket')
    logs_bucket = s3.Bucket(stack, 'test-logs-bucket')

    emr_components = emr_profile.EMRProfile(
        stack, 'test-emr-components',
        profile_name='TestCluster',
        vpc=vpc,
        artifacts_bucket=artifacts_bucket,
        logs_bucket=logs_bucket)

    cluster_config = cluster_configuration.InstanceGroupConfiguration(
        stack, 'test-instance-group-config',
        configuration_name='test-cluster',
        emr_profile=emr_components,
        subnet=vpc.private_subnets[0])

    assert cluster_config.config
