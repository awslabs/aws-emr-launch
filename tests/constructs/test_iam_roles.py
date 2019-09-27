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
    aws_s3 as s3,
    aws_kms as kms,
    core
)

from aws_emr_launch.constructs.iam_roles.emr import EMRRoles


def test_emr_security_groups():
    app = core.App()
    stack = core.Stack(app, 'test-stack')
    artifacts_bucket = s3.Bucket(stack, 'test-artifacts-bucket')
    logs_bucket = s3.Bucket(stack, 'test-logs-bucket')
    read_bucket = s3.Bucket(stack, 'test-read-bucket')
    read_write_bucket = s3.Bucket(stack, 'test-read-write-bucket')
    read_key = kms.Key(stack, 'test-read-key')
    write_key = kms.Key(stack, 'test-write-key')
    ebs_key = kms.Key(stack, 'test-ebs-key')

    emr_roles = EMRRoles(
        stack, 'test-emr-components',
        role_name_prefix='TestCluster',
        artifacts_bucket=artifacts_bucket, logs_bucket=logs_bucket,
        read_buckets=[read_bucket], read_write_buckets=[read_write_bucket],
        read_kms_keys=[read_key], write_kms_key=write_key, ebs_kms_key=ebs_key)

    assert emr_roles.service_role
    assert emr_roles.instance_role
    assert emr_roles.autoscaling_role