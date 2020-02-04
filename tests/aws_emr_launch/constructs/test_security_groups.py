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
    core
)

from aws_emr_launch.constructs.security_groups.emr import EMRSecurityGroups


def test_emr_security_groups():
    app = core.App()
    stack = core.Stack(app, 'test-stack')
    vpc = ec2.Vpc(stack, 'test-vpc')
    emr_security_groups = EMRSecurityGroups(stack, 'test-security-groups', vpc=vpc)

    assert emr_security_groups.service_group
    assert emr_security_groups.master_group
    assert emr_security_groups.workers_group
