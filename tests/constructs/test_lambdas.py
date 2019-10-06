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
    core
)

from aws_emr_launch.constructs.lambdas.emr_control import EMRControlLambdas


def test_emr_lambdas():
    app = core.App()
    stack = core.Stack(app, 'test-stack')
    emr_lambdas = EMRControlLambdas(stack, 'test-lambdas')

    assert emr_lambdas.run_job_flow
    assert emr_lambdas.add_job_flow_steps
    assert emr_lambdas.check_step_status
