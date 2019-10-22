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

from aws_emr_launch.constructs.step_functions.launch_emr_config import LaunchEMRConfigStack


def test_emr_lambdas():
    app = core.App()
    emr_lambdas_stack = EMRUtilities(app, 'test-lambdas-stack')
    step_functions_stack = LaunchEMRConfigStack(
        app,
        'test-step-functions-stack',
        run_job_flow_lambda=emr_lambdas_stack.run_job_flow,
        check_step_status_lambda=emr_lambdas_stack.check_step_status)

    assert step_functions_stack.to_string()
