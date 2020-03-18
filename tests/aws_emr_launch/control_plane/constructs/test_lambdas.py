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

from aws_emr_launch.control_plane.constructs.lambdas.apis import Apis


def test_emr_lambdas():
    app = core.App()
    stack = core.Stack(app, 'test-lambdas-stack')
    apis = Apis(stack, 'test-apis')

    assert apis.get_profile
    assert apis.get_profiles
    assert apis.get_configuration
    assert apis.get_configurations
    assert apis.get_function
    assert apis.get_functions
