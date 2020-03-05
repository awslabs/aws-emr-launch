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

from aws_cdk import core

from aws_emr_launch.control_plane.constructs.lambdas import apis


class ControlPlaneStack(core.Stack):
    def __init__(self, app: core.App, name: str = 'aws-emr-launch-control-plane', **kwargs):
        super().__init__(app, name, **kwargs)

        self._apis = apis.Apis(self, 'Apis')

    @property
    def apis(self) -> apis.Apis:
        return self._apis
