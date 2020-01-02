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

from .events.emr_events import EMREvents
from .lambdas import (
    emr_utilities,
    apis
)


class ControlPlaneStack(core.Stack):
    def __init__(self, app: core.App, name: str = 'aws-emr-launch-control-plane', **kwargs):
        super().__init__(app, name, **kwargs)

        self._emr_utilities = emr_utilities.EMRUtilities(self, 'EMRUtilities')
        self._apis = apis.Apis(self, 'Apis')

        self._emr_events = EMREvents(
            self, 'EMREvents',
            cluster_state_change_event=self._emr_utilities.cluster_state_change_event,
            step_state_change_event=self._emr_utilities.step_state_change_event
        )

    @property
    def emr_utilities(self) -> emr_utilities.EMRUtilities:
        return self._emr_utilities

    @property
    def apis(self) -> apis.Apis:
        return self._apis

    @property
    def emr_events(self) -> EMREvents:
        return self._emr_events
