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

from typing import List

from aws_cdk import (
    aws_lambda,
    aws_events as events,
    aws_events_targets as targets,
    core
)


class EMREvents(core.Construct):
    def __init__(self, scope: core.Construct, id: str, *,
                 cluster_state_change_event: aws_lambda.Function,
                 step_state_change_event: aws_lambda.Function) -> None:
        super().__init__(scope, id)

        self._events = []
        self._events.append(events.Rule(
            self, 'EMRClusterStateChange',
            event_pattern=events.EventPattern(source=['aws.emr'], detail_type=['EMR Cluster State Change']),
            targets=[targets.LambdaFunction(cluster_state_change_event)]))

        self._events.append(events.Rule(
            self, 'EMRStepStateChange',
            event_pattern=events.EventPattern(source=['aws.emr'], detail_type=['EMR Step Status Change']),
            targets=[targets.LambdaFunction(step_state_change_event)]))

    @property
    def events(self) -> List[events.Rule]:
        return self._events
