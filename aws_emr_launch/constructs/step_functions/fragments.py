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

from typing import Optional, List

from aws_cdk import (
    aws_lambda,
    aws_sns as sns,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_ssm as ssm,
    core
)

from ..emr_constructs.cluster_configurations import BaseConfiguration


class SuccessFragment(sfn.StateMachineFragment):
    def __init__(self, scope: core.Construct, id: str, *,
                 message: sfn.TaskInput, subject: Optional[str] = None,
                 topic: Optional[sns.Topic] = None):
        super().__init__(scope, id)

        self._succeed = sfn.Succeed(
            self, 'Succeeded'
        )

        self._chain = \
            sfn.Task(
                self, 'Failure Notification',
                input_path='$',
                output_path='$',
                result_path='$.PublishResult',
                task=sfn_tasks.PublishToTopic(
                    topic,
                    message=message,
                    subject=subject
                )
            ) \
            .next(self._succeed) if topic is not None else self._succeed

    @property
    def start_state(self) -> sfn.State:
        return self._chain

    @property
    def end_states(self) -> List[sfn.INextable]:
        return self._succeed


class FailFragment(sfn.StateMachineFragment):
    def __init__(self, scope: core.Construct, id: str, *,
                 message: sfn.TaskInput, subject: Optional[str] = None,
                 topic: Optional[sns.Topic] = None):
        super().__init__(scope, id)

        self._fail = sfn.Fail(
            self, 'Execution Failed',
            cause='Execution failed, check JSON output for more details'
        )

        self._chain = \
            sfn.Task(
                self, 'Failure Notification',
                input_path='$',
                output_path='$',
                result_path='$.PublishResult',
                task=sfn_tasks.PublishToTopic(
                    topic,
                    message=message,
                    subject=subject
                )
            ) \
            .next(self._fail) if topic is not None else self._fail

    @property
    def start_state(self) -> sfn.State:
        return self._chain

    @property
    def end_states(self) -> List[sfn.INextable]:
        return self._fail
