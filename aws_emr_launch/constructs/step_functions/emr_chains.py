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

from typing import Optional

from aws_cdk import (
    aws_sns as sns,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    core
)


class Success(core.Construct):
    def __init__(self, scope: core.Construct, id: str, *,
                 message: sfn.TaskInput, subject: Optional[str] = None,
                 topic: Optional[sns.Topic] = None):
        super().__init__(scope, id)

        succeed = sfn.Succeed(
            scope, 'Succeeded'
        )

        self._chain = \
            sfn.Task(
                scope, 'Success Notification',
                input_path='$',
                output_path='$',
                result_path='$.PublishResult',
                task=sfn_tasks.PublishToTopic(
                    topic,
                    message=message,
                    subject=subject
                )
            ) \
            .next(succeed) if topic is not None else succeed

    @property
    def chain(self) -> sfn.IChainable:
        return self._chain


class Fail(core.Construct):
    def __init__(self, scope: core.Construct, id: str, *,
                 message: sfn.TaskInput, subject: Optional[str] = None,
                 topic: Optional[sns.Topic] = None):
        super().__init__(scope, id)

        fail = sfn.Fail(
            scope, 'Execution Failed',
            cause='Execution failed, check JSON output for more details'
        )

        self._chain = \
            sfn.Task(
                scope, 'Failure Notification',
                input_path='$',
                output_path='$',
                result_path='$.PublishResult',
                task=sfn_tasks.PublishToTopic(
                    topic,
                    message=message,
                    subject=subject
                )
            ) \
            .next(fail) if topic is not None else fail

    @property
    def chain(self) -> sfn.IChainable:
        return self._chain
