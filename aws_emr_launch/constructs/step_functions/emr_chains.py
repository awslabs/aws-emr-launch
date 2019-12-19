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

from typing import Optional, Mapping

from aws_cdk import (
    aws_sns as sns,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    core
)

from ..lambdas import emr_lambdas


class Success(core.Construct):
    def __init__(self, scope: core.Construct, id: str, *,
                 message: sfn.TaskInput, subject: Optional[str] = None,
                 topic: Optional[sns.Topic] = None,
                 result_path: str = '$.PublishResult', output_path: Optional[str] = None):
        super().__init__(scope, id)

        succeed = sfn.Succeed(
            self, 'Succeeded', output_path=output_path
        )

        self._chain = \
            sfn.Task(
                self, 'Success Notification',
                input_path='$',
                output_path='$',
                result_path=result_path,
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
                 topic: Optional[sns.Topic] = None,
                 result_path: str = '$.PublishResult', output_path: str = '$',
                 cause: Optional[str] = None, comment: Optional[str] = None,
                 error: Optional[str] = None):
        super().__init__(scope, id)

        fail = sfn.Fail(
            self, 'Execution Failed', cause=cause, comment=comment, error=error
        )

        self._chain = \
            sfn.Task(
                self, 'Failure Notification',
                input_path='$',
                output_path=output_path,
                result_path=result_path,
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


class NestedStateMachine(core.Construct):
    def __init__(self, scope: core.Construct, id: str, name: str, state_machine: sfn.StateMachine,
                 input: Optional[Mapping[str, any]] = None, fail_chain: Optional[sfn.IChainable] = None):
        super().__init__(scope, id)

        state_machine_task = sfn.Task(
            self, name,
            task=sfn_tasks.StartExecution(
                state_machine,
                input=input,
                integration_pattern=sfn.ServiceIntegrationPattern.SYNC))

        parse_json_string = emr_lambdas.ParseJsonString(self, 'ParseJsonStringLambda').lambda_function

        parse_json_string_task = sfn.Task(
            self, 'Parse JSON Output',
            result_path='$',
            task=sfn_tasks.InvokeFunction(
                parse_json_string,
                payload={
                    'JsonString': sfn.TaskInput.from_data_at('$.Output').value
                })
        )

        if fail_chain:
            state_machine_task.add_catch(fail_chain, errors=['States.ALL'], result_path='$.Error')
            parse_json_string_task.add_catch(fail_chain, errors=['States.ALL'], result_path='$.Error')

        self._chain = state_machine_task.next(parse_json_string_task)

    @property
    def chain(self) -> sfn.IChainable:
        return self._chain
