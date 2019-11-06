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
import typing
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


# class SuccessFragment(sfn.StateMachineFragment):
#     def __init__(self, scope: core.Construct, id: str, *,
#                  message: sfn.TaskInput, subject: Optional[str] = None,
#                  topic: Optional[sns.Topic] = None):
#         super().__init__(scope, id)
#
#         self._succeed = sfn.Succeed(
#             self, 'Succeeded'
#         )
#
#         self._chain = \
#             sfn.Task(
#                 self, 'Failure Notification',
#                 input_path='$',
#                 output_path='$',
#                 result_path='$.PublishResult',
#                 task=sfn_tasks.PublishToTopic(
#                     topic,
#                     message=message,
#                     subject=subject
#                 )
#             ) \
#             .next(self._succeed) if topic is not None else self._succeed
#
#     def start_state(self) -> sfn.State:
#         return self._chain
#
#     def end_states(self) -> List[sfn.INextable]:
#         return self._succeed


# class FailFragment(sfn.StateMachineFragment):
#     def __init__(self, scope: core.Construct, id: str, *,
#                  message: sfn.TaskInput, subject: Optional[str] = None,
#                  topic: Optional[sns.Topic] = None):
#         super().__init__(scope, id)
#
#         self._fail = sfn.Fail(
#             self, 'Execution Failed',
#             cause='Execution failed, check JSON output for more details'
#         )
#
#         self._chain = \
#             sfn.Task(
#                 self, 'Failure Notification',
#                 input_path='$',
#                 output_path='$',
#                 result_path='$.PublishResult',
#                 task=sfn_tasks.PublishToTopic(
#                     topic,
#                     message=message,
#                     subject=subject
#                 )
#             ) \
#             .next(self._fail) if topic is not None else self._fail
#
#     @property
#     def start_state(self) -> sfn.State:
#         return self._chain
#
#     @property
#     def end_states(self) -> List[sfn.INextable]:
#         return self._fail

class EMRFragments:
    @staticmethod
    def success_fragment(scope: core.Construct, *,
                         message: sfn.TaskInput, subject: Optional[str] = None,
                         topic: Optional[sns.Topic] = None) -> sfn.IChainable:
        succeed = sfn.Succeed(
            scope, 'Succeeded'
        )

        chain = \
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
        return chain


    @staticmethod
    def fail_fragment(scope: core.Construct, *,
                      message: sfn.TaskInput, subject: Optional[str] = None,
                      topic: Optional[sns.Topic] = None) -> sfn.IChainable:
        fail = sfn.Fail(
            scope, 'Execution Failed',
            cause='Execution failed, check JSON output for more details'
        )

        chain = \
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
        return chain

    @staticmethod
    def override_cluster_configs_task(
            scope: core.Construct, *,
            cluster_config: dict,
            override_cluster_configs_lambda: Optional[aws_lambda.Function] = None,
            output_path: str = '$',
            result_path: str = '$.ClusterConfig') -> sfn.IChainable:
        aws_lambda.Function.from_function_arn(
            scope, 'OverrideClusterConfigs',
            ssm.StringParameter.value_for_string_parameter(
                scope,
                '/emr_launch/control_plane/lambda_arns/emr_utilities/EMRLaunch_EMRUtilities_OverrideClusterConfigs'
            )
        ) if override_cluster_configs_lambda is None else override_cluster_configs_lambda

        override_cluster_configs_task = sfn.Task(
            scope, 'Override Cluster Configs',
            output_path=output_path,
            result_path=result_path,
            task=sfn_tasks.InvokeFunction(
                override_cluster_configs_lambda,
                payload={
                    'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                    'ClusterConfig': cluster_config
                })
        )
        return override_cluster_configs_task

    @staticmethod
    def fail_if_job_running_task(
            scope: core.Construct, *, default_fail_if_job_running: bool) -> sfn.IChainable:
        fail_if_job_running_lambda = aws_lambda.Function.from_function_arn(
            scope, 'FailIfJobRunningLambda',
            ssm.StringParameter.value_for_string_parameter(
                scope,
                '/emr_launch/control_plane/lambda_arns/emr_utilities/EMRLaunch_EMRUtilities_FailIfJobRunning'
            )
        )

        fail_if_job_running_task = sfn.Task(
            scope, 'Fail If Job Running',
            output_path='$',
            result_path='$',
            task=sfn_tasks.InvokeFunction(
                fail_if_job_running_lambda,
                payload={
                    'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                    'DefaultFailIfJobRunning': default_fail_if_job_running,
                    'ClusterConfig': sfn.TaskInput.from_data_at('$.ClusterConfig').value
                })
        )
        return fail_if_job_running_task

    @staticmethod
    def run_job_flow_task(scope: core.Construct, *, result_path: str = '$.Result'):
        run_job_flow_lambda = aws_lambda.Function.from_function_arn(
            scope, 'RunJobFlowLambda',
            ssm.StringParameter.value_for_string_parameter(
                scope,
                '/emr_launch/control_plane/lambda_arns/emr_utilities/EMRLaunch_EMRUtilities_RunJobFlow'
            )
        )

        run_job_flow_task = sfn.Task(
            scope, 'Start EMR Cluster',
            output_path='$',
            result_path=result_path,
            task=sfn_tasks.RunLambdaTask(
                run_job_flow_lambda,
                integration_pattern=sfn.ServiceIntegrationPattern.WAIT_FOR_TASK_TOKEN,
                payload={
                    'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                    'ClusterConfig': sfn.TaskInput.from_data_at('$.ClusterConfig').value,
                    'TaskToken': sfn.Context.task_token
                })
        )
        return run_job_flow_task

