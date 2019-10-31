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
    aws_lambda,
    aws_sns as sns,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_ssm as ssm,
    core
)

from ..emr_constructs.cluster_configurations import BaseConfiguration


class LaunchEMRConfig(core.Construct):
    def __init__(self, scope: core.Construct, id: str, *,
                 cluster_config: BaseConfiguration,
                 default_fail_if_job_running: bool = False,
                 success_topic: Optional[sns.Topic] = None,
                 failure_topic: Optional[sns.Topic] = None,
                 override_cluster_configs_lambda: Optional[aws_lambda.Function] = None) -> None:
        super().__init__(scope, id)

        override_cluster_configs_lambda = aws_lambda.Function.from_function_arn(
            self, 'OverrideClusterConfigs',
            ssm.StringParameter.value_for_string_parameter(
                self,
                '/emr_launch/control_plane/lambda_arns/emr_utilities/EMRLaunch_EMRUtilities_OverrideClusterConfigs'
            )
        ) if override_cluster_configs_lambda is None else override_cluster_configs_lambda

        fail_if_job_running_lambda = aws_lambda.Function.from_function_arn(
            self, 'FailIfJobRunningLambda',
            ssm.StringParameter.value_for_string_parameter(
                self,
                '/emr_launch/control_plane/lambda_arns/emr_utilities/EMRLaunch_EMRUtilities_FailIfJobRunning'
            )
        )

        run_job_flow_lambda = aws_lambda.Function.from_function_arn(
            self, 'RunJobFlowLambda',
            ssm.StringParameter.value_for_string_parameter(
                self,
                '/emr_launch/control_plane/lambda_arns/emr_utilities/EMRLaunch_EMRUtilities_RunJobFlow'
            )
        )

        fail_if_job_running_task = sfn.Task(
            self, 'Fail If Job Running',
            output_path='$',
            result_path='$.Result',
            task=sfn_tasks.InvokeFunction(
                fail_if_job_running_lambda,
                payload={
                    'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                    'DefaultFailIfJobRunning': default_fail_if_job_running,
                    'ClusterName': cluster_config.config.get('Name', '')
                })
        )

        override_cluster_configs_task = sfn.Task(
            self, 'Override Cluster Configs',
            output_path='$',
            result_path='$.ClusterConfig',
            task=sfn_tasks.InvokeFunction(
                override_cluster_configs_lambda,
                payload={
                    'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                    'ClusterConfig': cluster_config.config
                })
        )

        run_job_flow_task = sfn.Task(
            self, 'Start EMR Cluster',
            output_path='$',
            result_path='$.Result',
            task=sfn_tasks.RunLambdaTask(
                run_job_flow_lambda,
                integration_pattern=sfn.ServiceIntegrationPattern.WAIT_FOR_TASK_TOKEN,
                payload={
                    'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                    'ClusterConfig': sfn.TaskInput.from_data_at('$.ClusterConfig').value,
                    'TaskToken': sfn.Context.task_token
                })
        )

        fail = sfn.Fail(
            self, 'Execution Failed',
            cause='Execution failed, check JSON output for more details'
        )

        failure_chain = \
            sfn.Task(
                self, 'Failure Notification',
                input_path='$',
                output_path='$',
                result_path='$.PublishResult',
                task=sfn_tasks.PublishToTopic(
                    failure_topic,
                    message=sfn.TaskInput.from_data_at('$.Result.Message'),
                    subject='Launch EMR Config Failure'
                )
            )\
            .next(fail) if failure_topic is not None else fail

        succeed = sfn.Succeed(
            self, 'Succeeded'
        )
        success_chain = \
            sfn.Task(
                self, 'Succeeded Notification',
                input_path='$',
                output_path='$',
                result_path='$.PublishResult',
                task=sfn_tasks.PublishToTopic(
                    success_topic,
                    message=sfn.TaskInput.from_data_at('$.Result'),
                    subject='Launch EMR Config Succeeded'
                )
            )\
            .next(succeed) if success_topic is not None else succeed

        definition = fail_if_job_running_task\
            .next(override_cluster_configs_task)\
            .next(sfn.Choice(self, 'Continue?')
                  .when(sfn.Condition.not_(sfn.Condition.number_equals('$.Result.Code', 0)), failure_chain)
                  .otherwise(run_job_flow_task
                             .next(sfn.Choice(self, 'Job Started?')
                                   .when(sfn.Condition.not_(
                                         sfn.Condition.number_equals('$.Result.Code', 0)), failure_chain)
                                   .otherwise(success_chain))))

        self._state_machine = sfn.StateMachine(self, 'StateMachine', definition=definition)
