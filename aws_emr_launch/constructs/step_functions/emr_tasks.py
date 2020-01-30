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

from typing import Optional, Dict, List

from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_lambda as aws_lambda,
    core
)

from ..lambdas import emr_lambdas
from ..emr_constructs import emr_code
from ..iam_roles import emr_roles


class LoadClusterConfigurationBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *,
              cluster_name: str,
              cluster_tags: List[core.Tag],
              profile_namespace: str,
              profile_name: str,
              configuration_namespace: str,
              configuration_name: str,
              output_path: str = '$',
              result_path: str = '$.ClusterConfig') -> sfn.Task:
        # We use a nested Construct to avoid collisions with Lambda and Task ids
        construct = core.Construct(scope, id)

        load_cluster_configuration_lambda = emr_lambdas.LoadClusterConfigurationBuilder.build(
            construct,
            profile_namespace=profile_namespace,
            profile_name=profile_name,
            configuration_namespace=configuration_namespace,
            configuration_name=configuration_name)

        return sfn.Task(
            construct, 'Load Cluster Configuration',
            output_path=output_path,
            result_path=result_path,
            task=sfn_tasks.InvokeFunction(
                load_cluster_configuration_lambda,
                payload={
                    'ClusterName': cluster_name,
                    'ClusterTags': [{'Key': t.key, 'Value': t.value} for t in cluster_tags],
                    'ProfileNamespace': profile_namespace,
                    'ProfileName': profile_name,
                    'ConfigurationNamespace': configuration_namespace,
                    'ConfigurationName': configuration_name,
                })
        )


class OverrideClusterConfigsBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *,
              override_cluster_configs_lambda: Optional[aws_lambda.Function] = None,
              allowed_cluster_config_overrides: Optional[Dict[str, str]] = None,
              output_path: str = '$',
              result_path: str = '$.ClusterConfig') -> sfn.Task:
        # We use a nested Construct to avoid collisions with Lambda and Task ids
        construct = core.Construct(scope, id)

        override_cluster_configs_lambda = \
            emr_lambdas.OverrideClusterConfigsBuilder.get_or_build(construct) \
            if override_cluster_configs_lambda is None \
            else override_cluster_configs_lambda

        return sfn.Task(
            construct, 'Override Cluster Configs',
            output_path=output_path,
            result_path=result_path,
            task=sfn_tasks.InvokeFunction(
                override_cluster_configs_lambda,
                payload={
                    'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                    'ClusterConfig': sfn.TaskInput.from_data_at('$.ClusterConfig').value,
                    'AllowedClusterConfigOverrides': allowed_cluster_config_overrides
                })
        )


class UpdateClusterTagsBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *,
              output_path: str = '$',
              result_path: str = '$.ClusterConfig') -> sfn.Task:
        # We use a nested Construct to avoid collisions with Lambda and Task ids
        construct = core.Construct(scope, id)

        update_cluster_tags_lambda = emr_lambdas.UpdateClusterTagsBuilder.get_or_build(construct)

        return sfn.Task(
            construct, 'Update Cluster Tags',
            output_path=output_path,
            result_path=result_path,
            task=sfn_tasks.InvokeFunction(
                update_cluster_tags_lambda,
                payload={
                    'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                    'ClusterConfig': sfn.TaskInput.from_data_at('$.ClusterConfig').value
                })
        )


class FailIfClusterRunningBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *, default_fail_if_cluster_running: bool) -> sfn.Task:
        # We use a nested Construct to avoid collisions with Lambda and Task ids
        construct = core.Construct(scope, id)

        fail_if_cluster_running_lambda = emr_lambdas.FailIfClusterRunningBuilder.get_or_build(construct)

        return sfn.Task(
            construct, 'Fail If Cluster Running',
            output_path='$',
            result_path='$',
            task=sfn_tasks.InvokeFunction(
                fail_if_cluster_running_lambda,
                payload={
                    'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                    'DefaultFailIfClusterRunning': default_fail_if_cluster_running,
                    'ClusterConfig': sfn.TaskInput.from_data_at('$.ClusterConfig').value
                })
        )


class CreateClusterBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *, roles: emr_roles.EMRRoles,
              result_path: Optional[str] = None, output_path: Optional[str] = None) -> sfn.Task:
        # We use a nested Construct to avoid collisions with Lambda and Task ids
        construct = core.Construct(scope, id)

        run_job_flow_lambda = emr_lambdas.RunJobFlowBuilder.get_or_build(construct, roles)

        return sfn.Task(
            construct, 'Start EMR Cluster',
            output_path=output_path,
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


class AddStepBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *,
              name: str, emr_step: emr_code.EMRStep, cluster_id: str,
              result_path: Optional[str] = None, output_path: Optional[str] = None) -> sfn.Task:
        # We use a nested Construct to avoid collisions with Lambda and Task ids
        construct = core.Construct(scope, id)

        add_job_flow_step_lambda = emr_lambdas.AddJobFlowStepBuilder.get_or_build(construct)

        return sfn.Task(
            construct, name,
            output_path=output_path,
            result_path=result_path,
            task=sfn_tasks.RunLambdaTask(
                add_job_flow_step_lambda,
                integration_pattern=sfn.ServiceIntegrationPattern.WAIT_FOR_TASK_TOKEN,
                payload={
                    'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                    'ClusterId': cluster_id,
                    'Step': emr_step.resolve(construct),
                    'TaskToken': sfn.Context.task_token
                }
            )
        )


class TerminateClusterBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *,
              name: str, cluster_id: str, result_path: Optional[str] = None,
              output_path: Optional[str] = None) -> sfn.Task:
        # We use a nested Construct to avoid collisions with Lambda and Task ids
        construct = core.Construct(scope, id)

        terminate_job_flow_lambda = emr_lambdas.TerminateJobFlowBuilder.get_or_build(construct)

        return sfn.Task(
            construct, name,
            output_path=output_path,
            result_path=result_path,
            task=sfn_tasks.RunLambdaTask(
                terminate_job_flow_lambda,
                integration_pattern=sfn.ServiceIntegrationPattern.WAIT_FOR_TASK_TOKEN,
                payload={
                    'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                    'ClusterId': cluster_id,
                    'TaskToken': sfn.Context.task_token
                }
            )
        )
