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

import jsii

from typing import Optional, Dict, List

from aws_cdk import (
    aws_events as events,
    aws_lambda,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    core
)

from aws_emr_launch.constructs.lambdas import emr_lambdas
from aws_emr_launch.constructs.emr_constructs import emr_code
from aws_emr_launch.constructs.iam_roles import emr_roles


class BaseTask:
    @staticmethod
    def get_resource_arn(
            service: str, api: str,
            integration_pattern: Optional[sfn.ServiceIntegrationPattern] = sfn.ServiceIntegrationPattern.SYNC) -> str:
        if not service or not api:
            raise ValueError('Both "service" and "api" are required to build the resource ARN')

        resource_arn_suffixes = {
            sfn.ServiceIntegrationPattern.FIRE_AND_FORGET: '',
            sfn.ServiceIntegrationPattern.SYNC: '.sync',
            sfn.ServiceIntegrationPattern.WAIT_FOR_TASK_TOKEN: '.waitForTaskToken'
        }

        return f'arn:{core.Aws.PARTITION}:states:::{service}:{api}{resource_arn_suffixes[integration_pattern]}'


@jsii.implements(sfn.IStepFunctionsTask)
class EmrCreateClusterTask(BaseTask):
    def __init__(self, roles: emr_roles.EMRRoles, cluster_configuration_path,
                 integration_pattern: Optional[sfn.ServiceIntegrationPattern] = sfn.ServiceIntegrationPattern.SYNC):
        self._roles = roles
        self._cluster_configuration_path = cluster_configuration_path
        self._integration_pattern = integration_pattern

        supported_patterns = [
            sfn.ServiceIntegrationPattern.SYNC,
            sfn.ServiceIntegrationPattern.WAIT_FOR_TASK_TOKEN
        ]

        if integration_pattern not in supported_patterns:
            raise ValueError(f'Invalid Service Integration Pattern: {integration_pattern}'
                             ' is not supported to call CreateCluster.')

    def _create_policy_statements(self, task: sfn.Task) -> List[iam.PolicyStatement]:
        stack = core.Stack.of(task)

        policy_statements = list()

        policy_statements.append(
            iam.PolicyStatement(
                actions=[
                    'elasticmapreduce:RunJobFlow',
                    'elasticmapreduce:DescribeCluster',
                    'elasticmapreduce:TerminateJobFlows'
                ],
                resources=['*']
            )
        )

        policy_statements.append(
            iam.PolicyStatement(
                actions=['iam:PassRole'],
                resources=[
                    self._roles.service_role.role_arn,
                    self._roles.instance_role.role_arn,
                    self._roles.autoscaling_role.role_arn
                ]
            )
        )

        if self._integration_pattern == sfn.ServiceIntegrationPattern.SYNC:
            policy_statements.append(
                iam.PolicyStatement(
                    actions=['events:PutTargets', 'events:PutRule', 'events:DescribeRule'],
                    resources=[stack.format_arn(
                        service='events',
                        resource='rule',
                        resource_name='StepFunctionsGetEventForEMRRunJobFlowRule'
                    )]
                )
            )

        return policy_statements

    def bind(self, task: sfn.Task) -> sfn.StepFunctionsTaskConfig:
        return sfn.StepFunctionsTaskConfig(
            resource_arn=self.get_resource_arn('elasticmapreduce', 'createCluster', self._integration_pattern),
            parameters={
                'AdditionalInfo': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.AdditionalInfo').value,
                'AmiVersion': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.AmiVersion').value,
                'Applications': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.Applications').value,
                'AutoScalingRole': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.AutoScalingRole').value,
                'BootstrapActions': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.BootstrapActions').value,
                'Configurations': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.Configurations').value,
                'CustomAmiId': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.CustomAmiId').value,
                'EbsRootVolumeSize': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.EbsRootVolumeSize').value,
                'Instances': {
                    'AdditionalMasterSecurityGroups': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.AdditionalMasterSecurityGroups').value,
                    'AdditionalSlaveSecurityGroups': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.AdditionalSlaveSecurityGroups').value,
                    'Ec2KeyName': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.Ec2KeyName').value,
                    'Ec2SubnetId': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.Ec2SubnetId').value,
                    'Ec2SubnetIds': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.Ec2SubnetIds').value,
                    'EmrManagedMasterSecurityGroup': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.EmrManagedMasterSecurityGroup').value,
                    'EmrManagedSlaveSecurityGroup': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.EmrManagedSlaveSecurityGroup').value,
                    'HadoopVersion': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.HadoopVersion').value,
                    'InstanceCount': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.InstanceCount').value,
                    'InstanceFleets': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.InstanceFleets').value,
                    'InstanceGroups': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.InstanceGroups').value,
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'MasterInstanceType': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.MasterInstanceType').value,
                    'Placement': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.Placement').value,
                    'ServiceAccessSecurityGroup': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.ServiceAccessSecurityGroup').value,
                    'SlaveInstanceType': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.SlaveInstanceType').value,
                    'TerminationProtected': sfn.TaskInput.from_data_at(
                        f'{self._cluster_configuration_path}.Instances.TerminationProtected').value,
                },
                'JobFlowRole': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.JobFlowRole').value,
                'KerberosAttributes': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.KerberosAttributes').value,
                'LogUri': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.LogUri').value,
                'Name': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.Name').value,
                'NewSupportedProducts': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.NewSupportedProducts').value,
                'ReleaseLabel': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.ReleaseLabel').value,
                'RepoUpgradeOnBoot': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.RepoUpgradeOnBoot').value,
                'ScaleDownBehavior': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.ScaleDownBehavior').value,
                'SecurityConfiguration': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.SecurityConfiguration').value,
                'ServiceRole': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.ServiceRole').value,
                'StepConcurrencyLevel': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.StepConcurrencyLevel').value,
                'SupportedProducts': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.SupportedProducts').value,
                'Tags': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.Tags').value,
                'VisibleToAllUsers': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.VisibleToAllUsers').value,
            },
            policy_statements=self._create_policy_statements(task)
        )


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
              result_path: str = '$.ClusterConfiguration') -> sfn.Task:
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
              cluster_configuration_path: str = '$.ClusterConfiguration.Cluster',
              output_path: str = '$',
              result_path: str = '$.ClusterConfiguration.Cluster') -> sfn.Task:
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
                    'ClusterConfiguration': sfn.TaskInput.from_data_at(cluster_configuration_path).value,
                    'AllowedClusterConfigOverrides': allowed_cluster_config_overrides
                })
        )


class FailIfClusterRunningBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *,
              default_fail_if_cluster_running: bool,
              cluster_configuration_path: str = '$.ClusterConfiguration.Cluster',
              output_path: str = '$',
              result_path: str = '$.ClusterConfiguration.Cluster') -> sfn.Task:
        # We use a nested Construct to avoid collisions with Lambda and Task ids
        construct = core.Construct(scope, id)

        fail_if_cluster_running_lambda = emr_lambdas.FailIfClusterRunningBuilder.get_or_build(construct)

        return sfn.Task(
            construct, 'Fail If Cluster Running',
            output_path=output_path,
            result_path=result_path,
            task=sfn_tasks.InvokeFunction(
                fail_if_cluster_running_lambda,
                payload={
                    'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                    'DefaultFailIfClusterRunning': default_fail_if_cluster_running,
                    'ClusterConfiguration': sfn.TaskInput.from_data_at(cluster_configuration_path).value
                })
        )


class UpdateClusterTagsBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *,
              cluster_configuration_path: str = '$.ClusterConfiguration.Cluster',
              output_path: str = '$',
              result_path: str = '$.ClusterConfiguration.Cluster') -> sfn.Task:
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
                    'ClusterConfiguration': sfn.TaskInput.from_data_at(cluster_configuration_path).value
                })
        )


class CreateClusterBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *,
              roles: emr_roles.EMRRoles,
              cluster_configuration_path: str = '$.ClusterConfiguration.Cluster',
              result_path: Optional[str] = None,
              output_path: Optional[str] = None) -> sfn.Task:
        # We use a nested Construct to avoid collisions with Lambda and Task ids
        construct = core.Construct(scope, id)

        return sfn.Task(
            construct, 'Start EMR Cluster',
            output_path=output_path,
            result_path=result_path,
            task=EmrCreateClusterTask(
                roles=roles,
                cluster_configuration_path=cluster_configuration_path,
                integration_pattern=sfn.ServiceIntegrationPattern.SYNC
            )
        )


class RunJobFlowBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *, roles: emr_roles.EMRRoles,
              kerberos_attributes_secret: Optional[secretsmanager.Secret] = None,
              secret_configurations: Optional[Dict[str, secretsmanager.Secret]] = None,
              cluster_configuration_path: str = '$.ClusterConfiguration',
              result_path: Optional[str] = None,
              output_path: Optional[str] = None) -> sfn.Task:
        # We use a nested Construct to avoid collisions with Lambda and Task ids
        construct = core.Construct(scope, id)

        event_rule = core.Stack.of(scope).node.try_find_child('EventRule')
        event_rule = events.Rule(
            construct, 'EventRule',
            enabled=False,
            schedule=events.Schedule.rate(core.Duration.minutes(1))) if event_rule is None else event_rule

        run_job_flow_lambda = emr_lambdas.RunJobFlowBuilder.get_or_build(construct, roles, event_rule)
        check_cluster_status_lambda = emr_lambdas.CheckClusterStatusBuilder.get_or_build(construct, event_rule)

        if kerberos_attributes_secret:
            kerberos_attributes_secret.grant_read(run_job_flow_lambda)

        if secret_configurations is not None:
            for secret in secret_configurations.values():
                secret.grant_read(run_job_flow_lambda)

        return sfn.Task(
            construct, 'Start EMR Cluster (with Secrets)',
            output_path=output_path,
            result_path=result_path,
            task=sfn_tasks.RunLambdaTask(
                run_job_flow_lambda,
                integration_pattern=sfn.ServiceIntegrationPattern.WAIT_FOR_TASK_TOKEN,
                payload={
                    'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                    'ClusterConfiguration': sfn.TaskInput.from_data_at(cluster_configuration_path).value,
                    'TaskToken': sfn.Context.task_token,
                    'CheckStatusLambda': check_cluster_status_lambda.function_arn,
                    'RuleName': event_rule.rule_name
                })
        )


class AddStepBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *,
              emr_step: emr_code.EMRStep,
              cluster_id: str,
              result_path: Optional[str] = None,
              output_path: Optional[str] = None) -> sfn.Task:
        # We use a nested Construct to avoid collisions with Task ids
        construct = core.Construct(scope, id)
        resolved_step = emr_step.resolve(construct)

        return sfn.Task(
            construct, emr_step.name,
            output_path=output_path,
            result_path=result_path,
            task=sfn_tasks.EmrAddStep(
                cluster_id=cluster_id,
                name=resolved_step['Name'],
                action_on_failure=sfn_tasks.ActionOnFailure[resolved_step['ActionOnFailure']],
                jar=resolved_step['HadoopJarStep']['Jar'],
                main_class=resolved_step['HadoopJarStep']['MainClass'],
                args=resolved_step['HadoopJarStep']['Args'],
                properties=resolved_step['HadoopJarStep']['Properties'],
                integration_pattern=sfn.ServiceIntegrationPattern.SYNC)
        )


class TerminateClusterBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *,
              name: str,
              cluster_id: str,
              result_path: Optional[str] = None,
              output_path: Optional[str] = None) -> sfn.Task:
        # We use a nested Construct to avoid collisions with Task ids
        construct = core.Construct(scope, id)

        return sfn.Task(
            construct, name,
            output_path=output_path,
            result_path=result_path,
            task=sfn_tasks.EmrTerminateCluster(
                cluster_id=cluster_id,
                integration_pattern=sfn.ServiceIntegrationPattern.SYNC
            )
        )
