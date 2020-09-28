import typing
from typing import Any, Dict, List, Mapping, Optional

import jsii
from aws_cdk import aws_events as events
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks
from aws_cdk import core

from aws_emr_launch.constructs.base import BaseBuilder
from aws_emr_launch.constructs.emr_constructs import emr_code
from aws_emr_launch.constructs.iam_roles import emr_roles
from aws_emr_launch.constructs.lambdas import emr_lambdas


class BaseTask(sfn.TaskStateBase):
    @staticmethod
    def get_resource_arn(
            service: str, api: str,
            integration_pattern: Optional[sfn.IntegrationPattern] = sfn.IntegrationPattern.RUN_JOB) -> str:
        if not service or not api:
            raise ValueError('Both "service" and "api" are required to build the resource ARN')

        resource_arn_suffixes = {
            sfn.IntegrationPattern.REQUEST_RESPONSE: '',
            sfn.IntegrationPattern.RUN_JOB: '.sync',
            sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN: '.waitForTaskToken'
        }

        return f'arn:{core.Aws.PARTITION}:states:::{service}:{api}{resource_arn_suffixes[integration_pattern]}'


class StartExecutionTask(BaseTask):
    def __init__(self, scope: core.Construct, id: str, *,
                 comment: Optional[str] = None,
                 heartbeat: Optional[core.Duration] = None,
                 input_path: Optional[str] = None,
                 integration_pattern: Optional[sfn.IntegrationPattern] = None,
                 output_path: Optional[str] = None,
                 result_path: Optional[str] = None,
                 timeout: Optional[core.Duration] = None,
                 state_machine: sfn.StateMachine,
                 input: Optional[Dict[str, any]] = None, name: Optional[str] = None,):

        super().__init__(scope, id,
                         comment=comment,
                         heartbeat=heartbeat,
                         input_path=input_path,
                         integration_pattern=integration_pattern,
                         output_path=output_path,
                         result_path=result_path,
                         timeout=timeout)

        self._state_machine = state_machine
        self._input = input
        self._name = name
        self._integration_pattern = integration_pattern
        self._metrics = None
        self._statements = self._create_policy_statements()

    def _create_policy_statements(self) -> List[iam.PolicyStatement]:
        stack = core.Stack.of(self)

        policy_statements = list()

        policy_statements.append(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=['states:StartExecution'],
                resources=[self._state_machine.state_machine_arn]
            )
        )

        if self._integration_pattern == sfn.IntegrationPattern.RUN_JOB:
            policy_statements.append(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['states:DescribeExecution', 'states:StopExecution'],
                    resources=['*']
                )
            )

            policy_statements.append(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['events:PutTargets', 'events:PutRule', 'events:DescribeRule'],
                    resources=[stack.format_arn(
                        service='events',
                        resource='rule',
                        resource_name='StepFunctionsGetEventsForStepFunctionsExecutionRule'
                    )]
                )
            )

        return policy_statements

    def _task_metrics(self) -> Optional[sfn.TaskMetricsConfig]:
        return self._metrics

    def _task_policies(self) -> List[iam.PolicyStatement]:
        return self._statements

    def to_state_json(self) -> Mapping[Any, Any]:
        input = self._input if self._input is not None else sfn.TaskInput.from_context_at('$$.Execution.Input').value
        return {
            'ResourceArn': self.get_resource_arn('states', 'startExecution', self._integration_pattern),
            'Parameters': sfn.FieldUtils.render_object({
                'StateMachineArn': self._state_machine.state_machine_arn,
                'Input': input,
                'Name': self._name
            }),
        }


class EmrCreateClusterTask(BaseTask):
    def __init__(self, scope: core.Construct, id: str, *,
                 comment: Optional[str] = None,
                 heartbeat: Optional[core.Duration] = None,
                 input_path: Optional[str] = None,
                 integration_pattern: Optional[sfn.IntegrationPattern] = None,
                 output_path: Optional[str] = None,
                 result_path: Optional[str] = None,
                 timeout: Optional[core.Duration] = None,
                 roles: emr_roles.EMRRoles, cluster_configuration_path,):
        super().__init__(scope, id,
                         comment=comment,
                         heartbeat=heartbeat,
                         input_path=input_path,
                         integration_pattern=integration_pattern,
                         output_path=output_path,
                         result_path=result_path,
                         timeout=timeout)

        self._roles = roles
        self._cluster_configuration_path = cluster_configuration_path
        self._integration_pattern = integration_pattern
        self._metrics = None
        self._statements = self._create_policy_statements()

    def _create_policy_statements(self) -> List[iam.PolicyStatement]:
        stack = core.Stack.of(self)

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

        if self._integration_pattern == sfn.IntegrationPattern.RUN_JOB:
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

    def _task_metrics(self) -> Optional[sfn.TaskMetricsConfig]:
        return self._metrics

    def _task_policies(self) -> List[iam.PolicyStatement]:
        return self._statements

    def to_state_json(self) -> Mapping[Any, Any]:
        return {
            'ResourceArn': self.get_resource_arn('elasticmapreduce', 'createCluster', self._integration_pattern),
            'Parameters': sfn.FieldUtils.render_object({
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
                'ManagedScalingPolicy': sfn.TaskInput.from_data_at(
                    f'{self._cluster_configuration_path}.ManagedScalingPolicy').value,
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
            }),
        }


class EmrAddStepTask(BaseTask):
    def __init__(self, scope: core.Construct, id: str, *,
                 comment: Optional[str] = None,
                 heartbeat: Optional[core.Duration] = None,
                 input_path: Optional[str] = None,
                 integration_pattern: Optional[sfn.IntegrationPattern] = None,
                 output_path: Optional[str] = None,
                 result_path: Optional[str] = None,
                 timeout: Optional[core.Duration] = None,
                 cluster_id: str, step: Dict[str, any],):
        super().__init__(scope, id,
                         comment=comment,
                         heartbeat=heartbeat,
                         input_path=input_path,
                         integration_pattern=integration_pattern,
                         output_path=output_path,
                         result_path=result_path,
                         timeout=timeout)

        self._cluster_id = cluster_id
        self._step = step
        self._integration_pattern = integration_pattern
        self._metrics = None
        self._statements = self._create_policy_statements()

    def _create_policy_statements(self) -> List[iam.PolicyStatement]:
        stack = core.Stack.of(self)

        policy_statements = list()

        policy_statements.append(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    'elasticmapreduce:AddJobFlowSteps',
                    'elasticmapreduce:DescribeStep',
                    'elasticmapreduce:CancelSteps'
                ],
                resources=[f'arn:aws:elasticmapreduce:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:cluster/*']
            )
        )

        if self._integration_pattern == sfn.IntegrationPattern.RUN_JOB:
            policy_statements.append(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['events:PutTargets', 'events:PutRule', 'events:DescribeRule'],
                    resources=[stack.format_arn(
                        service='events',
                        resource='rule',
                        resource_name='StepFunctionsGetEventForEMRAddJobFlowStepsRule'
                    )]
                )
            )

        return policy_statements

    def _task_metrics(self) -> Optional[sfn.TaskMetricsConfig]:
        return self._metrics

    def _task_policies(self) -> List[iam.PolicyStatement]:
        return self._statements

    def to_state_json(self) -> Mapping[Any, Any]:
        return {
            'ResourceArn': self.get_resource_arn('elasticmapreduce', 'addStep', self._integration_pattern),
            'Parameters': sfn.FieldUtils.render_object({
                'ClusterId': self._cluster_id,
                'Step': self._step
            }),
        }


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

        return sfn_tasks.LambdaInvoke(
            construct, 'Load Cluster Configuration',
            output_path=output_path,
            result_path=result_path,
            lambda_function=load_cluster_configuration_lambda,
            payload=sfn.TaskInput.from_object({
                'ClusterName': cluster_name,
                'ClusterTags': [{'Key': t.key, 'Value': t.value} for t in cluster_tags],
                'ProfileNamespace': profile_namespace,
                'ProfileName': profile_name,
                'ConfigurationNamespace': configuration_namespace,
                'ConfigurationName': configuration_name,
            }),
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

        return sfn_tasks.LambdaInvoke(
            construct, 'Override Cluster Configs',
            output_path=output_path,
            result_path=result_path,
            lambda_function=override_cluster_configs_lambda,
            payload=sfn.TaskInput.from_object({
                'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                'ClusterConfiguration': sfn.TaskInput.from_data_at(cluster_configuration_path).value,
                'AllowedClusterConfigOverrides': allowed_cluster_config_overrides
            }),
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

        return sfn_tasks.LambdaInvoke(
            construct, 'Fail If Cluster Running',
            output_path=output_path,
            result_path=result_path,
            lambda_function=fail_if_cluster_running_lambda,
            payload=sfn.TaskInput.from_object({
                'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                'DefaultFailIfClusterRunning': default_fail_if_cluster_running,
                'ClusterConfiguration': sfn.TaskInput.from_data_at(cluster_configuration_path).value
            }),
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

        return sfn_tasks.LambdaInvoke(
            construct, 'Update Cluster Tags',
            output_path=output_path,
            result_path=result_path,
            lambda_function=update_cluster_tags_lambda,
            payload=sfn.TaskInput.from_object({
                'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                'ClusterConfiguration': sfn.TaskInput.from_data_at(cluster_configuration_path).value
            }),
        )


class CreateClusterBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *,
              roles: emr_roles.EMRRoles,
              cluster_configuration_path: str = '$.ClusterConfiguration.Cluster',
              result_path: Optional[str] = None,
              output_path: Optional[str] = None,
              wait_for_cluster_start: bool = True) -> sfn.Task:
        # We use a nested Construct to avoid collisions with Lambda and Task ids
        construct = core.Construct(scope, id)

        integration_pattern = sfn.IntegrationPattern.RUN_JOB if wait_for_cluster_start \
            else sfn.IntegrationPattern.REQUEST_RESPONSE

        return EmrCreateClusterTask(
            construct, 'Start EMR Cluster',
            output_path=output_path,
            result_path=result_path,
            roles=roles,
            cluster_configuration_path=cluster_configuration_path,
            integration_pattern=integration_pattern,
        )


class RunJobFlowBuilder(BaseBuilder):
    @staticmethod
    def build(scope: core.Construct, id: str, *, roles: emr_roles.EMRRoles,
              kerberos_attributes_secret: Optional[secretsmanager.Secret] = None,
              secret_configurations: Optional[Dict[str, secretsmanager.Secret]] = None,
              cluster_configuration_path: str = '$.ClusterConfiguration',
              result_path: Optional[str] = None,
              output_path: Optional[str] = None,
              wait_for_cluster_start: bool = True) -> sfn.Task:
        # We use a nested Construct to avoid collisions with Lambda and Task ids
        construct = core.Construct(scope, id)

        event_rule = core.Stack.of(scope).node.try_find_child('EventRule')
        if event_rule is None:
            event_rule = events.Rule(
                construct, 'EventRule',
                enabled=False,
                schedule=events.Schedule.rate(core.Duration.minutes(1)))
            BaseBuilder.tag_construct(event_rule)

        run_job_flow_lambda = emr_lambdas.RunJobFlowBuilder.get_or_build(construct, roles, event_rule)
        check_cluster_status_lambda = emr_lambdas.CheckClusterStatusBuilder.get_or_build(construct, event_rule)

        if kerberos_attributes_secret:
            run_job_flow_lambda.add_to_role_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=['secretsmanager:GetSecretValue'],
                resources=[f'{kerberos_attributes_secret.secret_arn}*']
            ))

        if secret_configurations is not None:
            for secret in secret_configurations.values():
                run_job_flow_lambda.add_to_role_policy(iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['secretsmanager:GetSecretValue'],
                    resources=[f'{secret.secret_arn}*']
                ))

        return sfn_tasks.LambdaInvoke(
            construct, 'Start EMR Cluster (with Secrets)',
            output_path=output_path,
            result_path=result_path,
            lambda_function=run_job_flow_lambda,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            payload=sfn.TaskInput.from_object({
                'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                'ClusterConfiguration': sfn.TaskInput.from_data_at(cluster_configuration_path).value,
                'TaskToken': sfn.Context.task_token,
                'CheckStatusLambda': check_cluster_status_lambda.function_arn,
                'RuleName': event_rule.rule_name,
                'FireAndForget': not wait_for_cluster_start
            })
        )


class AddStepBuilder:
    @staticmethod
    def build(scope: core.Construct, id: str, *,
              emr_step: emr_code.EMRStep,
              cluster_id: str,
              result_path: Optional[str] = None,
              output_path: Optional[str] = None,
              wait_for_step_completion: bool = True) -> sfn.Task:
        # We use a nested Construct to avoid collisions with Task ids
        construct = core.Construct(scope, id)
        resolved_step = emr_step.resolve(construct)

        integration_pattern = sfn.IntegrationPattern.RUN_JOB if wait_for_step_completion \
            else sfn.IntegrationPattern.REQUEST_RESPONSE

        return EmrAddStepTask(
            construct, emr_step.name,
            output_path=output_path,
            result_path=result_path,
            cluster_id=cluster_id,
            step=resolved_step,
            integration_pattern=integration_pattern,
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

        return sfn_tasks.EmrTerminateCluster(
            construct, name,
            output_path=output_path,
            result_path=result_path,
            cluster_id=cluster_id,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )
