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

import json
import boto3

from typing import Optional, Mapping
from botocore.exceptions import ClientError

from aws_cdk import (
    aws_lambda,
    aws_sns as sns,
    aws_ssm as ssm,
    aws_stepfunctions as sfn,
    core
)

from . import (
    emr_chains,
    emr_tasks
)

from ..emr_constructs.cluster_configuration import ClusterConfiguration

SSM_PARAMETER_PREFIX = '/emr_launch/emr_launch_functions'


class EMRLaunchFunctionNotFoundError(Exception):
    pass


class EMRLaunchFunction(core.Construct):
    def __init__(self, scope: core.Construct, id: str, *,
                 launch_function_name: str,
                 cluster_configuration: Optional[ClusterConfiguration] = None,
                 namespace: str = 'default',
                 default_fail_if_cluster_running: bool = False,
                 success_topic: Optional[sns.Topic] = None,
                 failure_topic: Optional[sns.Topic] = None,
                 override_cluster_configs_lambda: Optional[aws_lambda.Function] = None,
                 allowed_cluster_config_overrides: Optional[Mapping[str, str]] = None,
                 description: Optional[str] = None) -> None:
        super().__init__(scope, id)

        if cluster_configuration is None:
            return

        self._launch_function_name = launch_function_name
        self._namespace = namespace
        self._cluster_configuration = cluster_configuration
        self._default_fail_if_cluster_running = default_fail_if_cluster_running
        self._success_topic = success_topic
        self._failure_topic = failure_topic
        self._override_cluster_configs_lambda = override_cluster_configs_lambda
        self._allowed_cluster_config_overrides = allowed_cluster_config_overrides
        self._description = description

        fail = emr_chains.Fail(
            self, 'FailChain',
            message=sfn.TaskInput.from_data_at('$.Error'),
            subject='EMR Launch Function Failure',
            topic=failure_topic).chain

        # Create Task for overriding cluster configurations
        override_cluster_configs = emr_tasks.OverrideClusterConfigs(
            self, 'OverrideClusterConfigsChain',
            cluster_config=cluster_configuration.config,
            override_cluster_configs_lambda=override_cluster_configs_lambda,
            allowed_cluster_config_overrides=allowed_cluster_config_overrides).task
        # Attach an error catch to the Task
        override_cluster_configs.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

        # Create a Task for updating the cluster tags at runtime
        update_cluster_tags = emr_tasks.UpdateClusterTags(
            self, 'UpdateClusterTagsChain').task
        # Attach an error catch to the Task
        update_cluster_tags.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

        # Create Task to conditionally fail if a cluster with this name is already
        # running, based on user input
        fail_if_cluster_running = emr_tasks.FailIfClusterRunning(
            self, 'FailIfClusterRunningChain',
            default_fail_if_cluster_running=default_fail_if_cluster_running).task
        # Attach an error catch to the task
        fail_if_cluster_running.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

        # Create a Task to create the cluster
        create_cluster = emr_tasks.CreateCluster(
            self, 'CreateClusterChain', result_path='$.LaunchClusterResult').task
        # Attach an error catch to the Task
        create_cluster.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

        success = emr_chains.Success(
            self, 'SuccessChain',
            message=sfn.TaskInput.from_data_at('$.LaunchClusterResult'),
            subject='Launch EMR Config Succeeded',
            topic=success_topic,
            output_path='$').chain

        definition = sfn.Chain \
            .start(override_cluster_configs) \
            .next(update_cluster_tags) \
            .next(fail_if_cluster_running) \
            .next(create_cluster) \
            .next(success)

        self._state_machine = sfn.StateMachine(
            self, 'StateMachine',
            state_machine_name=launch_function_name, definition=definition)

        self._ssm_parameter = ssm.StringParameter(
            self, 'SSMParameter',
            string_value=self._property_values_to_json(),
            parameter_name=f'{SSM_PARAMETER_PREFIX}/{namespace}/{launch_function_name}')

    def _property_values_to_json(self):
        return json.dumps({
            'LaunchFunctionName': self._launch_function_name,
            'Namespace': self._namespace,
            'ClusterConfiguration':
                f'{self._cluster_configuration.namespace}/{self._cluster_configuration.configuration_name}',
            'DefaultFailIfClusterRunning': self._default_fail_if_cluster_running,
            'SuccessTopic': self._success_topic.topic_arn if self._success_topic is not None else None,
            'FailureTopic': self._failure_topic.topic_arn if self._failure_topic is not None else None,
            'OverrideClusterConfigsLambda':
                self._override_cluster_configs_lambda.function_arn
                if self._override_cluster_configs_lambda is not None
                else None,
            'AllowedClusterConfigOverrides': self._allowed_cluster_config_overrides,
            'StateMachine': self._state_machine.state_machine_arn,
            'Description': self._description
        })

    def _property_values_from_json(self, property_values):
        self._launch_function_name = property_values['LaunchFunctionName']
        self._namespace = property_values['Namespace']

        config_parts = property_values['ClusterConfiguration'].split('/')
        self._cluster_configuration = ClusterConfiguration.from_stored_configuration(
            self, 'ClusterConfiguration', config_parts[1], config_parts[0])

        self._default_fail_if_cluster_running = property_values['DefaultFailIfClusterRunning']

        topic = property_values.get('SuccessTopic', None)
        self._success_topic = sns.Topic.from_topic_arn(self, 'SuccessTopic', topic) \
            if topic is not None \
            else None

        topic = property_values.get('FailureTopic', None)
        self._failure_topic = sns.Topic.from_topic_arn(self, 'FailureTopic', topic) \
            if topic is not None \
            else None

        func = property_values.get('OverrideClusterConfigsLambda', None)
        self._override_cluster_configs_lambda = aws_lambda.Function.from_function_arn(
            self, 'OverrideClusterConfigsLambda', func) \
            if func is not None \
            else None

        self._allowed_cluster_config_overrides = property_values.get('AllowedClusterConfigOverrides', None)
        self._description = property_values.get('Description', None)

        state_machine = property_values['StateMachine']
        self._state_machine = sfn.StateMachine.from_state_machine_arn(self, 'StateMachine', state_machine)

        return self

    @property
    def launch_function_name(self) -> str:
        return self._launch_function_name

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def cluster_configuration(self) -> ClusterConfiguration:
        return self._cluster_configuration

    @property
    def default_fail_if_cluster_running(self) -> bool:
        return self._default_fail_if_cluster_running

    @property
    def success_topic(self) -> sns.Topic:
        return self._success_topic

    @property
    def failure_topic(self) -> sns.Topic:
        return self._failure_topic

    @property
    def override_cluster_configs_lambda(self) -> aws_lambda.Function:
        return self._override_cluster_configs_lambda

    @property
    def allowed_cluster_config_overrides(self) -> Mapping[str, str]:
        return self._allowed_cluster_config_overrides

    @property
    def state_machine(self) -> sfn.StateMachine:
        return self._state_machine

    @property
    def description(self) -> str:
        return self._description

    @staticmethod
    def get_functions(namespace: str = 'default', next_token: Optional[str] = None) -> Mapping[str, any]:
        params = {
            'Path': f'{SSM_PARAMETER_PREFIX}/{namespace}/'
        }
        if next_token:
            params['NextToken'] = next_token
        result = boto3.client('ssm').get_parameters_by_path(**params)

        functions = {
            'EMRLaunchFunctions': [json.loads(p['Value']) for p in result['Parameters']]
        }
        if 'NextToken' in result:
            functions['NextToken'] = result['NextToken']
        return functions

    @staticmethod
    def get_function(launch_function_name: str, namespace: str = 'default') -> Mapping[str, any]:
        try:
            function_json = boto3.client('ssm').get_parameter(
                Name=f'{SSM_PARAMETER_PREFIX}/{namespace}/{launch_function_name}')['Parameter']['Value']
            return json.loads(function_json)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ParameterNotFound':
                raise EMRLaunchFunctionNotFoundError()

    @staticmethod
    def from_stored_function(scope: core.Construct, id: str, launch_function_name: str, namespace: str = 'default'):
        stored_function = EMRLaunchFunction.get_function(launch_function_name, namespace)
        launch_function = EMRLaunchFunction(scope, id, launch_function_name=launch_function_name, namespace=namespace)
        return launch_function._property_values_from_json(stored_function)
