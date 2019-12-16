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

from .emr_fragments import EMRFragments
from ..emr_constructs.cluster_configuration import BaseConfiguration

SSM_PARAMETER_PREFIX = '/emr_launch/emr_launch_functions'


class EMRLaunchFunctionNotFoundError(Exception):
    pass


class EMRLaunchFunction(core.Construct):
    def __init__(self, scope: core.Construct, id: str, *,
                 cluster_config: Optional[BaseConfiguration] = None,
                 launch_function_name: Optional[str] = None,
                 namespace: str = 'default',
                 default_fail_if_job_running: bool = False,
                 success_topic: Optional[sns.Topic] = None,
                 failure_topic: Optional[sns.Topic] = None,
                 override_cluster_configs_lambda: Optional[aws_lambda.Function] = None,
                 allowed_cluster_config_overrides: Optional[Mapping[str, str]] = None) -> None:
        super().__init__(scope, id)

        if cluster_config is None:
            return

        self._allowed_cluster_config_overrides = allowed_cluster_config_overrides

        fail = EMRFragments.fail_fragment(
            self,
            message=sfn.TaskInput.from_data_at('$.Error'),
            subject='Launch EMR Config Failure',
            topic=failure_topic)

        success = EMRFragments.success_fragment(
            self,
            message=sfn.TaskInput.from_data_at('$.Result'),
            subject='Launch EMR Config Succeeded',
            topic=success_topic)

        # Create Task for overriding cluster configurations
        override_cluster_configs_task = EMRFragments.override_cluster_configs_task(
            self, cluster_config=cluster_config.config,
            override_cluster_configs_lambda=override_cluster_configs_lambda,
            allowed_cluster_config_overrides=allowed_cluster_config_overrides)
        # Attach an error catch to the Task
        override_cluster_configs_task.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

        # Create Task to conditionally fail if a cluster with this name is already
        # running, based on user input
        fail_if_job_running_task = EMRFragments.fail_if_job_running_task(
            self, default_fail_if_job_running=default_fail_if_job_running)
        # Attach an error catch to the task
        fail_if_job_running_task.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

        # Create a Task to create the cluster
        create_cluster_task = EMRFragments.create_cluster_task(self)
        # Attach an error catch to the Task
        create_cluster_task.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

        definition = sfn.Chain \
            .start(override_cluster_configs_task) \
            .next(fail_if_job_running_task) \
            .next(create_cluster_task) \
            .next(success)

        self._state_machine = sfn.StateMachine(
            self, 'StateMachine',
            state_machine_name=launch_function_name, definition=definition)

        if launch_function_name is not None:
            self._ssm_parameter = ssm.StringParameter(
                self, 'SSMParameter',
                string_value=json.dumps({
                    'LaunchFunctionName': launch_function_name,
                    'ClusterConfiguration': cluster_config.cluster_name,
                    'DefaultFailIfJobRunning': default_fail_if_job_running,
                    'SuccessTopic': success_topic.topic_arn if success_topic is not None else None,
                    'FailureTopic': failure_topic.topic_arn if failure_topic is not None else None,
                    'OverrideClusterConfigsLambda':
                        override_cluster_configs_lambda.function_arn
                        if override_cluster_configs_lambda is not None
                        else None,
                    'AllowedClusterConfigOverrides': self._allowed_cluster_config_overrides,
                    'StateMachineArn': self._state_machine.state_machine_arn
                }),
                parameter_name=f'{SSM_PARAMETER_PREFIX}/{namespace}/{launch_function_name}')

    @property
    def allowed_cluster_config_overrides(self) -> Mapping[str, str]:
        return self._allowed_cluster_config_overrides

    @property
    def state_machine(self) -> sfn.StateMachine:
        return self._state_machine

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
