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
from ..emr_constructs.cluster_configurations import BaseConfiguration


class EMRLaunchFunctionNotFoundError(Exception):
    pass


class EMRLaunchFunction(core.Construct):
    def __init__(self, scope: core.Construct, id: str, *,
                 cluster_config: Optional[BaseConfiguration] = None,
                 launch_function_name: Optional[str] = None,
                 default_fail_if_job_running: bool = False,
                 success_topic: Optional[sns.Topic] = None,
                 failure_topic: Optional[sns.Topic] = None,
                 override_cluster_configs_lambda: Optional[aws_lambda.Function] = None,
                 allowed_cluster_config_overrides: Optional[Mapping[str, str]] = None) -> None:
        super().__init__(scope, id)

        if cluster_config is None:
            return

        self._allowed_cluster_config_overrides = allowed_cluster_config_overrides

        override_cluster_configs_task = EMRFragments.override_cluster_configs_task(
            self, cluster_config=cluster_config.config,
            override_cluster_configs_lambda=override_cluster_configs_lambda,
            allowed_cluster_config_overrides=allowed_cluster_config_overrides)

        fail_if_job_running_task = EMRFragments.fail_if_job_running_task(
            self, default_fail_if_job_running=default_fail_if_job_running)

        run_job_flow_task = EMRFragments.run_job_flow_task(self)

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

        definition = \
            override_cluster_configs_task.add_catch(fail, errors=['States.ALL'], result_path='$.Error') \
            .next(fail_if_job_running_task.add_catch(fail, errors=['States.ALL'], result_path='$.Error')) \
            .next(run_job_flow_task.add_catch(fail, errors=['States.ALL'], result_path='$.Error')) \
            .next(success)

        self._state_machine = sfn.StateMachine(
            self, 'StateMachine',
            state_machine_name=launch_function_name, definition=definition)

        if launch_function_name is not None:
            self._ssm_parameter = ssm.StringParameter(
                self, 'SSMParameter',
                string_value=json.dumps({
                    'AllowedClusterConfigOverrides': self._allowed_cluster_config_overrides,
                    'FunctionArn': self._state_machine.state_machine_arn
                }),
                parameter_name='/emr_launch/control_plane/emr_launch_functions/{}'.format(
                    launch_function_name))

    @property
    def allowed_cluster_config_overrides(self) -> Mapping[str, str]:
        return self._allowed_cluster_config_overrides

    @property
    def state_machine(self) -> sfn.StateMachine:
        return self._state_machine

    @staticmethod
    def from_stored_config(scope: core.Construct, id: str, launch_function_name: str):
        try:
            function_json = boto3.client('ssm', region_name=core.Stack.of(scope).region).get_parameter(
                Name='/emr_launch/control_plane/emr_launch_functions/{}'.format(launch_function_name))['Parameter']['Value']
            launch_function = EMRLaunchFunction(scope, id)
            stored_config = json.loads(function_json)
            launch_function._allowed_cluster_config_overrides = stored_config['AllowedClusterConfigOverrides']
            launch_function._state_machine = sfn.StateMachine.from_state_machine_arn(
                launch_function, 'StateMachine', stored_config['FunctionArn'])
            return launch_function
        except ClientError as e:
            if e.response['Error']['Code'] == 'ParameterNotFound':
                raise EMRLaunchFunctionNotFoundError()
