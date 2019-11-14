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
    aws_lambda,
    aws_sns as sns,
    aws_stepfunctions as sfn,
    core
)

from .emr_fragments import EMRFragments
from ..emr_constructs.cluster_configurations import BaseConfiguration


class LaunchEMRConfig(core.Construct):
    def __init__(self, scope: core.Construct, id: str, *,
                 cluster_config: BaseConfiguration,
                 launch_config_name: Optional[str] = None,
                 default_fail_if_job_running: bool = False,
                 success_topic: Optional[sns.Topic] = None,
                 failure_topic: Optional[sns.Topic] = None,
                 override_cluster_configs_lambda: Optional[aws_lambda.Function] = None,
                 allowed_cluster_config_overrides: Optional[Mapping[str, str]] = None) -> None:
        super().__init__(scope, id)

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
            state_machine_name=launch_config_name, definition=definition)

    @property
    def state_machine(self) -> sfn.StateMachine:
        return self._state_machine
