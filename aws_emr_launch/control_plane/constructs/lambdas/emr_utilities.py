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

from typing import Mapping

from aws_cdk import (
    aws_lambda,
    aws_iam as iam,
    core
)

from . import _lambda_path


class EMRUtilities(core.Construct):

    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        code = aws_lambda.Code.asset(_lambda_path('emr_utilities'))

        self._shared_functions = {}
        self._shared_layers = {}

        self._shared_functions['EMRLaunch_EMRUtilities_FailIfJobRunning'] = aws_lambda.Function(
            self,
            'FailIfJobRunning',
            function_name='EMRLaunch_EMRUtilities_FailIfJobRunning',
            code=code,
            handler='fail_if_job_running.handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'elasticmapreduce:ListClusters'
                    ],
                    resources=['*']
                )
            ]
        )

        self._shared_functions['EMRLaunch_EMRUtilities_RunJobFlow'] = aws_lambda.Function(
            self,
            'AddJobFlow',
            function_name='EMRLaunch_EMRUtilities_RunJobFlow',
            code=code,
            handler='run_job_flow.handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'elasticmapreduce:RunJobFlow',
                        'iam:PassRole',
                        'ssm:PutParameter'
                    ],
                    resources=['*']
                )
            ]
        )

        self._shared_functions['EMRLaunch_EMRUtilities_AddJobFlowSteps'] = aws_lambda.Function(
            self,
            'AddJobFlowSteps',
            function_name='EMRLaunch_EMRUtilities_AddJobFlowSteps',
            code=code,
            handler='add_job_flow_steps.handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'elasticmapreduce:DescribeCluster',
                        'elasticmapreduce:AddTags',
                        'elasticmapreduce:AddJobFlowSteps'
                    ],
                    resources=['*']
                )
            ]
        )

        emr_config_utils_layer = aws_lambda.LayerVersion(
            self,
            'EMRConfigUtilsLayer',
            layer_version_name='EMRLaunch_EMRUtilities_EMRConfigUtilsLayer',
            code=aws_lambda.Code.asset(_lambda_path('layers/emr_config_utils')),
            compatible_runtimes=[
                aws_lambda.Runtime.PYTHON_3_7
            ],
            description='EMR configuration utility functions'
        )
        self._shared_layers['EMRLaunch_EMRUtilities_EMRConfigUtilsLayer'] = emr_config_utils_layer

        self._shared_functions['EMRLaunch_EMRUtilities_OverrideClusterConfigs'] = aws_lambda.Function(
            self,
            'OverrideClusterConfigs',
            function_name='EMRLaunch_EMRUtilities_OverrideClusterConfigs',
            code=code,
            handler='override_cluster_configs.handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            layers=[emr_config_utils_layer]
        )

        self._cluster_state_change_event = aws_lambda.Function(
            self,
            'ClusterStateChangeEvent',
            function_name='EMRLaunch_EMRUtilities_ClusterStateChangeEvent',
            code=code,
            handler='cluster_state_change_event.handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'ssm:GetParameter',
                        'ssm:DeleteParameter',
                        'states:SendTaskSuccess',
                        'states:SendTaskHeartbeat',
                        'states:SendTaskFailure'
                    ],
                    resources=['*']
                )
            ]
        )

    @property
    def shared_functions(self) -> Mapping[str, aws_lambda.Function]:
        return self._shared_functions

    @property
    def shared_layers(self) -> Mapping[str, aws_lambda.Function]:
        return self._shared_layers

    @property
    def cluster_state_change_event(self) -> aws_lambda.Function:
        return self._cluster_state_change_event
