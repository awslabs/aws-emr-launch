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

from aws_cdk import (
    aws_lambda,
    aws_iam as iam,
    core
)

from . import _lambda_path


class FailIfJobRunning(core.Construct):
    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        code = aws_lambda.Code.asset(_lambda_path('emr_utilities'))

        self._lambda_function = aws_lambda.Function(
            self,
            'FailIfJobRunning',
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

    @property
    def lambda_function(self) -> aws_lambda.Function:
        return self._lambda_function


class OverrideClusterConfigs(core.Construct):
    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        code = aws_lambda.Code.asset(_lambda_path('emr_utilities'))

        self._layer = aws_lambda.LayerVersion(
            self,
            'EMRConfigUtilsLayer',
            layer_version_name='EMRLaunch_EMRUtilities_EMRConfigUtilsLayer',
            code=aws_lambda.Code.asset(_lambda_path('layers/emr_config_utils')),
            compatible_runtimes=[
                aws_lambda.Runtime.PYTHON_3_7
            ],
            description='EMR configuration utility functions'
        )

        self._lambda_function = aws_lambda.Function(
            self,
            'OverrideClusterConfigs',
            code=code,
            handler='override_cluster_configs.handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            layers=[self._layer]
        )

    @property
    def layer(self) -> aws_lambda.LayerVersion:
        return self._layer

    @property
    def lambda_function(self) -> aws_lambda.Function:
        return self._lambda_function
