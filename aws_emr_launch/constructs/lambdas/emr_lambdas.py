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


class FailIfClusterRunning(core.Construct):
    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities'))
        stack = core.Stack.of(scope)

        self._lambda_function = stack.node.try_find_child('FailIfClusterRunning')
        if self._lambda_function is None:
            self._lambda_function = aws_lambda.Function(
                stack,
                'FailIfClusterRunning',
                code=code,
                handler='fail_if_cluster_running.handler',
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

        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities'))
        stack = core.Stack.of(scope)

        layer = EMRConfigUtilsLayer(self, 'EmrConfigUtilsLayer')

        self._lambda_function = stack.node.try_find_child('OverrideClusterConfigs')
        if self._lambda_function is None:
            self._lambda_function = aws_lambda.Function(
                stack,
                'OverrideClusterConfigs',
                code=code,
                handler='override_cluster_configs.handler',
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                timeout=core.Duration.minutes(1),
                layers=[layer.layer]
            )

    @property
    def lambda_function(self) -> aws_lambda.Function:
        return self._lambda_function


class UpdateClusterTags(core.Construct):
    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities'))
        stack = core.Stack.of(scope)

        self._lambda_function = stack.node.try_find_child('UpdateClusterTags')
        if self._lambda_function is None:
            self._lambda_function = aws_lambda.Function(
                stack,
                'UpdateClusterTags',
                code=code,
                handler='update_cluster_tags.handler',
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                timeout=core.Duration.minutes(1)
            )

    @property
    def lambda_function(self) -> aws_lambda.Function:
        return self._lambda_function


class ParseJsonString(core.Construct):
    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities'))
        stack = core.Stack.of(scope)

        self._lambda_function = stack.node.try_find_child('ParseJsonString')
        if self._lambda_function is None:
            self._lambda_function = aws_lambda.Function(
                stack,
                'ParseJsonString',
                code=code,
                handler='parse_json_string.handler',
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                timeout=core.Duration.minutes(1)
            )

    @property
    def lambda_function(self) -> aws_lambda.Function:
        return self._lambda_function


class RunJobFlow(core.Construct):
    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities'))
        stack = core.Stack.of(scope)

        layer = EMRConfigUtilsLayer(self, 'EmrConfigUtilsLayer')

        self._lambda_function = stack.node.try_find_child('RunJobFlow')
        if self._lambda_function is None:
            self._lambda_function = aws_lambda.Function(
                stack,
                'RunJobFlow',
                code=code,
                handler='run_job_flow.handler',
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                timeout=core.Duration.minutes(1),
                layers=[layer.layer],
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

    @property
    def lambda_function(self) -> aws_lambda.Function:
        return self._lambda_function


class AddJobFlowSteps(core.Construct):
    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities'))
        stack = core.Stack.of(scope)

        self._lambda_function = stack.node.try_find_child('AddJobFlowSteps')
        if self._lambda_function is None:
            self._lambda_function = aws_lambda.Function(
                stack,
                'AddJobFlowSteps',
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
                            'elasticmapreduce:AddJobFlowSteps',
                            'ssm:PutParameter'
                        ],
                        resources=['*']
                    )
                ]
            )

    @property
    def lambda_function(self) -> aws_lambda.Function:
        return self._lambda_function


class TerminateJobFlow(core.Construct):
    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities'))
        stack = core.Stack.of(scope)

        self._lambda_function = stack.node.try_find_child('TerminateJobFlow')
        if self._lambda_function is None:
            self._lambda_function = aws_lambda.Function(
                stack,
                'TerminateJobFlow',
                code=code,
                handler='terminate_job_flow.handler',
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                timeout=core.Duration.minutes(1),
                initial_policy=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'elasticmapreduce:TerminateJobFlows',
                            'ssm:PutParameter'
                        ],
                        resources=['*']
                    )
                ]
            )

    @property
    def lambda_function(self) -> aws_lambda.Function:
        return self._lambda_function


class EMRConfigUtilsLayer(core.Construct):
    def __init__(self, scope: core.Construct, id: str):
        super().__init__(scope, id)

        code = aws_lambda.Code.from_asset(_lambda_path('layers/emr_config_utils'))
        stack = core.Stack.of(scope)

        self._layer = stack.node.try_find_child('EMRConfigUtilsLayer')
        if self._layer is None:
            self._layer = aws_lambda.LayerVersion(
                stack,
                'EMRConfigUtilsLayer',
                layer_version_name='EMRLaunch_EMRUtilities_EMRConfigUtilsLayer',
                code=code,
                compatible_runtimes=[
                    aws_lambda.Runtime.PYTHON_3_7
                ],
                description='EMR configuration utility functions'
            )

    @property
    def layer(self) -> aws_lambda.LayerVersion:
        return self._layer
