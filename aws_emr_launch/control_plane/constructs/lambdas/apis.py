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
from aws_emr_launch import __package__


class Apis(core.Construct):

    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        code = aws_lambda.Code.from_asset(_lambda_path('apis'))

        self._get_profile = aws_lambda.Function(
            self,
            'GetProfile',
            function_name='EMRLaunch_APIs_GetProfile',
            description=f'Version: {__package__}',
            code=code,
            handler='get_list_apis.get_profile_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'ssm:GetParameter'
                    ],
                    resources=['*']
                )
            ]
        )

        self._get_profiles = aws_lambda.Function(
            self,
            'GetProfiles',
            function_name='EMRLaunch_APIs_GetProfiles',
            description=f'Version: {__package__}',
            code=code,
            handler='get_list_apis.get_profiles_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'ssm:GetParametersByPath'
                    ],
                    resources=['*']
                )
            ]
        )

        self._get_configuration = aws_lambda.Function(
            self,
            'GetConfiguration',
            function_name='EMRLaunch_APIs_GetConfiguration',
            description=f'Version: {__package__}',
            code=code,
            handler='get_list_apis.get_configuration_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'ssm:GetParameter'
                    ],
                    resources=['*']
                )
            ]
        )

        self._get_configurations = aws_lambda.Function(
            self,
            'GetConfigurations',
            function_name='EMRLaunch_APIs_GetConfigurations',
            description=f'Version: {__package__}',
            code=code,
            handler='get_list_apis.get_configurations_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'ssm:GetParametersByPath'
                    ],
                    resources=['*']
                )
            ]
        )

        self._get_function = aws_lambda.Function(
            self,
            'GetFunction',
            function_name='EMRLaunch_APIs_GetFunction',
            description=f'Version: {__package__}',
            code=code,
            handler='get_list_apis.get_function_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'ssm:GetParameter'
                    ],
                    resources=['*']
                )
            ]
        )

        self._get_functions = aws_lambda.Function(
            self,
            'GetFunctions',
            function_name='EMRLaunch_APIs_GetFunctions',
            description=f'Version: {__package__}',
            code=code,
            handler='get_list_apis.get_functions_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'ssm:GetParametersByPath'
                    ],
                    resources=['*']
                )
            ]
        )
