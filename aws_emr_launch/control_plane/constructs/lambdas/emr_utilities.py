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


class EMRUtilities(core.Construct):

    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        code = aws_lambda.Code.asset(_lambda_path('emr_utilities'))

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

        self._step_state_change_event = aws_lambda.Function(
            self,
            'StepStateChangeEvent',
            function_name='EMRLaunch_EMRUtilities_StepStateChangeEvent',
            code=code,
            handler='step_state_change_event.handler',
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
    def cluster_state_change_event(self) -> aws_lambda.Function:
        return self._cluster_state_change_event

    @property
    def step_state_change_event(self) -> aws_lambda.Function:
        return self._step_state_change_event
