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

from . import _lambda_path

from aws_cdk import (
    aws_lambda,
    aws_iam as iam,
    core
)


class EMRControlLambdas(core.Construct):

    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        code = aws_lambda.Code.asset(_lambda_path('emr_step_function_lambdas'))

        self._run_job_flow = aws_lambda.Function(
            self,
            'AddJobFlow',
            code=code,
            handler='run_job_flow.handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(5),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'elasticmapreduce:RunJobFlow',
                        'elasticmapreduce:ListClusters',
                        'elasticmapreduce:ListSteps'
                    ],
                    resources=['*']
                )
            ]
        )

        self._add_job_flow_steps = aws_lambda.Function(
            self,
            'AddJobFlowSteps',
            code=code,
            handler='add_job_flow_steps.handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(5),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'elasticmapreduce:AddJobFlowSteps'
                    ],
                    resources=['*']
                )
            ]
        )

        self._check_step_status = aws_lambda.Function(
            self,
            'CheckStepStatus',
            code=code,
            handler='check_step_status.handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(5),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'elasticmapreduce:DescribeCluster',
                        'elasticmapreduce:DescribeStep',
                        'cloudwatch:PutMetricData'
                    ],
                    resources=['*']
                )
            ]
        )

    @property
    def run_job_flow(self) -> aws_lambda.Function:
        return self._run_job_flow

    @property
    def add_job_flow_steps(self) -> aws_lambda.Function:
        return self._add_job_flow_steps

    @property
    def check_step_status(self) -> aws_lambda.Function:
        return self._check_step_status
