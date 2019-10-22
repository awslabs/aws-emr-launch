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

from typing import Optional
from . import _lambda_path

from aws_cdk import (
    aws_lambda,
    aws_iam as iam,
    aws_sns as sns,
    aws_stepfunctions as sfn,
    core
)


class EMRUtilities(core.Construct):

    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        code = aws_lambda.Code.asset(_lambda_path('emr_utilities'))

        self._fail_if_job_running = aws_lambda.Function(
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

        self._run_job_flow = aws_lambda.Function(
            self,
            'AddJobFlow',
            code=code,
            handler='run_job_flow.handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'elasticmapreduce:RunJobFlow'
                    ],
                    resources=['*']
                )
            ]
        )

        self._cluster_state_change_event = aws_lambda.Function(
            self,
            'ClusterStateChangeEvent',
            code=code,
            handler='cluster_state_change_event.handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'elasticmapreduce:DescribeCluster',
                        'elasticmapreduce:RemoveTags',
                        'states:SendTaskSuccess',
                        'states:SendTaskHeartbeat',
                        'states:SendTaskFailure'
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

    @property
    def fail_if_job_running(self) -> aws_lambda.Function:
        return self._fail_if_job_running

    @property
    def run_job_flow(self) -> aws_lambda.Function:
        return self._run_job_flow

    @property
    def cluster_state_change_event(self) -> aws_lambda.Function:
        return self._cluster_state_change_event

    @property
    def add_job_flow_steps(self) -> aws_lambda.Function:
        return self._add_job_flow_steps

