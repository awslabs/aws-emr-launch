from . import _lambda_path

from aws_cdk import (
    aws_lambda,
    aws_iam as iam,
    core
)


class EMRLambdas(core.Construct):

    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        self._run_job_flow = aws_lambda.Function(
            self,
            'AddJobFlow',
            code=aws_lambda.Code.asset(_lambda_path('emr_functions')),
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
            code=aws_lambda.Code.asset(_lambda_path('emr_functions')),
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

    @property
    def run_job_flow(self):
        return self._run_job_flow

    @property
    def add_job_flow_steps(self):
        return self._add_job_flow_steps