import os

from aws_cdk import (
    core,
    aws_lambda,
    aws_lambda_event_sources as sources,
    aws_iam as iam,
    aws_s3 as s3,
    aws_sns as sns,
    aws_dynamodb as dynamo
)


class EmrTriggerStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str,
                 target_step_function_arn: str,
                 source_bucket_sns: sns.Topic,
                 dynamo_table: dynamo.Table,
                 **kwargs):
        super().__init__(scope, id, **kwargs)

        # SNS Triggered Pipeline
        lambda_code = aws_lambda.Code.from_asset('infrastructure/emr_trigger/lambda_source/')

        sns_lambda = aws_lambda.Function(
            self, 'SNSTriggeredLambda',
            code=lambda_code,
            handler='trigger.handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            environment={
                'PIPELINE_ARN': target_step_function_arn,
                'TABLE_NAME': dynamo_table.table_name
            },
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'states:StartExecution',
                        'states:ListExecutions'
                    ],
                    resources=[target_step_function_arn]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "dynamodb:BatchGet*",
                        "dynamodb:DescribeStream",
                        "dynamodb:DescribeTable",
                        "dynamodb:Get*",
                        "dynamodb:Query",
                        "dynamodb:Scan",
                        "dynamodb:BatchWrite*",
                        "dynamodb:CreateTable",
                        "dynamodb:Delete*",
                        "dynamodb:Update*",
                        "dynamodb:PutItem"
                    ],
                    resources=[dynamo_table.table_arn]
                )
            ],
            events=[
                sources.SnsEventSource(source_bucket_sns)
            ]
        )
