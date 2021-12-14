import os

from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as event_targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda, core


class JobSummaryStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        id: str,
        orchestration_sfn_name: str,
        launch_sfn_name: str,
        log_bucket_arn: str,
        destination_bucket_name: str,
        success_sns_topic_arn: str,
        failure_sns_topic_arn: str,
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)

        aws_account = os.environ["CDK_DEFAULT_ACCOUNT"]
        aws_region = os.environ["CDK_DEFAULT_REGION"]

        lambda_code = aws_lambda.Code.from_asset("infrastructure/job_summary/lambda_source/")

        job_summary_lambda = aws_lambda.Function(
            self,
            "EmrLaunchJobSummaryLambda",
            code=lambda_code,
            handler="main.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            environment={
                "DESTINATION_BUCKET_NAME": destination_bucket_name,
                "SUCCESS_SNS_TOPIC_ARN": success_sns_topic_arn,
                "FAILURE_SNS_TOPIC_ARN": failure_sns_topic_arn,
            },
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:DescribeExecution",
                        "states:GetExecutionHistory",
                    ],
                    resources=[
                        f"arn:aws:states:{aws_region}:{aws_account}:execution:{orchestration_sfn_name}:*",
                        f"arn:aws:states:{aws_region}:{aws_account}:execution:{launch_sfn_name}:*",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "elasticmapreduce:DescribeCluster",
                        "elasticmapreduce:ListSteps",
                    ],
                    resources=[
                        f"arn:aws:elasticmapreduce:{aws_region}:{aws_account}:cluster/*",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:ListBucket",
                        "s3:GetObject",
                        "s3:PutObject",
                    ],
                    resources=[
                        log_bucket_arn,
                        f"{log_bucket_arn}/*",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "SNS:Publish",
                    ],
                    resources=[
                        success_sns_topic_arn,
                        failure_sns_topic_arn,
                    ],
                ),
            ],
        )

        job_summary_event_rule = events.Rule(
            self,
            "EmrLaunchJobSummaryEventRule",
            description="Triggers the creation of SFN execution summary",
            event_pattern=events.EventPattern(
                source=["aws.states"],
                detail_type=["Step Functions Execution Status Change"],
                detail={
                    "status": ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"],
                    "stateMachineArn": [
                        f"arn:aws:states:{aws_region}:{aws_account}:stateMachine:{orchestration_sfn_name}",
                    ],
                },
            ),
        )

        job_summary_event_rule.add_target(
            event_targets.LambdaFunction(
                job_summary_lambda,
                event=events.RuleTargetInput.from_object(
                    {"sfnExecutionArn": events.EventField.from_path("$.detail.executionArn")}
                ),
            )
        )
