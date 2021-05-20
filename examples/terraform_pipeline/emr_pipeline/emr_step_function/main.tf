
provider "aws" {
  region = var.aws_region
}

resource "aws_sns_topic" "emr_success" {
  name = "${var.cluster-name}-emr-success"
}

resource "aws_sns_topic" "emr_failure" {
  name = "${var.cluster-name}-emr-failure"
}


data "aws_iam_policy_document" "emr_sfn_role_policy" {
  statement {
    effect = "Allow"
    actions = [
      "events:DescribeRule",
      "xray:PutTelemetryRecords",
      "events:PutRule",
      "states:StopExecution",
      "elasticmapreduce:DescribeCluster",
      "xray:GetSamplingTargets",
      "elasticmapreduce:SetTerminationProtection",
      "xray:PutTraceSegments",
      "events:PutTargets",
      "elasticmapreduce:DescribeStep",
      "sns:Publish",
      "states:DescribeExecution",
      "xray:GetSamplingRules",
      "states:StartExecution",
      "elasticmapreduce:AddJobFlowSteps",
      "elasticmapreduce:TerminateJobFlows",
      "elasticmapreduce:CancelSteps",
      "lambda:InvokeFunction"
    ]
    resources = [
      "*"
    ]
  }
}

resource "aws_iam_role" "sfn_service_role" {
  name = "${var.cluster-name}-sfn_service_role"
  tags = {
    env = var.stage
    region = var.aws_region
  }
  assume_role_policy = <<EOF
{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "states.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "attach_snf_emr_policy_to_sfn_service_role" {
  role = aws_iam_role.sfn_service_role.name
  policy_arn = aws_iam_policy.sfn_emr_policy.arn
}

resource "aws_iam_policy" "sfn_emr_policy" {
  name = "sfn_emr_policy-${var.cluster-name}"
  description = "Allows sfn to manage emr clusters"
  policy = data.aws_iam_policy_document.emr_sfn_role_policy.json
}

resource "aws_sfn_state_machine" "sfn-orchestrate" {
  name     = "${var.sfn_orchestrate_name}-sfn"
  role_arn = aws_iam_role.sfn_service_role.arn
  definition = data.template_file.sfn-orchestrate-template.rendered
  depends_on = [null_resource.module_depends_on]
}

data "template_file" "sfn-orchestrate-template" {
  template = file(var.sfn_orchestrate_definition_file)

  vars = {
    sfn-emr-launch-arn = var.emr-launch-arn
    failure-sns-arn = aws_sns_topic.emr_failure.arn
    success-sns-arn = aws_sns_topic.emr_success.arn
    jar_path = "s3://${var.artifacts_bucket}/${var.artifact_path}"
    json-parser-arn = aws_lambda_function.json_lambda.arn
    output_s3_path = var.output_s3_path
  }
}

variable "module_depends_on" {
  default = [""]
}

resource "null_resource" "module_depends_on" {
  triggers = {
    value = "${length(var.module_depends_on)}"
  }
}

resource "aws_iam_role" "iam_for_lambda" {
  name = "${var.cluster-name}_iam_for_lambda"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "lambda-basic-execution" {
  name        = "tf-${var.cluster-name}-lambda-json-parser"
  role   = aws_iam_role.iam_for_lambda.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Effect": "Allow",
      "Resource": "*"
    }
  ]
}
EOF
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir = "${path.module}/lambda"
  output_path = "${path.module}/lambda.zip"
}

resource "aws_lambda_function" "json_lambda" {
  filename      = "${path.module}/lambda.zip"
  function_name = "${var.cluster-name}-json-parser"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "lambda_parse_json.handler"
  depends_on = [data.archive_file.lambda_zip]
  source_code_hash = filebase64sha256("${path.module}/lambda/lambda_parse_json.py")

  runtime = "python3.7"
  timeout = 30
}