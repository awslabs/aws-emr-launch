import io
import json
import os

import boto3
from extracting import extract_sfn_execution_info
from helpers import make_s3_console_link
from rendering import render_html_page

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    print(f"INPUT: {json.dumps(event)}")

    execution_arn = event["sfnExecutionArn"]

    destination_bucket_name = event.get("destinationBucketName") or os.environ["DESTINATION_BUCKET_NAME"]
    destination_object_key = (
        event.get("destinationObjectKey") or f'job-summary/{execution_arn.split(":")[-1]}/summary.html'
    )

    success_sns_topic_arn = event.get("successSnsTopicArn") or os.environ.get("SUCCESS_SNS_TOPIC_ARN")
    failure_sns_topic_arn = event.get("failureSnsTopicArn") or os.environ.get("FAILURE_SNS_TOPIC_ARN")

    # Collect the information about execution
    info = extract_sfn_execution_info(execution_arn)
    print(f"SUMMARY: {json.dumps(info)}")

    # Store execution summary to S3
    save_execution_info(info, destination_bucket_name, destination_object_key)
    s3_console_link = make_s3_console_link(destination_bucket_name, destination_object_key)

    # Send a notification if SNS Topic ARN is provided
    sns_topic_arn = success_sns_topic_arn if info["status"] == "SUCCEEDED" else failure_sns_topic_arn
    if sns_topic_arn:
        send_notification(sns_topic_arn, execution_arn, info["status"], s3_console_link)


def save_execution_info(info, bucket_name, object_key):
    html_page = render_html_page(info)

    # with open("summary.html", "w") as f:
    #     print(html_page, file=f)

    html_file = io.BytesIO(html_page.encode("UTF-8"))

    print(f"Storing summary to s3://{bucket_name}/{object_key}")
    s3_client.upload_fileobj(html_file, bucket_name, object_key)

    aws_console_link = make_s3_console_link(bucket_name, object_key)
    print(f"AWS Console link: {aws_console_link}")


def send_notification(sns_topic_arn, execution_arn, status, s3_console_link):
    subject = f"SFN Execution Summary ({status})"
    message = f"Summary for SFN execution {execution_arn} is stored in S3 bucket: {s3_console_link}"

    sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=message,
    )
