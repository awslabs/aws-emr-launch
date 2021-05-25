#!/usr/bin/env python3

from aws_cdk import core
import os
import json
from infrastructure.emr_launch.cluster_definition import EMRClusterDefinition
from infrastructure.emr_orchestration.stack import StepFunctionStack
from infrastructure.emr_trigger.stack import EmrTriggerStack
from infrastructure.job_summary.stack import JobSummaryStack

from aws_cdk import (
    aws_s3,
    core,
    aws_sns as sns,
    aws_s3_notifications as s3n,
)

# Load config
project_dir = os.path.dirname(os.path.abspath(__file__))

config_file = os.path.join(project_dir, 'config.json')

with open(config_file) as json_file:
    config = json.load(json_file)

print(config)

app = core.App()
stack_id = config['stack-id']
cluster_name = config['emr']['CLUSTER_NAME']


def emr_launch(config, input_buckets: [str]):

    environment_variables = [
        'CLUSTER_NAME',
        'MASTER_INSTANCE_TYPE',
        'CORE_INSTANCE_TYPE',
        'CORE_INSTANCE_COUNT',
        'CORE_INSTANCE_MARKET',
        'TASK_INSTANCE_TYPE',
        'TASK_INSTANCE_COUNT',
        'TASK_INSTANCE_MARKET',
        'RELEASE_LABEL',
        'APPLICATIONS',
        'CONFIGURATION',
    ]

    list_vars = [
        'APPLICATIONS'
    ]

    int_vars = [
        'CORE_INSTANCE_COUNT',
        'TASK_INSTANCE_COUNT',
    ]

    json_vars = [
        'CONFIGURATION'
    ]

    clean_config = {
        'INPUT_BUCKETS': input_buckets
    }

    for v in environment_variables:
        val = config[v]
        clean_config[v] = val

    return EMRClusterDefinition(
        app,
        id=config['CLUSTER_NAME'] + '-EMRLaunch',
        config=clean_config
    )


class S3InputStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        src_bucket = aws_s3.Bucket(
            self,
            id='src-bucket',
            removal_policy=core.RemovalPolicy.DESTROY
        )

        new_files_topic = sns.Topic(self, 'NewFileEventNotification')
        src_bucket.add_event_notification(aws_s3.EventType.OBJECT_CREATED, s3n.SnsDestination(new_files_topic))

        self.input_bucket_sns = new_files_topic
        self.input_bucket_arn = src_bucket.bucket_arn
        print("Input bucket: " + self.input_bucket_arn)
        print("Input bucket SNS: " + self.input_bucket_sns.topic_arn)


# To create an input s3 bucket and sns topic
s3_stack = S3InputStack(
    app,
    id=cluster_name + '-S3InputBucket'
)

emr_cluster_stack = emr_launch(config['emr'], input_buckets=[s3_stack.input_bucket_arn])

emr_orchestration_stack = StepFunctionStack(
    app,
    id=cluster_name + '-EMROrchestration',
    emr_launch_stack=emr_cluster_stack,
    artifact_bucket=emr_cluster_stack.artifact_bucket,
    output_bucket=emr_cluster_stack.output_bucket
)

emr_trigger_stack = EmrTriggerStack(
    app,
    id=cluster_name + '-EMRTrigger',
    target_step_function_arn=emr_orchestration_stack.state_machine.state_machine_arn,
    source_bucket_sns=s3_stack.input_bucket_sns,
    dynamo_table=emr_orchestration_stack.dynamo_table
)

job_summary_stack = JobSummaryStack(
    app,
    id=cluster_name + '-JobSummary',
    orchestration_sfn_name=emr_orchestration_stack.state_machine.state_machine_name,
    launch_sfn_name=emr_cluster_stack.launch_function.state_machine.state_machine_name,
    log_bucket_arn=emr_cluster_stack.emr_profile.logs_bucket.bucket_arn,
    destination_bucket_name=emr_cluster_stack.emr_profile.logs_bucket.bucket_name,
    success_sns_topic_arn=emr_orchestration_stack.success_topic.topic_arn,
    failure_sns_topic_arn=emr_orchestration_stack.failure_topic.topic_arn
)

app.synth()