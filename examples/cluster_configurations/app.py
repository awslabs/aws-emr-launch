#!/usr/bin/env python3

from aws_cdk import (
    aws_ec2 as ec2,
    aws_kms as kms,
    aws_s3 as s3,
    aws_sns as sns,
    aws_iam as iam,
    core
)

from aws_emr_launch.constructs.emr_constructs import EMRProfileComponents, InstanceGroupConfiguration
from aws_emr_launch.constructs.step_functions.launch_emr_config import LaunchEMRConfig

app = core.App()
stack = core.Stack(app, 'test-stack', env=core.Environment(account='876929970656', region='us-west-2'))
vpc = ec2.Vpc(stack, 'ExampleVPC')
artifacts_bucket = s3.Bucket.from_bucket_name(stack, 'ArtifactsBucket', 'chamcca-emr-launch-artifacts-uw2')
logs_bucket = s3.Bucket.from_bucket_name(stack, 'LogsBucket', 'chamcca-emr-launch-logs-uw2')
data_bucket = s3.Bucket.from_bucket_name(stack, 'DataBucket', 'chamcca-emr-launch-data-uw2')

success_topic = sns.Topic(stack, 'SuccessTopic')
failure_topic = sns.Topic(stack, 'FailureTopic')

emr_components = EMRProfileComponents(
    stack, 'test-emr-components',
    profile_name='TestCluster',
    vpc=vpc,
    artifacts_bucket=artifacts_bucket,
    logs_bucket=logs_bucket)

emr_components \
    .authorize_input_buckets([data_bucket]) \
    .authorize_output_buckets([data_bucket])

cluster_config = InstanceGroupConfiguration(
    stack, 'test-instance-group-config',
    cluster_name='test-cluster',
    profile_components=emr_components,
    auto_terminate=False)

launch_config = LaunchEMRConfig(
    stack, 'test-step-functions-stack',
    cluster_config=cluster_config,
    success_topic=None,
    failure_topic=failure_topic)

# launch_role - iam.Role()

app.synth()
