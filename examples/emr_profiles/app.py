#!/usr/bin/env python3

import os

from aws_cdk import (
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_kms as kms,
    core
)

from aws_emr_launch.constructs.emr_constructs import emr_profile

app = core.App()
stack = core.Stack(app, 'EmrProfilesStack', env=core.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"],
    region=os.environ["CDK_DEFAULT_REGION"]))

# Load some preexisting resources from my environment
vpc = ec2.Vpc.from_lookup(stack, 'Vpc', vpc_id='vpc-01c3cc44009934845')
artifacts_bucket = s3.Bucket.from_bucket_name(stack, 'ArtifactsBucket', 'chamcca-emr-launch-artifacts-uw2')
logs_bucket = s3.Bucket.from_bucket_name(stack, 'LogsBucket', 'chamcca-emr-launch-logs-uw2')
data_bucket = s3.Bucket.from_bucket_name(stack, 'DataBucket', 'chamcca-emr-launch-data-uw2')

# A simple EMR Profile that grants proper access to the Logs and Artifacts buckets
# By default S3 Server Side encryption is enabled
sse_s3_profile = emr_profile.EMRProfile(
    stack, 'SSES3Profile',
    profile_name='sse-s3-profile',
    vpc=vpc,
    artifacts_bucket=artifacts_bucket,
    logs_bucket=logs_bucket)

sse_s3_profile \
    .authorize_input_buckets([data_bucket]) \
    .authorize_output_buckets([data_bucket])

# Here we create a KMS Key to use for At Rest Encryption in S3 and Locally
kms_key = kms.Key(stack, 'AtRestKMSKey')

# And a new profile to use the KMS Key
sse_kms_profile = emr_profile.EMRProfile(
    stack, 'SSEKMSProfile',
    profile_name='sse-kms-profile',
    vpc=vpc,
    artifacts_bucket=artifacts_bucket,
    logs_bucket=logs_bucket
)

# Authorize the profile for the Data Bucket and set the At Rest Encryption type
sse_kms_profile \
    .authorize_input_buckets([data_bucket]) \
    .authorize_output_buckets([data_bucket]) \
    .set_s3_encryption(emr_profile.S3EncryptionMode.SSE_KMS, encryption_key=kms_key) \
    .set_local_disk_encryption_key(kms_key, ebs_encryption=True) \


app.synth()
