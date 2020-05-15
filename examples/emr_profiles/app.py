#!/usr/bin/env python3

import os

from aws_cdk import (
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_kms as kms,
    aws_secretsmanager as secretsmanager,
    core
)

from aws_emr_launch.constructs.emr_constructs import emr_profile

app = core.App()
stack = core.Stack(app, 'EmrProfilesStack', env=core.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"],
    region=os.environ["CDK_DEFAULT_REGION"]))

# Load some preexisting resources from my environment
vpc = ec2.Vpc.from_lookup(stack, 'Vpc', vpc_id=os.environ['EMR_LAUNCH_EXAMPLES_VPC'])
artifacts_bucket = s3.Bucket.from_bucket_name(
    stack, 'ArtifactsBucket', os.environ['EMR_LAUNCH_EXAMPLES_ARTIFACTS_BUCKET'])
logs_bucket = s3.Bucket.from_bucket_name(
    stack, 'LogsBucket', os.environ['EMR_LAUNCH_EXAMPLES_LOGS_BUCKET'])
data_bucket = s3.Bucket.from_bucket_name(
    stack, 'DataBucket', os.environ['EMR_LAUNCH_EXAMPLES_DATA_BUCKET'])

kerberos_attributes_secret = secretsmanager.Secret.from_secret_arn(
    stack, 'KerberosAttributesSecret', os.environ['EMR_LAUNCH_EXAMPLES_KERBEROS_ATTRIBUTES_SECRET'])


# A simple EMR Profile that grants proper access to the Logs and Artifacts buckets
# By default S3 Server Side encryption is enabled
sse_s3_profile = emr_profile.EMRProfile(
    stack, 'SSES3Profile',
    profile_name='sse-s3-profile',
    vpc=vpc,
    logs_bucket=logs_bucket,
    artifacts_bucket=artifacts_bucket
)

sse_s3_profile \
    .authorize_input_bucket(data_bucket) \
    .authorize_output_bucket(data_bucket)


# Here we create a KMS Key to use for At Rest Encryption in S3 and Locally
kms_key = kms.Key(stack, 'AtRestKMSKey')

# And a new profile to use the KMS Key
sse_kms_profile = emr_profile.EMRProfile(
    stack, 'SSEKMSProfile',
    profile_name='sse-kms-profile',
    vpc=vpc,
    logs_bucket=logs_bucket,
    artifacts_bucket=artifacts_bucket
)

# Authorize the profile for the Data Bucket and set the At Rest Encryption type
sse_kms_profile \
    .authorize_input_bucket(data_bucket) \
    .authorize_output_bucket(data_bucket) \
    .set_s3_encryption(emr_profile.S3EncryptionMode.SSE_KMS, encryption_key=kms_key) \
    .set_local_disk_encryption(kms_key, ebs_encryption=True) \
    .set_local_kdc(kerberos_attributes_secret)

app.synth()
