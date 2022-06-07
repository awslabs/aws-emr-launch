#!/usr/bin/env python3

import json
import os

import aws_cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager

NAMING_PREFIX = f"emr-launch-{aws_cdk.Aws.ACCOUNT_ID}-{aws_cdk.Aws.REGION}"

app = aws_cdk.App()
stack = aws_cdk.Stack(
    app,
    "EmrLaunchExamplesEnvStack",
    env=aws_cdk.Environment(account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"]),
)

vpc = ec2.Vpc(stack, "EmrLaunchVpc", cidr="10.0.0.0/24", max_azs=2)

logs_bucket = s3.Bucket(
    stack,
    "EmrLaunchLogsBucket",
    bucket_name=f"{NAMING_PREFIX}-logs",
    block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
    removal_policy=aws_cdk.RemovalPolicy.DESTROY,
)
artifacts_bucket = s3.Bucket(
    stack,
    "EmrLaunchArtifactsBucket",
    bucket_name=f"{NAMING_PREFIX}-artifacts",
    block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
    removal_policy=aws_cdk.RemovalPolicy.DESTROY,
)
data_bucket = s3.Bucket(
    stack,
    "EmrLaunchDataBucket",
    bucket_name=f"{NAMING_PREFIX}-data",
    block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
    removal_policy=aws_cdk.RemovalPolicy.DESTROY,
)

external_metastore_secret = secretsmanager.Secret(
    stack,
    "EmrLaunchExternalMetastoreSecret",
    secret_name=f"{NAMING_PREFIX}-external-metastore",
    generate_secret_string=secretsmanager.SecretStringGenerator(
        secret_string_template=json.dumps(
            {
                "javax.jdo.option.ConnectionURL": "jdbc",
                "javax.jdo.option.ConnectionDriverName": "mariaDB",
                "javax.jdo.option.ConnectionUserName": "user",
            }
        ),
        generate_string_key="javax.jdo.option.ConnectionPassword",
    ),
)
kerberos_attributes_secret = secretsmanager.Secret(
    stack,
    "EmrLaunchKerberosAttributesSecret",
    secret_name=f"{NAMING_PREFIX}-kerberos-attributes",
    generate_secret_string=secretsmanager.SecretStringGenerator(
        secret_string_template=json.dumps(
            {
                "Realm": "EC2.INTERNAL",
            }
        ),
        generate_string_key="KdcAdminPassword",
    ),
)

app.synth()
