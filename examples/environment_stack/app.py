#!/usr/bin/env python3

import json
import os

from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import core

NAMING_PREFIX = f"emr-launch-{core.Aws.ACCOUNT_ID}-{core.Aws.REGION}"

app = core.App()
stack = core.Stack(
    app,
    "EmrLaunchExamplesEnvStack",
    env=core.Environment(account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"]),
)

vpc = ec2.Vpc(stack, "EmrLaunchVpc")

logs_bucket = s3.Bucket(
    stack,
    "EmrLaunchLogsBucket",
    bucket_name=f"{NAMING_PREFIX}-logs",
    block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
)
artifacts_bucket = s3.Bucket(
    stack,
    "EmrLaunchArtifactsBucket",
    bucket_name=f"{NAMING_PREFIX}-artifacts",
    block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
)
data_bucket = s3.Bucket(
    stack,
    "EmrLaunchDataBucket",
    bucket_name=f"{NAMING_PREFIX}-data",
    block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
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
