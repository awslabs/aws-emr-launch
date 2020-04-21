#!/usr/bin/env python3

# Copyright 2019 Amazon.com, Inc. and its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the 'License').
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#   http://aws.amazon.com/asl/
#
# or in the 'license' file accompanying this file. This file is distributed
# on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

from aws_cdk import (
    core,
    aws_iam as iam,
    aws_kms as kms,
    aws_s3 as s3
)


app = core.App()

stage = app.node.try_get_context('codepipeline-shared-resources')
DEPLOYMENT_ACCOUNT = stage['deployment-account']
DEPLOYMENT_REGION = stage['deployment-region']
TRUSTED_ACCOUNTS = stage['trusted-accounts']

stack = core.Stack(app, 'CodePipelineSharedResourcesStack', env=core.Environment(
    account=DEPLOYMENT_ACCOUNT,
    region=DEPLOYMENT_REGION
))


artifacts_key = kms.Key(stack, 'ArtifactsKey', removal_policy=core.RemovalPolicy.DESTROY)
artifacts_key.add_to_resource_policy(iam.PolicyStatement(
    effect=iam.Effect.ALLOW,
    principals=[iam.AccountPrincipal(account) for account in TRUSTED_ACCOUNTS + [DEPLOYMENT_ACCOUNT]],
    actions=[
        'kms:Decrypt',
        'kms:DescribeKey',
        'kms:Encrypt',
        'kms:ReEncrypt*',
        'kms:GenerateDataKey*'
    ],
    resources=['*']))

artifacts_bucket = s3.Bucket(
    stack, 'ArtifactsBucket',
    encryption=s3.BucketEncryption.KMS,
    encryption_key=artifacts_key,
    removal_policy=core.RemovalPolicy.DESTROY)
artifacts_bucket.add_to_resource_policy(iam.PolicyStatement(
    effect=iam.Effect.ALLOW,
    principals=[iam.AccountPrincipal(account) for account in TRUSTED_ACCOUNTS],
    actions=[
        's3:Abort*',
        's3:DeleteObject*',
        's3:GetBucket*',
        's3:GetObject*',
        's3:List*',
        's3:PutObject*'
    ],
    resources=[artifacts_bucket.bucket_arn, artifacts_bucket.arn_for_objects('*')]
))

artifacts_policy = iam.ManagedPolicy(
    stack, 'ArtifactsPolicy', statements=[
        iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                'kms:Decrypt',
                'kms:DescribeKey',
                'kms:Encrypt',
                'kms:ReEncrypt*',
                'kms:GenerateDataKey*'
            ],
            resources=[artifacts_key.key_arn]),
        iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                's3:Abort*',
                's3:DeleteObject*',
                's3:GetBucket*',
                's3:GetObject*',
                's3:List*',
                's3:PutObject*'
            ],
            resources=[
                artifacts_bucket.bucket_arn,
                f'{artifacts_bucket.bucket_arn}/*'
            ]
        )])

core.CfnOutput(stack, 'ArtifactsBucketOutput', value=artifacts_bucket.bucket_name)
core.CfnOutput(stack, 'ArtifactsKeyOutput', value=artifacts_key.key_arn)

app.synth()
