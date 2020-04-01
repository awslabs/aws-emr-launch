#!/usr/bin/env python3

from aws_cdk import (
    core,
    aws_iam as iam,
    aws_kms as kms,
    aws_s3 as s3
)


TRUSTED_ACCOUNTS = ['052886665315']

app = core.App()
stack = core.Stack(app, 'CodePipelineSharedResourcesStack', env=core.Environment(
    account='876929970656',
    region='us-west-2'
))


artifacts_key = kms.Key(stack, 'ArtifactsKey', removal_policy=core.RemovalPolicy.DESTROY)
artifacts_key.add_to_resource_policy(iam.PolicyStatement(
    effect=iam.Effect.ALLOW,
    principals=[iam.AccountPrincipal(account) for account in TRUSTED_ACCOUNTS],
    actions=[
        'kms:Decrypt',
        'kms:DescribeKey',
        'kms:Encrypt',
        'kms:ReEncrypt*',
        'kms:GenerateDataKey*'
    ],
    resources=['*']))
alias = kms.Alias(
    stack, 'ArtifactsKeyAlias',
    target_key=artifacts_key,
    alias_name='CodePipelineSharedResourcesKey')

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
core.CfnOutput(stack, 'ArtifactsKeyAliasOutput', value=alias.key_arn)
core.CfnOutput(stack, 'ArtifactsPolicyOutput', value=artifacts_policy.managed_policy_arn)

app.synth()
