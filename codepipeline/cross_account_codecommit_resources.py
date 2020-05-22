#!/usr/bin/env python3

from aws_cdk import aws_codecommit as codecommit
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_s3 as s3
from aws_cdk import core

app = core.App()

stage = app.node.try_get_context('cross-account-codecommit-resources')
DEPLOYMENT_ACCOUNT = stage['deployment-account']
DEPLOYMENT_REGION = stage['deployment-region']
CODECOMMIT_REPOSITORY = stage['codecommit-repository']
PIPELINE_ARTIFACTS_BUCKET = stage['pipeline-artifacts-bucket']
PIPELINE_ARTIFACTS_KEY = stage['pipeline-artifacts-key']
TRUSTED_ACCOUNTS = stage['trusted-accounts']

stack = core.Stack(app, 'CrossAccountCodeCommitResourcesStack', env=core.Environment(
    account=DEPLOYMENT_ACCOUNT,
    region=DEPLOYMENT_REGION
))

repository = codecommit.Repository.from_repository_name(
    stack, 'CodeCommitRepository', CODECOMMIT_REPOSITORY)
pipeline_artifacts_bucket = s3.Bucket.from_bucket_name(
    stack, 'PipelineArtifactsBucket', PIPELINE_ARTIFACTS_BUCKET)
pipeline_artifacts_key = kms.Key.from_key_arn(
    stack, 'PipelineArtifactsKey', PIPELINE_ARTIFACTS_KEY)

cross_account_codecommit_role = iam.Role(
    stack, 'CrossAccountCodeCommitRole',
    assumed_by=iam.CompositePrincipal(*[iam.AccountPrincipal(account) for account in TRUSTED_ACCOUNTS]),
    inline_policies={
        'codepipeline-policy': iam.PolicyDocument(statements=[
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    'codecommit:BatchGet*',
                    'codecommit:BatchDescribe*',
                    'codecommit:Describe*',
                    'codecommit:EvaluatePullRequestApprovalRules',
                    'codecommit:Get*',
                    'codecommit:List*',
                    'codecommit:GitPull',
                    'codecommit:UploadArchive'
                ],
                resources=[repository.repository_arn]
            ),
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
                    pipeline_artifacts_bucket.bucket_arn,
                    f'{pipeline_artifacts_bucket.bucket_arn}/*'
                ]
            ),
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    'kms:Decrypt',
                    'kms:DescribeKey',
                    'kms:Encrypt',
                    'kms:ReEncrypt*',
                    'kms:GenerateDataKey*'
                ],
                resources=[pipeline_artifacts_key.key_arn]
            )
        ])
    }
)

core.CfnOutput(
    stack, 'CrossAcountCodeCommitRoleOutput',
    value=cross_account_codecommit_role.role_arn)

app.synth()
