#!/usr/bin/env python3

from aws_cdk import (
    core,
    aws_codecommit as codecommit,
    aws_iam as iam,
    aws_kms as kms,
    aws_s3 as s3
)


DEPLOYMENT_ACCOUNT = '052886665315'
DEPLOYMENT_REGION = 'us-west-2'
CODE_COMMIT_REPOSITORY = 'AWSProServe_project_EMRLaunch'
PIPELINE_ARTIFACTS_BUCKET = 'codepipelinesharedresourc-artifactsbucket2aac5544-7c88w1xbywt5'
PIPELINE_ARTIFACTS_KEY = 'arn:aws:kms:us-west-2:876929970656:key/e5fff83f-1b47-4cb8-9307-27fdeea12a83'
TRUSTED_ACCOUNTS = ['876929970656']

app = core.App()
stack = core.Stack(app, 'CrossAccountCodeCommitResourcesStack', env=core.Environment(
    account=DEPLOYMENT_ACCOUNT,
    region=DEPLOYMENT_REGION
))

repository = codecommit.Repository.from_repository_name(
    stack, 'CodeCommitRepository', CODE_COMMIT_REPOSITORY)
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
