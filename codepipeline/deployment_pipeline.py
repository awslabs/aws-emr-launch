#!/usr/bin/env python3

import os

from aws_cdk import (
    core,
    aws_codebuild as codebuild,
    aws_codecommit as codecommit,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
    aws_iam as iam,
    aws_kms as kms,
    aws_s3 as s3
)

CODE_COMMIT_REPOSITORY = 'AWSProServe_project_EMRLaunch'
PIPELINE_ARTIFACTS_BUCKET = 'codepipelinesharedresourc-artifactsbucket2aac5544-7c88w1xbywt5'
PIPELINE_ARTIFACTS_POLICY = 'arn:aws:iam::876929970656:policy/CodePipelineSharedResourcesStack-ArtifactsPolicy3F840E1A-1I6RO5N5HJQGL'
# PIPELINE_ARTIFACTS_KEY = 'arn:aws:kms:us-west-2:876929970656:alias/CodePipelineSharedResourcesKey'
PIPELINE_ARTIFACTS_KEY = 'arn:aws:kms:us-west-2:876929970656:key/e5fff83f-1b47-4cb8-9307-27fdeea12a83'
CROSS_ACCOUNT_CODE_COMMIT_ROLE = 'arn:aws:iam::052886665315:role/CrossAccountCodeCommitRes-CrossAccountCodeCommitRo-1FZD9ODMJW3HY'


def create_build_spec(project_dir: str) -> codebuild.BuildSpec:
    return codebuild.BuildSpec.from_object({
        'version': '0.2',
        'env': {
            'variables': {
                'EMR_LAUNCH_EXAMPLES_VPC': os.environ['EMR_LAUNCH_EXAMPLES_VPC'],
                'EMR_LAUNCH_EXAMPLES_ARTIFACTS_BUCKET': os.environ['EMR_LAUNCH_EXAMPLES_ARTIFACTS_BUCKET'],
                'EMR_LAUNCH_EXAMPLES_LOGS_BUCKET': os.environ['EMR_LAUNCH_EXAMPLES_LOGS_BUCKET'],
                'EMR_LAUNCH_EXAMPLES_DATA_BUCKET': os.environ['EMR_LAUNCH_EXAMPLES_DATA_BUCKET'],
                'EMR_LAUNCH_EXAMPLES_KERBEROS_ATTRIBUTES_SECRET':
                    os.environ['EMR_LAUNCH_EXAMPLES_KERBEROS_ATTRIBUTES_SECRET'],
                'EMR_LAUNCH_EXAMPLES_SECRET_CONFIGS': os.environ['EMR_LAUNCH_EXAMPLES_SECRET_CONFIGS']
            }
        },
        'phases': {
            'install': {
                'runtime-versions': {
                    'python': 3.7,
                    'nodejs': 12
                },
                'commands': [
                    'npm install aws-cdk',
                    'python3 -m pip install --user pipenv',
                    "pipenv install '-e .'"
                ]
            },
            'build': {
                'commands': [
                    f'cd {project_dir}',
                    'pipenv run $(npm bin)/cdk --verbose --require-approval never deploy'
                ]
            }
        },
        'environment': {
            'buildImage': codebuild.LinuxBuildImage.UBUNTU_14_04_BASE
        }
    })


def create_build_role(scope: core.Construct, stack_name: str,
                      artifacts_policy: iam.ManagedPolicy) -> iam.Role:
    return iam.Role(
        scope, f'{stack_name}BuildRole',
        role_name=f'{stack_name}BuildRole',
        assumed_by=iam.ServicePrincipal('codebuild.amazonaws.com'),
        managed_policies=[
            iam.ManagedPolicy.from_aws_managed_policy_name('PowerUserAccess'),
            iam.ManagedPolicy.from_aws_managed_policy_name('IAMFullAccess'),
            # artifacts_policy
        ],
    )


app = core.App()
stack = core.Stack(
    app, 'EMRLaunchExamplesDeploymentPipeline', env=core.Environment(
        account='876929970656',
        region='us-west-2'))

repository = codecommit.Repository.from_repository_name(
    stack, 'CodeRepository',
    CODE_COMMIT_REPOSITORY)

artifacts_key = kms.Key.from_key_arn(
    stack, 'ArtifactsKey', PIPELINE_ARTIFACTS_KEY)
artifacts_bucket = s3.Bucket.from_bucket_attributes(
    stack, 'ArtifactsBucket',
    bucket_name=PIPELINE_ARTIFACTS_BUCKET, encryption_key=artifacts_key)
artifacts_policy = iam.ManagedPolicy.from_managed_policy_arn(
    stack, 'ArtifactsPolicy', PIPELINE_ARTIFACTS_POLICY)
cross_account_codecommit_role = iam.Role.from_role_arn(
    stack, 'CrossAccountCodeCommitRole', CROSS_ACCOUNT_CODE_COMMIT_ROLE)

source_output = codepipeline.Artifact('SourceOutput')

emr_profiles_build = codebuild.PipelineProject(
    stack, 'EMRProfilesBuild',
    build_spec=create_build_spec('examples/emr_profiles'),
    role=create_build_role(stack, 'EmrProfilesStack', artifacts_policy))

cluster_configurations_build = codebuild.PipelineProject(
    stack, 'ClusterConfigurationsBuild',
    build_spec=create_build_spec('examples/cluster_configurations'),
    role=create_build_role(stack, 'ClusterConfigurationsStack', artifacts_policy))

pipeline = codepipeline.Pipeline(
    stack, 'Pipeline',
    artifact_bucket=artifacts_bucket, stages=[
        codepipeline.StageProps(stage_name='Source', actions=[
            codepipeline_actions.CodeCommitSourceAction(
                action_name='CodeCommit_Source',
                repository=repository,
                output=source_output,
                role=cross_account_codecommit_role,
                branch='mainline'
            )]),
        codepipeline.StageProps(stage_name='Profiles-and-Configurations', actions=[
            codepipeline_actions.CodeBuildAction(
                action_name='EMRProfiles_Build',
                project=emr_profiles_build,
                input=source_output,
            ),
            codepipeline_actions.CodeBuildAction(
                action_name='ClusterConfigurations_Build',
                project=cluster_configurations_build,
                input=source_output,
            ),
        ])
    ])

pipeline.role.add_managed_policy(artifacts_policy)
cross_account_codecommit_role.grant(pipeline.role, 'sts:AssumeRole')

app.synth()
