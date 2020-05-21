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
                    'python3 -m venv /root/venv',
                    'source /root/venv/bin/activate',
                    "pip install -r requirements.txt"
                ]
            },
            'build': {
                'commands': [
                    f'cd {project_dir}',
                    'source /root/venv/bin/activate',
                    '$(npm bin)/cdk --verbose --require-approval never deploy'
                ]
            }
        },
        'cache': {
            'paths': ['/root/venv/**/*']
        },
        'environment': {
            'buildImage': codebuild.LinuxBuildImage.UBUNTU_14_04_BASE
        }
    })


app = core.App()

stage = app.node.try_get_context('deployment-pipeline')
DEPLOYMENT_ACCOUNT = stage['deployment-account']
DEPLOYMENT_REGION = stage['deployment-region']
CODECOMMIT_REPOSITORY = stage['codecommit-repository']
PIPELINE_ARTIFACTS_BUCKET = stage['pipeline-artifacts-bucket']
PIPELINE_ARTIFACTS_KEY = stage['pipeline-artifacts-key']
CROSS_ACCOUNT_CODECOMMIT_ROLE = stage['cross-account-codecommit-role']


stack = core.Stack(
    app, 'EMRLaunchExamplesDeploymentPipeline', env=core.Environment(
        account=DEPLOYMENT_ACCOUNT,
        region=DEPLOYMENT_REGION))

repository = codecommit.Repository.from_repository_name(
    stack, 'CodeRepository',
    CODECOMMIT_REPOSITORY)

artifacts_key = kms.Key.from_key_arn(
    stack, 'ArtifactsKey', PIPELINE_ARTIFACTS_KEY)
artifacts_bucket = s3.Bucket.from_bucket_attributes(
    stack, 'ArtifactsBucket',
    bucket_name=PIPELINE_ARTIFACTS_BUCKET, encryption_key=artifacts_key)
cross_account_codecommit_role = iam.Role.from_role_arn(
    stack, 'CrossAccountCodeCommitRole', CROSS_ACCOUNT_CODECOMMIT_ROLE)

source_output = codepipeline.Artifact('SourceOutput')

code_build_role = iam.Role(
    stack, 'EMRLaunchExamplesBuildRole',
    role_name='EMRLaunchExamplesBuildRole',
    assumed_by=iam.ServicePrincipal('codebuild.amazonaws.com'),
    managed_policies=[
        iam.ManagedPolicy.from_aws_managed_policy_name('PowerUserAccess'),
        iam.ManagedPolicy.from_aws_managed_policy_name('IAMFullAccess')
    ],
)

pipeline = codepipeline.Pipeline(
    stack, 'Pipeline',
    artifact_bucket=artifacts_bucket, stages=[
        codepipeline.StageProps(stage_name='Source', actions=[
            codepipeline_actions.CodeCommitSourceAction(
                action_name='CodeCommit_Source',
                repository=repository,
                output=source_output,
                role=cross_account_codecommit_role,
                branch='mainline',
                trigger=codepipeline_actions.CodeCommitTrigger.POLL
            )]),
        codepipeline.StageProps(stage_name='Control-Plane', actions=[
           codepipeline_actions.CodeBuildAction(
               action_name='ControlPlane_Deploy',
               project=codebuild.PipelineProject(
                   stack, 'ControlPlaneBuild',
                   build_spec=create_build_spec('examples/control_plane'),
                   role=code_build_role),
               input=source_output
           )
        ]),
        codepipeline.StageProps(stage_name='Profiles-and-Configurations', actions=[
            codepipeline_actions.CodeBuildAction(
                action_name='EMRProfiles_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'EMRProfilesBuild',
                    build_spec=create_build_spec('examples/emr_profiles'),
                    role=code_build_role),
                input=source_output,
            ),
            codepipeline_actions.CodeBuildAction(
                action_name='ClusterConfigurations_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'ClusterConfigurationsBuild',
                    build_spec=create_build_spec('examples/cluster_configurations'),
                    role=code_build_role),
                input=source_output,
            ),
        ]),
        codepipeline.StageProps(stage_name='EMR-Launch-Function', actions=[
            codepipeline_actions.CodeBuildAction(
                action_name='EMRLaunchFunction_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'EMRLaunchFunctionBuild',
                    build_spec=create_build_spec('examples/emr_launch_function'),
                    role=code_build_role),
                input=source_output
            )
        ]),
        codepipeline.StageProps(stage_name='Pipelines', actions=[
            codepipeline_actions.CodeBuildAction(
                action_name='TransientClusterPipeline_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'TransientClusterPipelineBuild',
                    build_spec=create_build_spec('examples/transient_cluster_pipeline'),
                    role=code_build_role),
                input=source_output,
            ),
            codepipeline_actions.CodeBuildAction(
                action_name='PersistentClusterPipeline_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'PersistentClusterPipelineBuild',
                    build_spec=create_build_spec('examples/persistent_cluster_pipeline'),
                    role=code_build_role),
                input=source_output,
            ),
            codepipeline_actions.CodeBuildAction(
                action_name='SNSTriggeredPipeline_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'SNSTriggeredPipelineBuild',
                    build_spec=create_build_spec('examples/sns_triggered_pipeline'),
                    role=code_build_role),
                input=source_output
            )
        ]),
    ])

cross_account_codecommit_role.grant(pipeline.role, 'sts:AssumeRole')

app.synth()
