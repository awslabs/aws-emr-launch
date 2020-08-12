#!/usr/bin/env python3

import os

from aws_cdk import aws_codebuild as codebuild
from aws_cdk import aws_codepipeline as codepipeline
from aws_cdk import aws_codepipeline_actions as codepipeline_actions
from aws_cdk import aws_codestarnotifications as notifications
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import core

app = core.App()

pipeline_params = app.node.try_get_context('release-pipeline')
deployment_secret = pipeline_params['deployment-secret']

stack = core.Stack(
    app, 'EMRLaunchReleaseDeploymentPipeline', env=core.Environment(
        account=os.environ["CDK_DEFAULT_ACCOUNT"],
        region=os.environ["CDK_DEFAULT_REGION"]))

artifacts_bucket = s3.Bucket(stack, 'ArtifactsBucket')
deployment_bucket = s3.Bucket.from_bucket_name(
    stack, 'DeploymentBucket', core.Token.as_string(core.SecretValue.secrets_manager(
        secret_id=deployment_secret['secret-id'],
        json_field=deployment_secret['json-fields']['deployment-bucket'])))

source_output = codepipeline.Artifact('SourceOutput')
release_output = codepipeline.Artifact('ReleaseOutput')

code_build_role = iam.Role(
    stack, 'EMRLaunchReleaseBuildRole',
    role_name='EMRLaunchReleaseBuildRole',
    assumed_by=iam.ServicePrincipal('codebuild.amazonaws.com'),
    managed_policies=[
        iam.ManagedPolicy.from_aws_managed_policy_name('PowerUserAccess'),
        iam.ManagedPolicy.from_aws_managed_policy_name('IAMFullAccess')
    ],
)

pipeline = codepipeline.Pipeline(
    stack, 'CodePipeline',
    pipeline_name='EMR_Launch_Release',
    restart_execution_on_update=True,
    artifact_bucket=artifacts_bucket, stages=[
        codepipeline.StageProps(stage_name='Source', actions=[
            codepipeline_actions.GitHubSourceAction(
                action_name='GitHub_Source',
                repo='aws-emr-launch',
                branch=pipeline_params['github-branch'],
                owner=pipeline_params['github-owner'],
                oauth_token=core.SecretValue.secrets_manager(
                    secret_id=deployment_secret['secret-id'],
                    json_field=deployment_secret['json-fields']['github-oauth-token']),
                trigger=codepipeline_actions.GitHubTrigger.WEBHOOK,
                output=source_output,
            )]),
        codepipeline.StageProps(stage_name='Self-Update', actions=[
            codepipeline_actions.CodeBuildAction(
                action_name='Self_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'CodePipelineBuild',
                    build_spec=codebuild.BuildSpec.from_source_filename(
                        'codepipeline/examples-pipeline-buildspec.yaml'),
                    role=code_build_role,
                    environment=codebuild.BuildEnvironment(
                        build_image=codebuild.LinuxBuildImage.STANDARD_4_0,
                        environment_variables={
                            'PROJECT_DIR': codebuild.BuildEnvironmentVariable(value='codepipeline'),
                            'STACK_FILE': codebuild.BuildEnvironmentVariable(value='release_pipeline.py')
                        }
                    )
                ),
                input=source_output
            )
        ]),
        codepipeline.StageProps(stage_name='PyPi-Release', actions=[
            codepipeline_actions.CodeBuildAction(
                action_name='PyPi_Release',
                project=codebuild.PipelineProject(
                    stack, 'PyPiReleaseBuild',
                    build_spec=codebuild.BuildSpec.from_source_filename(
                        'codepipeline/release-buildspec.yaml'),
                    role=code_build_role,
                    environment=codebuild.BuildEnvironment(
                        build_image=codebuild.LinuxBuildImage.STANDARD_4_0,
                    )
                ),
                input=source_output,
                outputs=[release_output]
            )
        ]),
        codepipeline.StageProps(stage_name='S3-Deploy', actions=[
            codepipeline_actions.S3DeployAction(
                action_name='S3_Deployment',
                bucket=deployment_bucket,
                input=release_output,
                object_key=core.Token.as_string(core.SecretValue.secrets_manager(
                    secret_id=deployment_secret['secret-id'],
                    json_field=deployment_secret['json-fields']['deployment-path'])),
            )
        ]),
    ])

notification_rule = notifications.CfnNotificationRule(
    stack, 'CodePipelineNotifications',
    detail_type='FULL',
    event_type_ids=[
        'codepipeline-pipeline-pipeline-execution-failed',
        'codepipeline-pipeline-pipeline-execution-canceled',
        'codepipeline-pipeline-pipeline-execution-succeeded'
    ],
    name='aws-emr-launch-codepipeline-notifications',
    resource=pipeline.pipeline_arn,
    targets=[
        notifications.CfnNotificationRule.TargetProperty(
            target_address=core.Token.as_string(core.SecretValue.secrets_manager(
                secret_id=deployment_secret['secret-id'],
                json_field=deployment_secret['json-fields']['slack-chatbot'])),
            target_type='AWSChatbotSlack')
    ],
)

app.synth()
