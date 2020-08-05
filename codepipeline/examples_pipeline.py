#!/usr/bin/env python3

import os

from aws_cdk import aws_codebuild as codebuild
from aws_cdk import aws_codepipeline as codepipeline
from aws_cdk import aws_codepipeline_actions as codepipeline_actions
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import core


app = core.App()

pipeline_params = app.node.try_get_context('examples-pipeline')

stack = core.Stack(
    app, 'EMRLaunchExamplesDeploymentPipeline', env=core.Environment(
        account=os.environ["CDK_DEFAULT_ACCOUNT"],
        region=os.environ["CDK_DEFAULT_REGION"]))

artifacts_bucket = s3.Bucket(stack, 'ArtifactsBucket')

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
    stack, 'CodePipeline',
    restart_execution_on_update=True,
    artifact_bucket=artifacts_bucket, stages=[
        codepipeline.StageProps(stage_name='Source', actions=[
            codepipeline_actions.GitHubSourceAction(
                action_name='GitHub_Source',
                repo='aws-emr-launch',
                branch=pipeline_params['github-branch'],
                owner=pipeline_params['github-owner'],
                oauth_token=core.SecretValue.secrets_manager(
                    secret_id=pipeline_params['deployment-secret'],
                    json_field=pipeline_params['github-oauth-token-key']),
                trigger=codepipeline_actions.GitHubTrigger.WEBHOOK,
                output=source_output,
            )]),
        codepipeline.StageProps(stage_name='Self-Update', actions=[
            codepipeline_actions.CodeBuildAction(
                action_name='Self_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'CodePipelineBuild',
                    build_spec=codebuild.BuildSpec.from_source_filename('codepipeline/examples-buildspec.yaml'),
                    role=code_build_role,
                    environment=codebuild.BuildEnvironment(
                        build_image=codebuild.LinuxBuildImage.STANDARD_4_0,
                        environment_variables={
                            'PROJECT_DIR': codebuild.BuildEnvironmentVariable(value='codepipline'),
                            'STACK_FILE': codebuild.BuildEnvironmentVariable(value='examples_pipeline.py')
                        }
                    )
                ),
                input=source_output
            )
        ]),
        codepipeline.StageProps(stage_name='Examples-Environment', actions=[
            codepipeline_actions.CodeBuildAction(
                action_name='Environment_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'EnvironmentBuild',
                    build_spec=codebuild.BuildSpec.from_source_filename('codepipeline/examples-buildspec.yaml'),
                    role=code_build_role,
                    environment=codebuild.BuildEnvironment(
                        build_image=codebuild.LinuxBuildImage.STANDARD_4_0,
                        environment_variables={
                            'PROJECT_DIR': codebuild.BuildEnvironmentVariable(value='examples/environment_stack'),
                            'STACK_FILE': codebuild.BuildEnvironmentVariable(value='app.py')
                        }
                    )
                ),
                input=source_output
            )
        ]),
        codepipeline.StageProps(stage_name='Control-Plane', actions=[
           codepipeline_actions.CodeBuildAction(
               action_name='ControlPlane_Deploy',
               project=codebuild.PipelineProject(
                   stack, 'ControlPlaneBuild',
                   build_spec=codebuild.BuildSpec.from_source_filename('codepipeline/examples-buildspec.yaml'),
                   role=code_build_role,
                   environment=codebuild.BuildEnvironment(
                       build_image=codebuild.LinuxBuildImage.STANDARD_4_0,
                       environment_variables={
                           'PROJECT_DIR': codebuild.BuildEnvironmentVariable(value='examples/control_plane'),
                           'STACK_FILE': codebuild.BuildEnvironmentVariable(value='app.py')
                       }
                   )
               ),
               input=source_output
           )
        ]),
        codepipeline.StageProps(stage_name='Profiles-and-Configurations', actions=[
            codepipeline_actions.CodeBuildAction(
                action_name='EMRProfiles_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'EMRProfilesBuild',
                    build_spec=codebuild.BuildSpec.from_source_filename('codepipeline/examples-buildspec.yaml'),
                    role=code_build_role,
                    environment=codebuild.BuildEnvironment(
                        build_image=codebuild.LinuxBuildImage.STANDARD_4_0,
                        environment_variables={
                            'PROJECT_DIR': codebuild.BuildEnvironmentVariable(value='examples/emr_profiles'),
                            'STACK_FILE': codebuild.BuildEnvironmentVariable(value='app.py')
                        }
                    )
                ),
                input=source_output,
            ),
            codepipeline_actions.CodeBuildAction(
                action_name='ClusterConfigurations_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'ClusterConfigurationsBuild',
                    build_spec=codebuild.BuildSpec.from_source_filename('codepipeline/examples-buildspec.yaml'),
                    role=code_build_role,
                    environment=codebuild.BuildEnvironment(
                        build_image=codebuild.LinuxBuildImage.STANDARD_4_0,
                        environment_variables={
                            'PROJECT_DIR': codebuild.BuildEnvironmentVariable(value='examples/cluster_configurations'),
                            'STACK_FILE': codebuild.BuildEnvironmentVariable(value='app.py')
                        }
                    )
                ),
                input=source_output,
            ),
        ]),
        codepipeline.StageProps(stage_name='EMR-Launch-Function', actions=[
            codepipeline_actions.CodeBuildAction(
                action_name='EMRLaunchFunction_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'EMRLaunchFunctionBuild',
                    build_spec=codebuild.BuildSpec.from_source_filename('codepipeline/examples-buildspec.yaml'),
                    role=code_build_role,
                    environment=codebuild.BuildEnvironment(
                        build_image=codebuild.LinuxBuildImage.STANDARD_4_0,
                        environment_variables={
                            'PROJECT_DIR': codebuild.BuildEnvironmentVariable(value='examples/emr_launch_functions'),
                            'STACK_FILE': codebuild.BuildEnvironmentVariable(value='app.py')
                        }
                    )
                ),
                input=source_output
            )
        ]),
        codepipeline.StageProps(stage_name='Pipelines', actions=[
            codepipeline_actions.CodeBuildAction(
                action_name='TransientClusterPipeline_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'TransientClusterPipelineBuild',
                    build_spec=codebuild.BuildSpec.from_source_filename('codepipeline/examples-buildspec.yaml'),
                    role=code_build_role,
                    environment=codebuild.BuildEnvironment(
                        build_image=codebuild.LinuxBuildImage.STANDARD_4_0,
                        environment_variables={
                            'PROJECT_DIR': codebuild.BuildEnvironmentVariable(
                                value='examples/transient_cluster_pipeline'),
                            'STACK_FILE': codebuild.BuildEnvironmentVariable(value='app.py')
                        }
                    )
                ),
                input=source_output,
            ),
            codepipeline_actions.CodeBuildAction(
                action_name='PersistentClusterPipeline_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'PersistentClusterPipelineBuild',
                    build_spec=codebuild.BuildSpec.from_source_filename('codepipeline/examples-buildspec.yaml'),
                    role=code_build_role,
                    environment=codebuild.BuildEnvironment(
                        build_image=codebuild.LinuxBuildImage.STANDARD_4_0,
                        environment_variables={
                            'PROJECT_DIR': codebuild.BuildEnvironmentVariable(
                                value='examples/persistent_cluster_pipeline'),
                            'STACK_FILE': codebuild.BuildEnvironmentVariable(value='app.py')
                        }
                    )
                ),
                input=source_output,
            ),
            codepipeline_actions.CodeBuildAction(
                action_name='SNSTriggeredPipeline_Deploy',
                project=codebuild.PipelineProject(
                    stack, 'SNSTriggeredPipelineBuild',
                    build_spec=codebuild.BuildSpec.from_source_filename('codepipeline/examples-buildspec.yaml'),
                    role=code_build_role,
                    environment=codebuild.BuildEnvironment(
                        build_image=codebuild.LinuxBuildImage.STANDARD_4_0,
                        environment_variables={
                            'PROJECT_DIR': codebuild.BuildEnvironmentVariable(value='examples/sns_triggered_pipeline'),
                            'STACK_FILE': codebuild.BuildEnvironmentVariable(value='app.py')
                        }
                    )
                ),
                input=source_output
            )
        ]),
    ])

app.synth()
