from aws_cdk import aws_events as events
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda, core

from aws_emr_launch.constructs.base import BaseBuilder
from aws_emr_launch.constructs.iam_roles import emr_roles
from aws_emr_launch.constructs.lambdas import _lambda_path


class FailIfClusterRunningBuilder(BaseBuilder):
    @staticmethod
    def get_or_build(scope: core.Construct) -> aws_lambda.Function:
        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities/fail_if_cluster_running'))
        stack = core.Stack.of(scope)

        layer = EMRConfigUtilsLayerBuilder.get_or_build(scope)

        lambda_function = stack.node.try_find_child('FailIfClusterRunning')
        if lambda_function is None:
            lambda_function = aws_lambda.Function(
                stack,
                'FailIfClusterRunning',
                code=code,
                handler='lambda_source.handler',
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                timeout=core.Duration.minutes(1),
                layers=[layer],
                initial_policy=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'elasticmapreduce:ListClusters'
                        ],
                        resources=['*']
                    )
                ]
            )
            BaseBuilder.tag_construct(lambda_function)
        return lambda_function


class LoadClusterConfigurationBuilder(BaseBuilder):
    @staticmethod
    def build(scope: core.Construct, profile_namespace: str, profile_name: str,
              configuration_namespace: str, configuration_name: str) -> aws_lambda.Function:
        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities/load_cluster_configuration'))
        stack = core.Stack.of(scope)

        layer = EMRConfigUtilsLayerBuilder.get_or_build(scope)

        lambda_function = aws_lambda.Function(
            scope,
            'LoadClusterConfiguration',
            code=code,
            handler='lambda_source.handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            layers=[layer],
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['ssm:GetParameter'],
                    resources=[
                        stack.format_arn(
                            partition=stack.partition,
                            service='ssm',
                            resource='parameter/emr_launch/cluster_configurations/'
                            f'{configuration_namespace}/{configuration_name}'
                        ),
                        stack.format_arn(
                            partition=stack.partition,
                            service='ssm',
                            resource='parameter/emr_launch/emr_profiles/'
                            f'{profile_namespace}/{profile_name}'
                        )
                    ]
                )
            ]
        )
        BaseBuilder.tag_construct(lambda_function)
        return lambda_function


class OverrideClusterConfigsBuilder(BaseBuilder):
    @staticmethod
    def get_or_build(scope: core.Construct) -> aws_lambda.Function:
        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities/override_cluster_configs'))
        stack = core.Stack.of(scope)

        layer = EMRConfigUtilsLayerBuilder.get_or_build(scope)

        lambda_function = stack.node.try_find_child('OverrideClusterConfigs')
        if lambda_function is None:
            lambda_function = aws_lambda.Function(
                stack,
                'OverrideClusterConfigs',
                code=code,
                handler='lambda_source.handler',
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                timeout=core.Duration.minutes(1),
                layers=[layer]
            )
            BaseBuilder.tag_construct(lambda_function)
        return lambda_function


class UpdateClusterTagsBuilder(BaseBuilder):
    @staticmethod
    def get_or_build(scope: core.Construct) -> aws_lambda.Function:
        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities/update_cluster_tags'))
        stack = core.Stack.of(scope)

        layer = EMRConfigUtilsLayerBuilder.get_or_build(scope)

        lambda_function = stack.node.try_find_child('UpdateClusterTags')
        if lambda_function is None:
            lambda_function = aws_lambda.Function(
                stack,
                'UpdateClusterTags',
                code=code,
                handler='lambda_source.handler',
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                timeout=core.Duration.minutes(1),
                layers=[layer]
            )
            BaseBuilder.tag_construct(lambda_function)
        return lambda_function


class ParseJsonStringBuilder(BaseBuilder):
    @staticmethod
    def get_or_build(scope: core.Construct) -> aws_lambda.Function:
        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities/parse_json_string'))
        stack = core.Stack.of(scope)

        layer = EMRConfigUtilsLayerBuilder.get_or_build(scope)

        lambda_function = stack.node.try_find_child('ParseJsonString')
        if lambda_function is None:
            lambda_function = aws_lambda.Function(
                stack,
                'ParseJsonString',
                code=code,
                handler='lambda_source.handler',
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                timeout=core.Duration.minutes(1),
                layers=[layer]
            )
            BaseBuilder.tag_construct(lambda_function)
        return lambda_function


class OverrideStepArgsBuilder(BaseBuilder):
    @staticmethod
    def get_or_build(scope: core.Construct) -> aws_lambda.Function:
        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities/override_step_args'))
        stack = core.Stack.of(scope)

        layer = EMRConfigUtilsLayerBuilder.get_or_build(scope)

        lambda_function = stack.node.try_find_child('OverrideStepArgs')
        if lambda_function is None:
            lambda_function = aws_lambda.Function(
                stack,
                'OverrideStepArgs',
                code=code,
                handler='lambda_source.handler',
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                timeout=core.Duration.minutes(1),
                layers=[layer]
            )
            BaseBuilder.tag_construct(lambda_function)
        return lambda_function


class RunJobFlowBuilder(BaseBuilder):
    @staticmethod
    def get_or_build(scope: core.Construct, roles: emr_roles.EMRRoles, event_rule: events.Rule) -> aws_lambda.Function:
        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities/run_job_flow'))
        stack = core.Stack.of(scope)

        layer = EMRConfigUtilsLayerBuilder.get_or_build(scope)

        lambda_function = stack.node.try_find_child('RunJobFlow')
        if lambda_function is None:
            lambda_function = aws_lambda.Function(
                stack,
                'RunJobFlow',
                code=code,
                handler='lambda_source.handler',
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                timeout=core.Duration.minutes(1),
                layers=[layer],
                initial_policy=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=['elasticmapreduce:RunJobFlow'],
                        resources=['*']
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=['iam:PassRole'],
                        resources=[
                            roles.service_role.role_arn,
                            roles.instance_role.role_arn,
                            roles.autoscaling_role.role_arn
                        ]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=['states:SendTaskSuccess'],
                        resources=['*']
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=['events:EnableRule', 'events:PutTargets'],
                        resources=[event_rule.rule_arn]
                    )
                ]
            )
            BaseBuilder.tag_construct(lambda_function)
        return lambda_function


class CheckClusterStatusBuilder(BaseBuilder):
    @staticmethod
    def get_or_build(scope: core.Construct, event_rule: events.Rule) -> aws_lambda.Function:
        code = aws_lambda.Code.from_asset(_lambda_path('emr_utilities/check_cluster_status'))
        stack = core.Stack.of(scope)

        layer = EMRConfigUtilsLayerBuilder.get_or_build(scope)

        lambda_function = stack.node.try_find_child('CheckClusterStatus')
        if lambda_function is None:
            lambda_function = aws_lambda.Function(
                stack,
                'CheckClusterStatus',
                code=code,
                handler='lambda_source.handler',
                runtime=aws_lambda.Runtime.PYTHON_3_7,
                timeout=core.Duration.minutes(1),
                layers=[layer],
                initial_policy=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'states:SendTaskSuccess',
                            'states:SendTaskHeartbeat',
                            'states:SendTaskFailure'
                        ],
                        resources=['*']
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=['elasticmapreduce:DescribeCluster'],
                        resources=['*']
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'events:ListTargetsByRule',
                            'events:DisableRule',
                            'events:RemoveTargets'],
                        resources=[event_rule.rule_arn]
                    )
                ]
            )
            BaseBuilder.tag_construct(lambda_function)
            lambda_function.add_permission(
                'EventRulePermission',
                principal=iam.ServicePrincipal('events.amazonaws.com'),
                action='lambda:InvokeFunction',
                source_arn=event_rule.rule_arn)

        return lambda_function


class EMRConfigUtilsLayerBuilder(BaseBuilder):
    @staticmethod
    def get_or_build(scope: core.Construct) -> aws_lambda.LayerVersion:
        code = aws_lambda.Code.from_asset(_lambda_path('layers/emr_config_utils'))
        stack = core.Stack.of(scope)

        layer = stack.node.try_find_child('EMRConfigUtilsLayer')
        if layer is None:
            layer = aws_lambda.LayerVersion(
                stack,
                'EMRConfigUtilsLayer',
                layer_version_name='EMRLaunch_EMRUtilities_EMRConfigUtilsLayer',
                code=code,
                compatible_runtimes=[
                    aws_lambda.Runtime.PYTHON_3_7
                ],
                description='EMR configuration utility functions'
            )
            BaseBuilder.tag_construct(layer)
        return layer
