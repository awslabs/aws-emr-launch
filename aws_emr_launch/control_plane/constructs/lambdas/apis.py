from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda, core

from aws_emr_launch import __package__
from aws_emr_launch.control_plane.constructs.lambdas import _lambda_path


class Apis(core.Construct):

    def __init__(self, scope: core.Construct, id: str) -> None:
        super().__init__(scope, id)

        stack = core.Stack.of(scope)
        code = aws_lambda.Code.from_asset(_lambda_path('apis'))

        self._get_profile = aws_lambda.Function(
            self,
            'GetProfile',
            function_name='EMRLaunch_APIs_GetProfile',
            description=f'Version: {__package__}',
            code=code,
            handler='get_list_apis.get_profile_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'ssm:GetParameter'
                    ],
                    resources=[
                        stack.format_arn(
                            partition=stack.partition,
                            service='ssm',
                            resource='parameter/emr_launch/emr_profiles/*'
                        )
                    ]
                )
            ]
        )

        self._get_profiles = aws_lambda.Function(
            self,
            'GetProfiles',
            function_name='EMRLaunch_APIs_GetProfiles',
            description=f'Version: {__package__}',
            code=code,
            handler='get_list_apis.get_profiles_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'ssm:GetParametersByPath'
                    ],
                    resources=[
                        stack.format_arn(
                            partition=stack.partition,
                            service='ssm',
                            resource='parameter/emr_launch/emr_profiles/*'
                        )
                    ]
                )
            ]
        )

        self._get_configuration = aws_lambda.Function(
            self,
            'GetConfiguration',
            function_name='EMRLaunch_APIs_GetConfiguration',
            description=f'Version: {__package__}',
            code=code,
            handler='get_list_apis.get_configuration_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'ssm:GetParameter'
                    ],
                    resources=[
                        stack.format_arn(
                            partition=stack.partition,
                            service='ssm',
                            resource='parameter/emr_launch/cluster_configurations/*'
                        )
                    ]
                )
            ]
        )

        self._get_configurations = aws_lambda.Function(
            self,
            'GetConfigurations',
            function_name='EMRLaunch_APIs_GetConfigurations',
            description=f'Version: {__package__}',
            code=code,
            handler='get_list_apis.get_configurations_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'ssm:GetParametersByPath'
                    ],
                    resources=[
                        stack.format_arn(
                            partition=stack.partition,
                            service='ssm',
                            resource='parameter/emr_launch/cluster_configurations/*'
                        )
                    ]
                )
            ]
        )

        self._get_function = aws_lambda.Function(
            self,
            'GetFunction',
            function_name='EMRLaunch_APIs_GetFunction',
            description=f'Version: {__package__}',
            code=code,
            handler='get_list_apis.get_function_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'ssm:GetParameter'
                    ],
                    resources=[
                        stack.format_arn(
                            partition=stack.partition,
                            service='ssm',
                            resource='parameter/emr_launch/emr_launch_functions/*'
                        )
                    ]
                )
            ]
        )

        self._get_functions = aws_lambda.Function(
            self,
            'GetFunctions',
            function_name='EMRLaunch_APIs_GetFunctions',
            description=f'Version: {__package__}',
            code=code,
            handler='get_list_apis.get_functions_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.minutes(1),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'ssm:GetParametersByPath'
                    ],
                    resources=[
                        stack.format_arn(
                            partition=stack.partition,
                            service='ssm',
                            resource='parameter/emr_launch/emr_launch_functions/*'
                        )
                    ]
                )
            ]
        )

    @property
    def get_profile(self) -> aws_lambda.Function:
        return self._get_profile

    @property
    def get_profiles(self) -> aws_lambda.Function:
        return self._get_profiles

    @property
    def get_configuration(self) -> aws_lambda.Function:
        return self._get_configuration

    @property
    def get_configurations(self) -> aws_lambda.Function:
        return self._get_configurations

    @property
    def get_function(self) -> aws_lambda.Function:
        return self._get_function

    @property
    def get_functions(self) -> aws_lambda.Function:
        return self._get_functions
