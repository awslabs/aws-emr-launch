from aws_cdk import core

from aws_emr_launch.control_plane.constructs.lambdas.apis import Apis


def test_emr_lambdas():
    app = core.App()
    stack = core.Stack(app, 'test-lambdas-stack')
    apis = Apis(stack, 'test-apis')

    assert apis.get_profile
    assert apis.get_profiles
    assert apis.get_configuration
    assert apis.get_configurations
    assert apis.get_function
    assert apis.get_functions
