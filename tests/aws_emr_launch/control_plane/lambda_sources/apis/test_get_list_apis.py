import json
import logging
import unittest

import boto3
from moto import mock_ssm

from aws_emr_launch.control_plane.lambda_sources.apis import get_list_apis
from aws_emr_launch.control_plane.lambda_sources.apis.get_list_apis import (
    ClusterConfigurationNotFoundError, EMRLaunchFunctionNotFoundError,
    EMRProfileNotFoundError)

# Turn the LOGGER off for the tests
get_list_apis.LOGGER.setLevel(logging.WARN)


class TestControlPlaneApis(unittest.TestCase):
    @mock_ssm
    def test_get_profiles_handler(self):
        event = {
            'Namespace': 'test'
        }

        self.assertEquals(get_list_apis.get_profiles_handler(event, None), {'EMRProfiles': []})

    @mock_ssm
    def test_get_profile(self):
        profile = {
            'Profile': 'TestProfile',
            'Key': 'Value'
        }

        ssm = boto3.client('ssm')
        ssm.put_parameter(
            Name=f'{get_list_apis.PROFILES_SSM_PARAMETER_PREFIX}/default/test', Value=json.dumps(profile))

        event = {
            'ProfileName': 'test'
        }

        self.assertEquals(get_list_apis.get_profile_handler(event, None), profile)

    @mock_ssm
    def test_profile_not_found(self):
        event = {
            'ProfileName': 'test'
        }

        with self.assertRaises(EMRProfileNotFoundError):
            get_list_apis.get_profile_handler(event, None)

    @mock_ssm
    def test_get_configurations_handler(self):
        event = {
            'Namespace': 'test'
        }

        self.assertEquals(get_list_apis.get_configurations_handler(event, None), {'ClusterConfigurations': []})

    @mock_ssm
    def test_get_configuration(self):
        configuration = {
            'Configuration': 'TestConfiguration',
            'Key': 'Value'
        }

        ssm = boto3.client('ssm')
        ssm.put_parameter(
            Name=f'{get_list_apis.CONFIGURATIONS_SSM_PARAMETER_PREFIX}/default/test', Value=json.dumps(configuration))

        event = {
            'ConfigurationName': 'test'
        }

        self.assertEquals(get_list_apis.get_configuration_handler(event, None), configuration)

    @mock_ssm
    def test_configuration_not_found(self):
        event = {
            'ConfigurationName': 'test'
        }

        with self.assertRaises(ClusterConfigurationNotFoundError):
            get_list_apis.get_configuration_handler(event, None)

    @mock_ssm
    def test_get_functions_handler(self):
        event = {
            'Namespace': 'test'
        }

        self.assertEquals(get_list_apis.get_functions_handler(event, None), {'EMRLaunchFunctions': []})

    @mock_ssm
    def test_get_function(self):
        function = {
            'Function': 'TestFunction',
            'Key': 'Value'
        }

        ssm = boto3.client('ssm')
        ssm.put_parameter(Name=f'{get_list_apis.FUNCTIONS_SSM_PARAMETER_PREFIX}/default/test', Value=json.dumps(function))

        event = {
            'FunctionName': 'test'
        }

        self.assertEquals(get_list_apis.get_function_handler(event, None), function)

    @mock_ssm
    def test_function_not_found(self):
        event = {
            'FunctionName': 'test'
        }

        with self.assertRaises(EMRLaunchFunctionNotFoundError):
            get_list_apis.get_function_handler(event, None)
