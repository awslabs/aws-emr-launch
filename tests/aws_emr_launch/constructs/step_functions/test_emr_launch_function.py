import json
import unittest
from typing import Any, Dict, cast

import aws_cdk
import boto3
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_sns as sns
from moto import mock_ssm

from aws_emr_launch import __product__, __version__
from aws_emr_launch.constructs.emr_constructs import cluster_configuration, emr_profile
from aws_emr_launch.constructs.step_functions import emr_launch_function


class TestControlPlaneApis(unittest.TestCase):

    default_function = {
        "AllowedClusterConfigOverrides": {
            "ClusterName": {"Default": "test-configuration", "JsonPath": "Name"},
            "ReleaseLabel": {"Default": "emr-5.29.0", "JsonPath": "ReleaseLabel"},
            "StepConcurrencyLevel": {"Default": 1, "JsonPath": "StepConcurrencyLevel"},
        },
        "ClusterConfiguration": "default/test-configuration",
        "ClusterName": "test-cluster",
        "ClusterTags": [
            {"Key": "deployment:product:name", "Value": __product__},
            {"Key": "deployment:product:version", "Value": __version__},
        ],
        "DefaultFailIfClusterRunning": False,
        "Description": "test description",
        "EMRProfile": "default/test-profile",
        "FailureTopic": {"Ref": "FailureTopic74C6EA16"},
        "LaunchFunctionName": "test-function",
        "Namespace": "default",
        "StateMachine": {"Ref": "testfunctionStateMachineF50AE8F9"},
        "SuccessTopic": {"Ref": "SuccessTopic495EEDDD"},
        "WaitForClusterStart": False,
    }

    def print_and_assert(self, function_json: Dict[str, Any], function: emr_launch_function.EMRLaunchFunction) -> None:
        stack = aws_cdk.Stack.of(function)

        resolved_function = stack.resolve(function.to_json())
        print(self.default_function)
        print(resolved_function)
        assert function.launch_function_name
        assert function.namespace
        assert function.emr_profile
        assert function.cluster_configuration
        assert function.cluster_name
        assert not function.default_fail_if_cluster_running
        assert function.success_topic
        assert function.failure_topic
        assert function.override_cluster_configs_lambda is None
        assert function.allowed_cluster_config_overrides
        assert function.state_machine
        assert function.description

        assert function_json == resolved_function

    def test_emr_launch_function(self) -> None:
        stack = aws_cdk.Stack(aws_cdk.App(), "test-stack")
        vpc = ec2.Vpc(stack, "Vpc")
        success_topic = sns.Topic(stack, "SuccessTopic")
        failure_topic = sns.Topic(stack, "FailureTopic")

        profile = emr_profile.EMRProfile(stack, "test-profile", profile_name="test-profile", vpc=vpc)
        configuration = cluster_configuration.ClusterConfiguration(
            stack, "test-configuration", configuration_name="test-configuration"
        )

        function = emr_launch_function.EMRLaunchFunction(
            stack,
            "test-function",
            launch_function_name="test-function",
            emr_profile=profile,
            cluster_configuration=configuration,
            cluster_name="test-cluster",
            description="test description",
            success_topic=success_topic,
            failure_topic=failure_topic,
            allowed_cluster_config_overrides=configuration.override_interfaces["default"],
            wait_for_cluster_start=False,
        )

        self.print_and_assert(self.default_function, function)

    def test_emr_secure_launch_function(self) -> None:
        stack = aws_cdk.Stack(aws_cdk.App(), "test-stack")
        vpc = ec2.Vpc(stack, "Vpc")
        success_topic = sns.Topic(stack, "SuccessTopic")
        failure_topic = sns.Topic(stack, "FailureTopic")

        profile = emr_profile.EMRProfile(
            stack,
            "test-profile",
            profile_name="test-profile",
            vpc=vpc,
        )
        configuration = cluster_configuration.ClusterConfiguration(
            stack,
            "test-configuration",
            configuration_name="test-configuration",
            secret_configurations={"SecretConfiguration": secretsmanager.Secret(stack, "Secret")},
        )

        function = emr_launch_function.EMRLaunchFunction(
            stack,
            "test-function",
            description="test description",
            launch_function_name="test-function",
            emr_profile=profile,
            cluster_configuration=configuration,
            cluster_name="test-cluster",
            success_topic=success_topic,
            failure_topic=failure_topic,
            allowed_cluster_config_overrides=configuration.override_interfaces["default"],
            wait_for_cluster_start=False,
        )

        self.print_and_assert(self.default_function, function)

    @mock_ssm
    def test_get_function(self) -> None:
        stack = aws_cdk.Stack(
            aws_cdk.App(), "test-stack", env=aws_cdk.Environment(account="123456789012", region="us-east-1")
        )
        vpc = ec2.Vpc.from_lookup(stack, "test-vpc", vpc_id="vpc-12345678")
        success_topic = sns.Topic(stack, "SuccessTopic")
        failure_topic = sns.Topic(stack, "FailureTopic")

        profile = emr_profile.EMRProfile(stack, "test-profile", profile_name="test-profile", vpc=cast(ec2.Vpc, vpc))
        configuration = cluster_configuration.ClusterConfiguration(
            stack, "test-configuration", configuration_name="test-configuration"
        )

        function = emr_launch_function.EMRLaunchFunction(
            stack,
            "test-function",
            launch_function_name="test-function",
            emr_profile=profile,
            cluster_configuration=configuration,
            cluster_name="test-cluster",
            description="test description",
            success_topic=success_topic,
            failure_topic=failure_topic,
            allowed_cluster_config_overrides=configuration.override_interfaces["default"],
            wait_for_cluster_start=False,
        )

        ssm = boto3.client("ssm")

        ssm.put_parameter(
            Name=f"{emr_profile.SSM_PARAMETER_PREFIX}/{profile.namespace}/{profile.profile_name}",
            Value=json.dumps(profile.to_json()),
        )
        ssm.put_parameter(
            Name=f"{cluster_configuration.SSM_PARAMETER_PREFIX}/"
            f"{configuration.namespace}/{configuration.configuration_name}",
            Value=json.dumps(configuration.to_json()),
        )
        ssm.put_parameter(
            Name=f"{emr_launch_function.SSM_PARAMETER_PREFIX}/{function.namespace}/{function.launch_function_name}",
            Value=json.dumps(function.to_json()),
        )

        restored_function = emr_launch_function.EMRLaunchFunction.from_stored_function(
            stack,
            "test-restored-function",
            namespace=function.namespace,
            launch_function_name=function.launch_function_name,
        )

        self.assertEquals(function.to_json(), restored_function.to_json())
