from typing import Any, Dict

from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import core

from aws_emr_launch.constructs.emr_constructs import emr_code, emr_profile
from aws_emr_launch.constructs.step_functions import emr_tasks


def print_and_assert(default_task_json: Dict[str, Any], task: sfn.TaskStateBase) -> None:
    stack = core.Stack.of(task)
    resolved_task = stack.resolve(task.to_state_json())
    print(default_task_json)
    print(resolved_task)
    assert default_task_json == resolved_task


def test_start_execution_task() -> None:
    default_task_json = {
        "Resource": {"Fn::Join": ["", ["arn:", {"Ref": "AWS::Partition"}, ":states:::states:startExecution.sync"]]},
        "Parameters": {"StateMachineArn": {"Ref": "teststatemachine7F4C511D"}, "Input.$": "$$.Execution.Input"},
        "End": True,
        "Type": "Task",
    }

    stack = core.Stack(core.App(), "test-stack")

    state_machine = sfn.StateMachine(
        stack, "test-state-machine", definition=sfn.Chain.start(sfn.Succeed(stack, "Succeeded"))
    )

    task = emr_tasks.StartExecutionTask(
        stack,
        "test-task",
        state_machine=state_machine,
    )

    print_and_assert(default_task_json, task)


def test_start_execution_task_with_input() -> None:
    default_task_json = {
        "Resource": {"Fn::Join": ["", ["arn:", {"Ref": "AWS::Partition"}, ":states:::states:startExecution.sync"]]},
        "Parameters": {
            "StateMachineArn": {"Ref": "teststatemachine7F4C511D"},
            "Input": {"Key1": "Value1"},
            "Name": "test-sfn-task",
        },
        "End": True,
        "Type": "Task",
    }

    stack = core.Stack(core.App(), "test-stack")

    state_machine = sfn.StateMachine(
        stack, "test-state-machine", definition=sfn.Chain.start(sfn.Succeed(stack, "Succeeded"))
    )

    task = emr_tasks.StartExecutionTask(
        stack, "test-task", state_machine=state_machine, input={"Key1": "Value1"}, name="test-sfn-task"
    )

    print_and_assert(default_task_json, task)


def test_emr_create_cluster_task() -> None:
    default_task_json = {
        "Resource": {
            "Fn::Join": ["", ["arn:", {"Ref": "AWS::Partition"}, ":states:::elasticmapreduce:createCluster.sync"]]
        },
        "Parameters": {
            "AdditionalInfo.$": "$.AdditionalInfo",
            "AmiVersion.$": "$.AmiVersion",
            "Applications.$": "$.Applications",
            "AutoScalingRole.$": "$.AutoScalingRole",
            "BootstrapActions.$": "$.BootstrapActions",
            "Configurations.$": "$.Configurations",
            "CustomAmiId.$": "$.CustomAmiId",
            "EbsRootVolumeSize.$": "$.EbsRootVolumeSize",
            "Instances": {
                "AdditionalMasterSecurityGroups.$": "$.Instances.AdditionalMasterSecurityGroups",
                "AdditionalSlaveSecurityGroups.$": "$.Instances.AdditionalSlaveSecurityGroups",
                "Ec2KeyName.$": "$.Instances.Ec2KeyName",
                "Ec2SubnetId.$": "$.Instances.Ec2SubnetId",
                "Ec2SubnetIds.$": "$.Instances.Ec2SubnetIds",
                "EmrManagedMasterSecurityGroup.$": "$.Instances.EmrManagedMasterSecurityGroup",
                "EmrManagedSlaveSecurityGroup.$": "$.Instances.EmrManagedSlaveSecurityGroup",
                "HadoopVersion.$": "$.Instances.HadoopVersion",
                "InstanceCount.$": "$.Instances.InstanceCount",
                "InstanceFleets.$": "$.Instances.InstanceFleets",
                "InstanceGroups.$": "$.Instances.InstanceGroups",
                "KeepJobFlowAliveWhenNoSteps": True,
                "MasterInstanceType.$": "$.Instances.MasterInstanceType",
                "Placement.$": "$.Instances.Placement",
                "ServiceAccessSecurityGroup.$": "$.Instances.ServiceAccessSecurityGroup",
                "SlaveInstanceType.$": "$.Instances.SlaveInstanceType",
                "TerminationProtected.$": "$.Instances.TerminationProtected",
            },
            "JobFlowRole.$": "$.JobFlowRole",
            "KerberosAttributes.$": "$.KerberosAttributes",
            "LogUri.$": "$.LogUri",
            "ManagedScalingPolicy.$": "$.ManagedScalingPolicy",
            "Name.$": "$.Name",
            "NewSupportedProducts.$": "$.NewSupportedProducts",
            "ReleaseLabel.$": "$.ReleaseLabel",
            "RepoUpgradeOnBoot.$": "$.RepoUpgradeOnBoot",
            "ScaleDownBehavior.$": "$.ScaleDownBehavior",
            "SecurityConfiguration.$": "$.SecurityConfiguration",
            "ServiceRole.$": "$.ServiceRole",
            "StepConcurrencyLevel.$": "$.StepConcurrencyLevel",
            "SupportedProducts.$": "$.SupportedProducts",
            "Tags.$": "$.Tags",
            "VisibleToAllUsers.$": "$.VisibleToAllUsers",
        },
        "End": True,
        "Type": "Task",
        "InputPath": "$.ClusterConfiguration.Cluster",
    }

    stack = core.Stack(core.App(), "test-stack")

    task = emr_tasks.EmrCreateClusterTask(
        stack,
        "test-task",
        roles=emr_profile.EMRRoles(stack, "test-emr-roles", role_name_prefix="test-roles"),
        input_path="$.ClusterConfiguration.Cluster",
    )

    print_and_assert(default_task_json, task)


def test_emr_add_step_task() -> None:
    default_task_json = {
        "Resource": {"Fn::Join": ["", ["arn:", {"Ref": "AWS::Partition"}, ":states:::elasticmapreduce:addStep.sync"]]},
        "Parameters": {"ClusterId": "test-cluster-id", "Step": {"Key1": {"Key2": "Value2"}}},
        "End": True,
        "Type": "Task",
    }

    stack = core.Stack(core.App(), "test-stack")

    task = emr_tasks.EmrAddStepTask(stack, "test-task", cluster_id="test-cluster-id", step={"Key1": {"Key2": "Value2"}})

    print_and_assert(default_task_json, task)


def test_load_cluster_configuration_builder() -> None:
    default_task_json = {
        "End": True,
        "Retry": [
            {
                "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
                "IntervalSeconds": 2,
                "MaxAttempts": 6,
                "BackoffRate": 2,
            }
        ],
        "Type": "Task",
        "Resource": {"Fn::GetAtt": ["testtaskLoadClusterConfiguration518ECBAD", "Arn"]},
        "Parameters": {
            "ClusterName": "test-cluster",
            "ClusterTags": [{"Key": "Key1", "Value": "Value1"}],
            "ProfileNamespace": "test",
            "ProfileName": "test-profile",
            "ConfigurationNamespace": "test",
            "ConfigurationName": "test-configuration",
        },
    }

    stack = core.Stack(core.App(), "test-stack")

    task = emr_tasks.LoadClusterConfigurationBuilder.build(
        stack,
        "test-task",
        cluster_name="test-cluster",
        cluster_tags=[core.Tag("Key1", "Value1")],
        profile_namespace="test",
        profile_name="test-profile",
        configuration_namespace="test",
        configuration_name="test-configuration",
    )

    print_and_assert(default_task_json, task)


def test_override_cluster_configs_builder() -> None:
    default_task_json = {
        "End": True,
        "Retry": [
            {
                "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
                "IntervalSeconds": 2,
                "MaxAttempts": 6,
                "BackoffRate": 2,
            }
        ],
        "Type": "Task",
        "Resource": {"Fn::GetAtt": ["OverrideClusterConfigsAEEA22C0", "Arn"]},
        "Parameters": {"ExecutionInput.$": "$$.Execution.Input", "Input.$": "$"},
    }

    stack = core.Stack(core.App(), "test-stack")

    task = emr_tasks.OverrideClusterConfigsBuilder.build(
        stack,
        "test-task",
    )

    print_and_assert(default_task_json, task)


def test_fail_if_cluster_running_builder() -> None:
    default_task_json = {
        "End": True,
        "Retry": [
            {
                "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
                "IntervalSeconds": 2,
                "MaxAttempts": 6,
                "BackoffRate": 2,
            }
        ],
        "Type": "Task",
        "Resource": {"Fn::GetAtt": ["FailIfClusterRunningC0A7FE52", "Arn"]},
        "Parameters": {"ExecutionInput.$": "$$.Execution.Input", "DefaultFailIfClusterRunning": True, "Input.$": "$"},
    }

    stack = core.Stack(core.App(), "test-stack")

    task = emr_tasks.FailIfClusterRunningBuilder.build(stack, "test-task", default_fail_if_cluster_running=True)

    print_and_assert(default_task_json, task)


def test_update_cluster_tags_builder() -> None:
    default_task_json = {
        "End": True,
        "Retry": [
            {
                "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
                "IntervalSeconds": 2,
                "MaxAttempts": 6,
                "BackoffRate": 2,
            }
        ],
        "Type": "Task",
        "Resource": {"Fn::GetAtt": ["UpdateClusterTags9DD0067C", "Arn"]},
        "Parameters": {"ExecutionInput.$": "$$.Execution.Input", "Input.$": "$"},
    }

    stack = core.Stack(core.App(), "test-stack")

    task = emr_tasks.UpdateClusterTagsBuilder.build(
        stack,
        "test-task",
    )

    print_and_assert(default_task_json, task)


def test_create_cluster_builder() -> None:
    default_task_json = {
        "Resource": {
            "Fn::Join": ["", ["arn:", {"Ref": "AWS::Partition"}, ":states:::elasticmapreduce:createCluster.sync"]]
        },
        "Parameters": {
            "AdditionalInfo.$": "$.AdditionalInfo",
            "AmiVersion.$": "$.AmiVersion",
            "Applications.$": "$.Applications",
            "AutoScalingRole.$": "$.AutoScalingRole",
            "BootstrapActions.$": "$.BootstrapActions",
            "Configurations.$": "$.Configurations",
            "CustomAmiId.$": "$.CustomAmiId",
            "EbsRootVolumeSize.$": "$.EbsRootVolumeSize",
            "Instances": {
                "AdditionalMasterSecurityGroups.$": "$.Instances.AdditionalMasterSecurityGroups",
                "AdditionalSlaveSecurityGroups.$": "$.Instances.AdditionalSlaveSecurityGroups",
                "Ec2KeyName.$": "$.Instances.Ec2KeyName",
                "Ec2SubnetId.$": "$.Instances.Ec2SubnetId",
                "Ec2SubnetIds.$": "$.Instances.Ec2SubnetIds",
                "EmrManagedMasterSecurityGroup.$": "$.Instances.EmrManagedMasterSecurityGroup",
                "EmrManagedSlaveSecurityGroup.$": "$.Instances.EmrManagedSlaveSecurityGroup",
                "HadoopVersion.$": "$.Instances.HadoopVersion",
                "InstanceCount.$": "$.Instances.InstanceCount",
                "InstanceFleets.$": "$.Instances.InstanceFleets",
                "InstanceGroups.$": "$.Instances.InstanceGroups",
                "KeepJobFlowAliveWhenNoSteps": True,
                "MasterInstanceType.$": "$.Instances.MasterInstanceType",
                "Placement.$": "$.Instances.Placement",
                "ServiceAccessSecurityGroup.$": "$.Instances.ServiceAccessSecurityGroup",
                "SlaveInstanceType.$": "$.Instances.SlaveInstanceType",
                "TerminationProtected.$": "$.Instances.TerminationProtected",
            },
            "JobFlowRole.$": "$.JobFlowRole",
            "KerberosAttributes.$": "$.KerberosAttributes",
            "LogUri.$": "$.LogUri",
            "ManagedScalingPolicy.$": "$.ManagedScalingPolicy",
            "Name.$": "$.Name",
            "NewSupportedProducts.$": "$.NewSupportedProducts",
            "ReleaseLabel.$": "$.ReleaseLabel",
            "RepoUpgradeOnBoot.$": "$.RepoUpgradeOnBoot",
            "ScaleDownBehavior.$": "$.ScaleDownBehavior",
            "SecurityConfiguration.$": "$.SecurityConfiguration",
            "ServiceRole.$": "$.ServiceRole",
            "StepConcurrencyLevel.$": "$.StepConcurrencyLevel",
            "SupportedProducts.$": "$.SupportedProducts",
            "Tags.$": "$.Tags",
            "VisibleToAllUsers.$": "$.VisibleToAllUsers",
        },
        "End": True,
        "Type": "Task",
        "InputPath": "$",
    }

    stack = core.Stack(core.App(), "test-stack")

    task = emr_tasks.CreateClusterBuilder.build(
        stack, "test-task", roles=emr_profile.EMRRoles(stack, "test-emr-roles", role_name_prefix="test-roles")
    )

    print_and_assert(default_task_json, task)


def test_run_job_flow_builder() -> None:
    default_task_json = {
        "End": True,
        "Retry": [
            {
                "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
                "IntervalSeconds": 2,
                "MaxAttempts": 6,
                "BackoffRate": 2,
            }
        ],
        "Type": "Task",
        "Resource": {"Fn::Join": ["", ["arn:", {"Ref": "AWS::Partition"}, ":states:::lambda:invoke.waitForTaskToken"]]},
        "Parameters": {
            "FunctionName": {"Fn::GetAtt": ["RunJobFlow9B18A53F", "Arn"]},
            "Payload": {
                "ExecutionInput.$": "$$.Execution.Input",
                "Input.$": "$",
                "TaskToken.$": "$$.Task.Token",
                "CheckStatusLambda": {"Fn::GetAtt": ["CheckClusterStatusA7C1019E", "Arn"]},
                "RuleName": {"Ref": "testtaskEventRule9A04A93E"},
                "FireAndForget": False,
            },
        },
    }

    stack = core.Stack(core.App(), "test-stack")

    task = emr_tasks.RunJobFlowBuilder.build(
        stack,
        "test-task",
        roles=emr_profile.EMRRoles(stack, "test-emr-roles", role_name_prefix="test-roles"),
        kerberos_attributes_secret=secretsmanager.Secret(stack, "test-kerberos-secret"),
        secret_configurations={"Secret": secretsmanager.Secret(stack, "test-secret-configurations-secret")},
    )

    print_and_assert(default_task_json, task)


def test_add_step_builder() -> None:
    default_task_json = {
        "Resource": {"Fn::Join": ["", ["arn:", {"Ref": "AWS::Partition"}, ":states:::elasticmapreduce:addStep.sync"]]},
        "Parameters": {
            "ClusterId": "test-cluster-id",
            "Step": {
                "Name": "test-step",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {"Jar": "Jar", "MainClass": "Main", "Args": ["Arg1", "Arg2"], "Properties": []},
            },
        },
        "End": True,
        "Type": "Task",
    }

    stack = core.Stack(core.App(), "test-stack")

    task = emr_tasks.AddStepBuilder.build(
        stack,
        "test-task",
        cluster_id="test-cluster-id",
        emr_step=emr_code.EMRStep("test-step", "Jar", "Main", ["Arg1", "Arg2"]),
    )

    print_and_assert(default_task_json, task)


def test_terminate_cluster_builder() -> None:
    default_task_json = {
        "End": True,
        "Parameters": {"ClusterId": "test-cluster-id"},
        "Type": "Task",
        "Resource": {
            "Fn::Join": ["", ["arn:", {"Ref": "AWS::Partition"}, ":states:::elasticmapreduce:terminateCluster.sync"]]
        },
    }

    stack = core.Stack(core.App(), "test-stack")

    task = emr_tasks.TerminateClusterBuilder.build(
        stack,
        "test-task",
        name="test-terminate-task",
        cluster_id="test-cluster-id",
    )

    print_and_assert(default_task_json, task)
