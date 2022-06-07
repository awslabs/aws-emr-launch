from typing import Any, Dict

import aws_cdk
from aws_cdk import assertions
from aws_cdk import aws_sns as sns
from aws_cdk import aws_stepfunctions as sfn

from aws_emr_launch.constructs.emr_constructs import emr_code
from aws_emr_launch.constructs.step_functions import emr_chains


def print_and_assert(default_fragment_json: Dict[str, Any], fragment: sfn.StateMachineFragment) -> None:
    stack = aws_cdk.Stack.of(fragment)
    resolved_fragment = stack.resolve(fragment.to_single_state().to_state_json())
    print(default_fragment_json)
    print(resolved_fragment)
    assert default_fragment_json == resolved_fragment


def test_success_chain() -> None:
    default_fragment_json = {
        "Fn::Join": [
            "",
            [
                (
                    '{"StartAt":"Success Notification","States":{"Success Notification":{"Next":"Succeeded","Type":'
                    '"Task","InputPath":"$","OutputPath":"$","ResultPath":"$.PublishResult","Resource":"arn:'
                ),
                {"Ref": "AWS::Partition"},
                ':states:::sns:publish","Parameters":{"TopicArn":"',
                {"Ref": "testtopicB3D54793"},
                '","Message":"TestMessage","Subject":"TestSubject"}},"Succeeded":{"Type":"Succeed"}}}',
            ],
        ]
    }

    stack = aws_cdk.Stack(aws_cdk.App(), "test-stack")
    sfn.StateMachine(
        stack,
        "test-machine",
        definition=emr_chains.Success(
            stack,
            "test-fragment",
            message=sfn.TaskInput.from_text("TestMessage"),
            subject="TestSubject",
            topic=sns.Topic(stack, "test-topic"),
        ),
    )

    template = assertions.Template.from_stack(stack)
    print(template.find_resources("AWS::StepFunctions::StateMachine"))
    template.has_resource_properties(
        "AWS::StepFunctions::StateMachine", {"DefinitionString": assertions.Match.object_like(default_fragment_json)}
    )


def test_fail_chain() -> None:
    default_fragment_json = {
        "Fn::Join": [
            "",
            [
                (
                    '{"StartAt":"Failure Notification","States":{"Failure Notification":{"Next":"Execution Failed",'
                    '"Type":"Task","InputPath":"$","OutputPath":"$","ResultPath":"$.PublishResult","Resource":"arn:'
                ),
                {"Ref": "AWS::Partition"},
                ':states:::sns:publish","Parameters":{"TopicArn":"',
                {"Ref": "testtopicB3D54793"},
                (
                    '","Message":"TestMessage","Subject":"TestSubject"}},"Execution Failed":{"Type":"Fail","Comment":'
                    '"TestComment","Error":"TestError","Cause":"TestCause"}}}'
                ),
            ],
        ]
    }

    stack = aws_cdk.Stack(aws_cdk.App(), "test-stack")
    sfn.StateMachine(
        stack,
        "test-machine",
        definition=emr_chains.Fail(
            stack,
            "test-fragment",
            message=sfn.TaskInput.from_text("TestMessage"),
            subject="TestSubject",
            topic=sns.Topic(stack, "test-topic"),
            cause="TestCause",
            comment="TestComment",
            error="TestError",
        ),
    )

    template = assertions.Template.from_stack(stack)
    print(template.find_resources("AWS::StepFunctions::StateMachine"))
    template.has_resource_properties(
        "AWS::StepFunctions::StateMachine", {"DefinitionString": assertions.Match.object_like(default_fragment_json)}
    )


def test_nested_state_machine_chain() -> None:
    default_fragment_json = {
        "Fn::Join": [
            "",
            [
                '{"StartAt":"test-nested-state-machine","States":{"test-nested-state-machine":{"Resource":"arn:',
                {"Ref": "AWS::Partition"},
                ':states:::states:startExecution.sync","Parameters":{"StateMachineArn":"',
                {"Ref": "teststatemachine7F4C511D"},
                (
                    '","Input":{"Key1":"Value1"}},"Next":"test-nested-state-machine - Parse JSON Output","Catch":'
                    '[{"ErrorEquals":["States.ALL"],"ResultPath":"$.Error","Next":"test-fail"}],"Type":"Task"},'
                    '"test-nested-state-machine - Parse JSON Output":{"End":true,"Retry":[{"ErrorEquals":['
                    '"Lambda.ServiceException","Lambda.AWSLambdaException","Lambda.SdkClientException"],'
                    '"IntervalSeconds":2,"MaxAttempts":6,"BackoffRate":2}],"Catch":[{"ErrorEquals":["States.ALL"],'
                    '"ResultPath":"$.Error","Next":"test-fail"}],"Type":"Task","ResultPath":"$","Resource":"'
                ),
                {"Fn::GetAtt": ["ParseJsonString859DB4F0", "Arn"]},
                '","Parameters":{"JsonString.$":"$.Output"}},"test-fail":{"Type":"Fail"}}}',
            ],
        ]
    }

    stack = aws_cdk.Stack(aws_cdk.App(), "test-stack")
    state_machine = sfn.StateMachine(
        stack, "test-state-machine", definition=sfn.Chain.start(sfn.Succeed(stack, "Succeeded"))
    )

    sfn.StateMachine(
        stack,
        "test-machine",
        definition=emr_chains.NestedStateMachine(
            stack,
            "test-fragment",
            name="test-nested-state-machine",
            state_machine=state_machine,
            input={"Key1": "Value1"},
            fail_chain=sfn.Fail(stack, "test-fail"),
        ),
    )

    template = assertions.Template.from_stack(stack)
    print(template.find_resources("AWS::StepFunctions::StateMachine"))
    template.has_resource_properties(
        "AWS::StepFunctions::StateMachine", {"DefinitionString": assertions.Match.object_like(default_fragment_json)}
    )


def test_add_step_with_argument_overrides() -> None:
    default_fragment_json = {
        "Fn::Join": [
            "",
            [
                (
                    '{"StartAt":"test-step - Override Args","States":{"test-step - Override Args":{"Next":"test-step",'
                    '"Retry":[{"ErrorEquals":["Lambda.ServiceException","Lambda.AWSLambdaException",'
                    '"Lambda.SdkClientException"],"IntervalSeconds":2,"MaxAttempts":6,"BackoffRate":2}],"Catch":['
                    '{"ErrorEquals":["States.ALL"],"ResultPath":"$.Error","Next":"test-fail"}],"Type":"Task",'
                    '"ResultPath":"$.test-fragmentResultArgs","Resource":"'
                ),
                {"Fn::GetAtt": ["OverrideStepArgsE9376C9F", "Arn"]},
                (
                    '","Parameters":{"ExecutionInput.$":"$$.Execution.Input","StepName":"test-step","Args":["Arg1",'
                    '"Arg2"]}},"test-step":{"Resource":"arn:'
                ),
                {"Ref": "AWS::Partition"},
                (
                    ':states:::elasticmapreduce:addStep.sync","Parameters":{"ClusterId":"test-cluster-id","Step":'
                    '{"Name":"test-step","ActionOnFailure":"CONTINUE","HadoopJarStep":{"Jar":"Jar","MainClass":"Main",'
                    '"Args.$":"$.test-fragmentResultArgs","Properties":[]}}},"End":true,"Catch":[{"ErrorEquals":'
                    '["States.ALL"],"ResultPath":"$.Error","Next":"test-fail"}],"Type":"Task"},"test-fail":'
                    '{"Type":"Fail"}}}'
                ),
            ],
        ]
    }

    stack = aws_cdk.Stack(aws_cdk.App(), "test-stack")
    sfn.StateMachine(
        stack,
        "test-machine",
        definition=emr_chains.AddStepWithArgumentOverrides(
            stack,
            "test-fragment",
            emr_step=emr_code.EMRStep("test-step", "Jar", "Main", ["Arg1", "Arg2"]),
            cluster_id="test-cluster-id",
            fail_chain=sfn.Fail(stack, "test-fail"),
        ),
    )

    template = assertions.Template.from_stack(stack)
    print(template.find_resources("AWS::StepFunctions::StateMachine"))
    template.has_resource_properties(
        "AWS::StepFunctions::StateMachine", {"DefinitionString": assertions.Match.object_like(default_fragment_json)}
    )
