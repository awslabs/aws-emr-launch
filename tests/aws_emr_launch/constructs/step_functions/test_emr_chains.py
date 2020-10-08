from aws_cdk import aws_sns as sns
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import core

from aws_emr_launch.constructs.emr_constructs import emr_code, emr_profile
from aws_emr_launch.constructs.step_functions import emr_chains


def print_and_assert(default_fragment_json: dict, fragment: sfn.StateMachineFragment):
    stack = core.Stack.of(fragment)
    resolved_fragment = stack.resolve(fragment.to_single_state().to_state_json())
    print(default_fragment_json)
    print(resolved_fragment)
    assert default_fragment_json == resolved_fragment


def test_success_chain():
    default_fragment_json = {
        'Type': 'Parallel',
        'End': True,
        'Branches': [{
            'StartAt': 'test-fragment: Success Notification',
            'States': {
                'test-fragment: Success Notification': {
                    'Next': 'test-fragment: Succeeded',
                    'InputPath': '$',
                    'Parameters': {
                        'TopicArn': {
                            'Ref': 'testtopicB3D54793'
                        },
                        'Message': 'TestMessage',
                        'Subject': 'TestSubject'
                    },
                    'OutputPath': '$',
                    'Type': 'Task',
                    'Resource': {'Fn::Join': ['', ['arn:', {'Ref': 'AWS::Partition'}, ':states:::sns:publish']]},
                    'ResultPath': '$.PublishResult'
                },
                'test-fragment: Succeeded': {
                    'Type': 'Succeed'
                }
            }
        }]
    }

    stack = core.Stack(core.App(), 'test-stack')

    fragment = emr_chains.Success(
        stack, 'test-fragment',
        message=sfn.TaskInput.from_text('TestMessage'),
        subject='TestSubject',
        topic=sns.Topic(stack, 'test-topic')
    )

    print_and_assert(default_fragment_json, fragment)


def test_fail_chain():
    default_fragment_json = {
        'Type': 'Parallel',
        'End': True,
        'Branches': [{
            'StartAt': 'test-fragment: Failure Notification',
            'States': {
                'test-fragment: Failure Notification': {
                    'Next': 'test-fragment: Execution Failed',
                    'InputPath': '$',
                    'Parameters': {
                        'TopicArn': {
                            'Ref': 'testtopicB3D54793'
                        },
                        'Message': 'TestMessage',
                        'Subject': 'TestSubject'
                    },
                    'OutputPath': '$',
                    'Type': 'Task',
                    'Resource': {'Fn::Join': ['', ['arn:', {'Ref': 'AWS::Partition'}, ':states:::sns:publish']]},
                    'ResultPath': '$.PublishResult'
                },
                'test-fragment: Execution Failed': {
                    'Type': 'Fail',
                    'Comment': 'TestComment',
                    'Error': 'TestError',
                    'Cause': 'TestCause'
                }
            }
        }]
    }

    stack = core.Stack(core.App(), 'test-stack')

    fragment = emr_chains.Fail(
        stack, 'test-fragment',
        message=sfn.TaskInput.from_text('TestMessage'),
        subject='TestSubject',
        topic=sns.Topic(stack, 'test-topic'),
        cause='TestCause',
        comment='TestComment',
        error='TestError'
    )

    print_and_assert(default_fragment_json, fragment)


def test_nested_state_machine_chain():
    default_fragment_json = {
        'Type': 'Parallel',
        'End': True,
        'Branches': [{
            'StartAt': 'test-fragment: test-nested-state-machine',
            'States': {
                'test-fragment: test-nested-state-machine': {
                    'Resource': {
                        'Fn::Join': ['', ['arn:', {
                            'Ref': 'AWS::Partition'
                        }, ':states:::states:startExecution.sync']]
                    },
                    'Parameters': {
                        'StateMachineArn': {
                            'Ref': 'teststatemachine7F4C511D'
                        },
                        'Input': {
                            'Key1': 'Value1'
                        }
                    },
                    'Next': 'test-fragment: test-nested-state-machine - Parse JSON Output',
                    'Catch': [{
                        'ErrorEquals': ['States.ALL'],
                        'ResultPath': '$.Error',
                        'Next': 'test-fail'
                    }],
                    'Type': 'Task'
                },
                'test-fragment: test-nested-state-machine - Parse JSON Output': {
                    'End': True,
                    'Retry': [{
                        'ErrorEquals': ['Lambda.ServiceException', 'Lambda.AWSLambdaException', 'Lambda.SdkClientException'],
                        'IntervalSeconds': 2,
                        'MaxAttempts': 6,
                        'BackoffRate': 2
                    }],
                    'Catch': [{
                        'ErrorEquals': ['States.ALL'],
                        'ResultPath': '$.Error',
                        'Next': 'test-fail'
                    }],
                    'Type': 'Task',
                    'ResultPath': '$',
                    'Resource': {
                        'Fn::GetAtt': ['ParseJsonString859DB4F0', 'Arn']
                    },
                    'Parameters': {
                        'JsonString.$': '$.Output'
                    }
                },
                'test-fail': {
                    'Type': 'Fail'
                }
            }
        }]
    }

    stack = core.Stack(core.App(), 'test-stack')

    state_machine = sfn.StateMachine(
        stack, 'test-state-machine',
        definition=sfn.Chain.start(sfn.Succeed(stack, 'Succeeded')))

    fragment = emr_chains.NestedStateMachine(
        stack, 'test-fragment',
        name='test-nested-state-machine',
        state_machine=state_machine,
        input={'Key1': 'Value1'},
        fail_chain=sfn.Fail(stack, 'test-fail')
    )

    print_and_assert(default_fragment_json, fragment)


def test_add_step_with_argument_overrides():
    default_fragment_json = {
        'Type': 'Parallel',
        'End': True,
        'Branches': [{
            'StartAt': 'test-fragment: test-step - Override Args',
            'States': {
                'test-fragment: test-step - Override Args': {
                    'Next': 'test-fragment: test-step',
                    'Retry': [{
                        'ErrorEquals': ['Lambda.ServiceException', 'Lambda.AWSLambdaException', 'Lambda.SdkClientException'],
                        'IntervalSeconds': 2,
                        'MaxAttempts': 6,
                        'BackoffRate': 2
                    }],
                    'Catch': [{
                        'ErrorEquals': ['States.ALL'],
                        'ResultPath': '$.Error',
                        'Next': 'test-fail'
                    }],
                    'Type': 'Task',
                    'ResultPath': '$.test-fragmentResultArgs',
                    'Resource': {
                        'Fn::GetAtt': ['OverrideStepArgsE9376C9F', 'Arn']
                    },
                    'Parameters': {
                        'ExecutionInput.$': '$$.Execution.Input',
                        'StepName': 'test-step',
                        'Args': ['Arg1', 'Arg2']
                    }
                },
                'test-fragment: test-step': {
                    'Resource': {
                        'Fn::Join': ['', ['arn:', {
                            'Ref': 'AWS::Partition'
                        }, ':states:::elasticmapreduce:addStep.sync']]
                    },
                    'Parameters': {
                        'ClusterId': 'test-cluster-id',
                        'Step': {
                            'Name': 'test-step',
                            'ActionOnFailure': 'CONTINUE',
                            'HadoopJarStep': {
                                'Jar': 'Jar',
                                'MainClass': 'Main',
                                'Args.$': '$.test-fragmentResultArgs',
                                'Properties': []
                            }
                        }
                    },
                    'End': True,
                    'Catch': [{
                        'ErrorEquals': ['States.ALL'],
                        'ResultPath': '$.Error',
                        'Next': 'test-fail'
                    }],
                    'Type': 'Task'
                },
                'test-fail': {
                    'Type': 'Fail'
                }
            }
        }]
    }

    stack = core.Stack(core.App(), 'test-stack')

    fragment = emr_chains.AddStepWithArgumentOverrides(
        stack, 'test-fragment',
        emr_step=emr_code.EMRStep('test-step', 'Jar', 'Main', ['Arg1', 'Arg2']),
        cluster_id='test-cluster-id',
        fail_chain=sfn.Fail(stack, 'test-fail')
    )

    print_and_assert(default_fragment_json, fragment)
