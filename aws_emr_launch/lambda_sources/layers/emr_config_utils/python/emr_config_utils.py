import os
import json
import boto3

from typing import Mapping


class StepFunctionParameters(object):

    def __init__(self, mapping: Mapping):
        self._step_function_arn = mapping.get('DEFAULT_STEP_FUNCTION_ARN', None)
        self._step_function_wait_time = mapping.get('DEFAULT_STEP_FUNCTION_WAIT_TIME', None)
        self._success_topic_arn = mapping.get('DEFAULT_SUCCESS_TOPIC', None)
        self._failure_topic_arn  = mapping.get('DEFAULT_FAILURE_TOPIC', None)
        self._cluster_config = mapping.get('DEFAULT_CLUSTER_CONFIG', None)
        self._fail_if_job_running = mapping.get('DEFAULT_FAIL_IF_JOB_RUNNING', None)

    @property
    def step_function_arn(self) -> str:
        return self._step_function_arn

    @property
    def step_function_wait_tine(self) -> int:
        return self._step_function_wait_time

    @property
    def success_topic_arn(self) -> str:
        return self._success_topic_arn

    @property
    def failure_topic_arn(self) -> str:
        return self._failure_topic_arn

    @property
    def cluster_config(self) -> dict:
        return self._cluster_config

    @property
    def fail_if_job_running(self) -> bool:
        return self._fail_if_job_running


def load_defaults():
    return StepFunctionParameters(os.environ)


def execute_step_function(parameters: StepFunctionParameters):
    sfn = boto3.client('stepfunctions')
    json_input = json.dumps({
        'WaitTime': parameters.step_function_wait_tine,
        'SuccessTopicArn': parameters.success_topic_arn,
        'FailureTopicArn': parameters.failure_topic_arn,
        'FailIfJobRunning': parameters.fail_if_job_running,
        'ClusterConfig': parameters.cluster_config
    })

    response = sfn.start_execution(
        stateMachineArn=parameters.step_function_arn,
        input=json_input
    )

    return response
