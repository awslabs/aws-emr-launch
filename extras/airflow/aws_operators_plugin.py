# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
from typing import Dict, Optional, Union

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class StepFunctionHook(AwsHook):
    """
    Interact with an AWS Step Functions State Machine.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, region_name=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.region_name = region_name
        self.conn = None

    def get_conn(self):
        if not self.conn:
            self.conn = self.get_client_type("stepfunctions", self.region_name)
        return self.conn

    def start_execution(
        self, state_machine_arn: str, name: Optional[str] = None, input: Union[Dict[str, any], str, None] = None
    ):
        """
        Start Execution of the State Machine.
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html#SFN.Client.start_execution

        :param state_machine_arn: AWS Step Function State Machine ARN
        :type state_machine_arn: str
        :param name: The name of the execution.
        :type name: Optional[str]
        :param input: JSON data input to pass to the State Machine
        :type input: Union[Dict[str, any], str, None]
        :return: Execution ARN
        :rtype: str
        """
        execution_args = {"stateMachineArn": state_machine_arn}
        if name is not None:
            execution_args["name"] = name
        if input is not None:
            if isinstance(input, str):
                execution_args["input"] = str
            elif isinstance(input, dict):
                execution_args["input"] = json.dumps(input)

        self.log.info(f"Executing Step Function State Machine: {state_machine_arn}")

        response = self.get_conn().start_execution(**execution_args)
        return response["executionArn"] if "executionArn" in response else None

    def describe_execution(self, execution_arn: str):
        """
        Describes a State Machine Execution
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html#SFN.Client.describe_execution

        :param execution_arn: ARN of the State Machine Execution
        :type execution_arn: str
        :return: Dict with Execution details
        :rtype: dict
        """
        return self.get_conn().describe_execution(executionArn=execution_arn)


class StepFunctionGetExecutionOutputOperator(BaseOperator):
    """
    An Operator that begins execution of an Step Function State Machine

    Additional arguments may be specified and are passed down to the underlying BaseOperator.

    .. seealso::
        :class:`~airflow.models.BaseOperator`

    :param execution_arn: ARN of the Step Function State Machine Execution
    :type execution_arn: str
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    :type aws_conn_id: str
    """

    template_fields = ["execution_arn"]
    template_ext = ()
    ui_color = "#f9c915"

    @apply_defaults
    def __init__(self, execution_arn: str, aws_conn_id="aws_default", region_name=None, *args, **kwargs):
        if kwargs.get("xcom_push") is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")
        super().__init__(*args, **kwargs)
        self.execution_arn = execution_arn
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    def execute(self, context):
        hook = StepFunctionHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

        execution_status = hook.describe_execution(self.execution_arn)
        execution_output = json.loads(execution_status["output"]) if "output" in execution_status else None

        if self.do_xcom_push:
            context["ti"].xcom_push(key="execution_output", value=execution_output)

        self.log.info(f"Got State Machine Execution output for {self.execution_arn}")

        return execution_output


class StepFunctionStartExecutionOperator(BaseOperator):
    """
    An Operator that begins execution of an Step Function State Machine

    Additional arguments may be specified and are passed down to the underlying BaseOperator.

    .. seealso::
        :class:`~airflow.models.BaseOperator`

    :param state_machine_arn: ARN of the Step Function State Machine
    :type state_machine_arn: str
    :param name: The name of the execution.
    :type name: Optional[str]
    :param input: JSON data input to pass to the State Machine
    :type input: Union[Dict[str, any], str, None]
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param do_xcom_push: if True, execution_arn is pushed to XCom with key execution_arn.
    :type do_xcom_push: bool
    """

    template_fields = ["state_machine_arn", "name", "input"]
    template_ext = ()
    ui_color = "#f9c915"

    @apply_defaults
    def __init__(
        self,
        state_machine_arn: str,
        name: Optional[str] = None,
        input: Union[Dict[str, any], str, None] = None,
        aws_conn_id="aws_default",
        region_name=None,
        *args,
        **kwargs,
    ):
        if kwargs.get("xcom_push") is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")
        super().__init__(*args, **kwargs)
        self.state_machine_arn = state_machine_arn
        self.name = name
        self.input = input
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    def execute(self, context):
        hook = StepFunctionHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

        execution_arn = hook.start_execution(self.state_machine_arn, self.name, self.input)

        if execution_arn is None:
            raise AirflowException(f"Failed to start State Machine execution for: {self.state_machine_arn}")

        if self.do_xcom_push:
            context["ti"].xcom_push(key="execution_arn", value=execution_arn)

        self.log.info(f"Started State Machine execution for {self.state_machine_arn}: {execution_arn}")

        return execution_arn


class StepFunctionExecutionSensor(BaseSensorOperator):
    """
    Asks for the state of the Step Function State Machine Execution until it
    reaches a failure state or success state.
    If it fails, failing the task.

    On successful completion of the Execution the Sensor will do an XCom Push
    of the State Machine's output to `output`

    :param execution_arn: execution_arn to check the state of
    :type execution_arn: str
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    :type aws_conn_id: str
    """

    INTERMEDIATE_STATES = ("RUNNING",)
    FAILURE_STATES = (
        "FAILED",
        "TIMED_OUT",
        "ABORTED",
    )
    SUCCESS_STATES = ("SUCCEEDED",)

    template_fields = ["execution_arn"]
    template_ext = ()
    ui_color = "#66c3ff"

    @apply_defaults
    def __init__(self, execution_arn: str, aws_conn_id="aws_default", region_name=None, *args, **kwargs):
        if kwargs.get("xcom_push") is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")
        super().__init__(*args, **kwargs)
        self.execution_arn = execution_arn
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.hook = None

    def poke(self, context):
        execution_status = self.get_hook().describe_execution(self.execution_arn)
        state = execution_status["status"]
        output = json.loads(execution_status["output"]) if "output" in execution_status else None

        if state in self.FAILURE_STATES:
            raise AirflowException(f"Step Function sensor failed. State Machine Output: {output}")

        if state in self.INTERMEDIATE_STATES:
            return False

        self.log.info("Doing xcom_push of output")
        self.xcom_push(context, "output", output)
        return True

    def get_hook(self):
        """Create and return an StepFunctionHook"""
        if not self.hook:
            self.log.info(f"region_name: {self.region_name}")
            self.hook = StepFunctionHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
        return self.hook


class AwsOperatorsPlugin(AirflowPlugin):
    name = "aws_operators_plugin"
    hooks = [StepFunctionHook]
    operators = [StepFunctionStartExecutionOperator, StepFunctionGetExecutionOutputOperator]
    sensors = [StepFunctionExecutionSensor]
