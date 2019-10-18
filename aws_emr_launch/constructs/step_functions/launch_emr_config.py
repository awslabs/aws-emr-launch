# Copyright 2019 Amazon.com, Inc. and its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the 'License').
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#   http://aws.amazon.com/asl/
#
# or in the 'license' file accompanying this file. This file is distributed
# on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

from aws_cdk import (
    aws_lambda,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    core
)


class LaunchEMRConfigStack(core.Stack):
    def __init__(self, app: core.App, id: str, *,
                 run_job_flow_lambda: aws_lambda.Function,
                 check_step_status_lambda: aws_lambda.Function,
                 **kwargs) -> None:
        super().__init__(app, id, **kwargs)

        run_job_flow = sfn.Task(
            self, 'LaunchEMRConfigStack_RunJobFlow',
            input_path='$',
            output_path='$',
            result_path='$.Result',
            task=sfn_tasks.RunLambdaTask(
                run_job_flow_lambda,
                integration_pattern=sfn.ServiceIntegrationPattern.WAIT_FOR_TASK_TOKEN,
                payload={'TaskToken': sfn.Context.task_token}))


        run_job_flow.to_string()
