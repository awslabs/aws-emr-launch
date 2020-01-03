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

import json
import boto3
import logging
import traceback

from botocore.exceptions import ClientError

ssm = boto3.client('ssm')
sfn = boto3.client('stepfunctions')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

PARAMETER_STORE_PREFIX = '/emr_launch/control_plane/task_tokens/emr_utilities/{}/{}'


def cluster_state_change_key(cluster_id):
    return PARAMETER_STORE_PREFIX.format('cluster_state', cluster_id)


def step_state_change_key(step_id):
    return PARAMETER_STORE_PREFIX.format('step_state', step_id)


def handler(event, context):

    LOGGER.info('Lambda metadata: {} (type = {})'.format(json.dumps(event), type(event)))
    cluster_id = event['detail']['clusterId']
    state = event['detail']['state']
    state_change_reason = json.loads(event['detail']['stateChangeReason'])

    parameter_name = cluster_state_change_key(cluster_id)
    LOGGER.info('Getting TaskToken from Parameter Store: {}'.format(parameter_name))
    try:
        parameter_value = json.loads(ssm.get_parameter(Name=parameter_name)['Parameter']['Value'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'ParameterNotFound':
            parameter_value = None
        else:
            raise e

    task_token = parameter_value.get('TaskToken', None) if parameter_value is not None else None

    try:
        if task_token is None:
            LOGGER.info('No {} Parameter found'.format(parameter_name))
            return

        if state == 'WAITING':
            success = True
        elif state == 'TERMINATED_WITH_ERRORS':
            success = False
        elif state == 'TERMINATED':
            success = parameter_value.get('TerminationRequested', False)
        else:
            LOGGER.info(f'Sending Task Heartbeat, TaskToken: {task_token}, ClusterState: {state}')
            sfn.send_task_heartbeat(taskToken=task_token)
            return

        message = {
            'ClusterId': cluster_id,
            'ClusterState': state,
            'StateChangeReason': state_change_reason
        }

        LOGGER.info(f'Removing TaskToken Parameter: {parameter_name}')
        ssm.delete_parameter(Name=parameter_name)

        if success:
            LOGGER.info(f'Sending Task Success, TaskToken: {task_token}, Output: {message}')
            sfn.send_task_success(taskToken=task_token, output=json.dumps(message))
        else:
            LOGGER.info(f'Sending Task Failure, TaskToken: {task_token}, Output: {message}')
            sfn.send_task_failure(taskToken=task_token, error='ClusterFailedError', cause=json.dumps(message))

    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed handling state change {}: {}\n\n{}'.format(str(event), str(e), trc)
        LOGGER.error(s)
        if task_token:
            sfn.send_task_failure(taskToken=task_token, error=str(e), cause=s)
        raise e
