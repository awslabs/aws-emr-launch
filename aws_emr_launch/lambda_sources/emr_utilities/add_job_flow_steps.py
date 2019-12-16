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

import boto3
import json
import logging
import traceback

emr = boto3.client('emr')
ssm = boto3.client('ssm')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


PARAMETER_STORE_PREFIX = '/emr_launch/control_plane/task_tokens/emr_utilities/{}/{}'


def cluster_state_change_key(cluster_id):
    return PARAMETER_STORE_PREFIX.format('cluster_state', cluster_id)


def step_state_change_key(step_id):
    return PARAMETER_STORE_PREFIX.format('step_state', step_id)


def handler(event, context):

    try:
        LOGGER.info('Lambda metadata: {} (type = {})'.format(json.dumps(event), type(event)))
        cluster_id = event.get('ClusterId', None)
        step = event.get('Step', None)
        task_token = event.get('TaskToken', None)

        LOGGER.info('Submitting Step {} to Cluster {}'.format(json.dumps(step), cluster_id))
        response = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[step]
        )

        LOGGER.info('Got step response {}'.format(json.dumps(response)))
        step_id = response['StepIds'][0]

        parameter_name = step_state_change_key(step_id)
        LOGGER.info('Putting TaskToken to Parameter Store: {}'.format(parameter_name))
        ssm.put_parameter(Name=parameter_name, Type='String', Value=task_token)
    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed adding steps to cluster {}: {}\n\n{}'.format(cluster_id, str(e), trc)
        LOGGER.error(s)
        raise e
