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
        cluster_config = event['ClusterConfig']
        task_token = event.get('TaskToken', None)

        # run job
        LOGGER.info('Submitting new job flow {}'.format(json.dumps(cluster_config)))
        response = emr.run_job_flow(**cluster_config)

        LOGGER.info('Got job flow response {}'.format(json.dumps(response)))
        cluster_id = response['JobFlowId']

        parameter_name = cluster_state_change_key(cluster_id)
        LOGGER.info('Putting TaskToken to Parameter Store: {}'.format(parameter_name))

        parameter_value = {
            'TaskToken': task_token,
            'ExpectedState': 'WAITING'
        }

        ssm.put_parameter(Name=parameter_name, Type='String', Value=json.dumps(parameter_value))
    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed running flow {}: {}\n\n{}'.format(str(event), str(e), trc)
        LOGGER.error(s)
        raise e
