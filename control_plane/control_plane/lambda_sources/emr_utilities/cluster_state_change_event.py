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

from . import *

emr = boto3.client('emr')
sfn = boto3.client('stepfunctions')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def handler(event, context):

    try:
        LOGGER.info('Lambda metadata: {} (type = {})'.format(json.dumps(event), type(event)))
        cluster_id = event['detail']['clusterId']
        state = event['detail']['state']
        state_change_reason = json.loads(event['detail']['stateChangeReason'])

        response = emr.describe_cluster(ClusterId=cluster_id)
        task_token = get_tag_value(response['Cluster']['Tags'], ClusterEventTags.STATE_CHANGE)

        if task_token is None:
            LOGGER.info('No {} Tag found in response: {}'.format(ClusterEventTags.STATE_CHANGE, response))
            return

        if state in ['WAITING', 'TERMINATED']:
            message = return_message(code=0, message='ClusterState: {}'.format(state))
        elif state == 'TERMINATED_WITH_ERRORS':
            message = return_message(code=1, message='ClusterState: {} StateChangeReason: {}'
                                     .format(state, state_change_reason))
        else:
            LOGGER.info('Sending Task Heartbeat, TaskToken: {}, ClusterState: {}'.format(task_token, state))
            sfn.send_task_heartbeat(taskToken=task_token)

        LOGGER.info('Removing Cluster Tag, ClusterId: {}, Tag: {}'.format(cluster_id, ClusterEventTags.STATE_CHANGE))
        emr.remove_tags(ResourceId=cluster_id, TagKeys=[ClusterEventTags.STATE_CHANGE])

        LOGGER.info('Sending Task Success, TaskToken: {}, Output: {}'.format(task_token, message))
        sfn.send_task_success(taskToken=task_token, output=json.dumps(message))

    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed handling state change {}: {}\n\n{}'.format(str(event), str(e), trc)
        LOGGER.error(s)
        raise e
