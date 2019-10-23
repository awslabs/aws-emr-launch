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

from utils import *

emr = boto3.client('emr')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def handler(event, context):

    try:
        LOGGER.info('Lambda metadata: {} (type = {})'.format(json.dumps(event), type(event)))

        # This will work for {"JobInput": {"FailIfJobRunning": true}} or {"FailIfJobRunning": true}
        fail_if_job_running = parse_bool(event.get('ExecutionInput', event).get('FailIfJobRunning', False))

        # check if job flow already exists
        if fail_if_job_running:
            cluster_name = event.get('ClusterName', '')
            job_is_running = False
            LOGGER.info('Checking if job flow {} is running already'.format(cluster_name))
            response = emr.list_clusters(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])
            for job_flow_running in response['Clusters']:
                jf_name = job_flow_running['Name']
                if jf_name == cluster_name:
                    LOGGER.info('Job flow {} is already running: terminate? {}'
                                .format(cluster_name, str(fail_if_job_running)))
                    job_is_running = True
                    break

            if job_is_running and fail_if_job_running:
                return return_message(code=2, message='Job Flow already running')
            else:
                return return_message(code=0, message='Job Flow is not running')

        else:
            return return_message(code=0, message='FailIfJobRunning is: {}'.format(fail_if_job_running))

    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed checking flow {}: {}\n\n{}'.format(str(event), str(e), trc)
        LOGGER.error(s)
        return return_message(code=1, message=s)
