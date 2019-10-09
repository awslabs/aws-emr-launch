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

from . import return_message, str2bool

emr = boto3.client('emr')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def handler(event, context):

    try:
        LOGGER.info('SFN metadata: {} (type = {})'.format(json.dumps(event), type(event)))
        cluster_input = event['ClusterInput']

        fail_if_job_running = True
        if 'FailIfJobRunning' in event:
            fail_if_job_running = str2bool(event['FailIfJobRunning'])

        # check if job flow already exists
        job_flow_name = cluster_input['Name']
        job_is_running = False
        LOGGER.info('Checking if job flow {} is running already'.format(job_flow_name))
        response = emr.list_clusters(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])
        for job_flow_running in response['Clusters']:
            jf_name = job_flow_running['Name']
            if jf_name == job_flow_name:
                LOGGER.info('Job flow {} is already running: terminate? {}'
                            .format(job_flow_name, str(fail_if_job_running)))
                job_is_running = True
                break

        if job_is_running and fail_if_job_running:
            return return_message(code=2, message='Job Flow already running')
        else:
            # run job
            LOGGER.info('Submitting new job flow {} with fail_if_job_running set to {}'
                        .format(json.dumps(cluster_input), str(fail_if_job_running)))
            response = emr.run_job_flow(**cluster_input)

            LOGGER.info('Got job flow response {}'.format(json.dumps(response)))
            job_flow = response['JobFlowId']

            response = emr.list_steps(
                ClusterId=job_flow
            )

            raw_steps = response['Steps']
            step_ids = [x['Id'] for x in raw_steps]
            LOGGER.info('Got job flow steps {}'.format(','.join(step_ids)))

            return return_message(step_ids=step_ids, cluster_id=job_flow)

    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed running flow {}: {}\n\n{}'.format(str(event), str(e), trc)
        LOGGER.error(s)
        return return_message(code=1, message=s)
