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


def handler(event, context):

    try:
        LOGGER.info('Lambda metadata: {} (type = {})'.format(json.dumps(event), type(event)))
        cluster_id = event['ClusterId']

        # Terminate Job FLow
        LOGGER.info('Terminating job flow {}'.format(json.dumps(cluster_id)))
        response = emr.terminate_job_flows(JobFlowIds=[cluster_id])

        LOGGER.info('Got job flow response {}'.format(json.dumps(response)))
    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed terminating flow {}: {}\n\n{}'.format(str(event), str(e), trc)
        LOGGER.error(s)
        raise e
