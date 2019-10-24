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
        overrides = event.get('ExecutionInput', {}).get('ClusterConfigOverrides', {})
        cluster_config = event.get('ClusterConfig', {})



    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed checking flow {}: {}\n\n{}'.format(str(event), str(e), trc)
        LOGGER.error(s)
        return return_message(code=1, message=s)
