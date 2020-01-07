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

import os
import json
import boto3
import logging
import traceback

sfn = boto3.client('stepfunctions')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def handler(event, context):
    LOGGER.info('Lambda metadata: {} (type = {})'.format(json.dumps(event), type(event)))

    try:
        pipeline_arn = os.environ.get('PIPELINE_ARN', '')
        pipeline_input = json.dumps({
            'ClusterConfigurationOverrides': {
                'Name': 'sns-triggered-pipeline'
            },
            'Tags': []
        })
        sfn.start_execution(
            stateMachineArn=pipeline_arn,
            input=pipeline_input
        )

        LOGGER.info(f'Started StateMachine {pipeline_arn} with input "{pipeline_input}"')

    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed parsing JSON {}: {}\n\n{}'.format(str(event), str(e), trc)
        LOGGER.error(s)
        raise e
