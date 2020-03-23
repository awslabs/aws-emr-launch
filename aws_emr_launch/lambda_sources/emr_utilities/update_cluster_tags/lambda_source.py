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

logger = logging.getLogger()
logger.setLevel(logging.INFO)
emr = boto3.client('emr')


def handler(event, context):
    logger.info(f'Lambda metadata: {json.dumps(event)} (type = {type(event)})')
    new_tags = event.get('ExecutionInput', {}).get('Tags', [])
    cluster_config = event.get('ClusterConfiguration', {})
    current_tags = cluster_config.get('Tags', [])

    try:
        new_tags_dict = {tag['Key']: tag['Value'] for tag in new_tags}
        current_tags_dict = {tag['Key']: tag['Value'] for tag in current_tags}

        merged_tags_dict = dict(current_tags_dict, **new_tags_dict)
        merged_tags = [{'Key': k, 'Value': v} for k, v in merged_tags_dict.items()]

        cluster_config['Tags'] = merged_tags
        return cluster_config

    except Exception as e:
        logger.error(f'Error processing event {json.dumps(event)}')
        logger.exception(e)
        raise e
