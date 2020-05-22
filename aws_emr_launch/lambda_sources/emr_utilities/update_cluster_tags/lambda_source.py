import json
import logging

import boto3

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
