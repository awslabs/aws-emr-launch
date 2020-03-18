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
import base64
import logging

from typing import Dict, List
from botocore.exceptions import ClientError

logger = logging.getLogger()
emr = boto3.client('emr')
events = boto3.client('events')
secretsmanager = boto3.client('secretsmanager')


class SecretNotFoundError(Exception):
    pass


class SecretDecryptionFailureError(Exception):
    pass


def get_secret_value(secret_id: str):
    try:
        secret_response = secretsmanager.get_secret_value(
            SecretId=secret_id
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise SecretDecryptionFailureError(f'SecretDecryptionFailure: {secret_id}')
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise SecretNotFoundError(f'SecretNotFound: {secret_id}')
        else:
            raise e

    val = json.loads(secret_response.pop('SecretString')) \
        if 'SecretString' in secret_response \
        else json.loads(base64.b64decode(secret_response.pop('SecretBinary')))
    logger.info(f'SecretFound: {secret_id}')
    return val


def log_and_raise(e, event):
    logger.error(f'Error processing event {json.dumps(event)}')
    logger.exception(e)
    raise e


def update_configurations(configurations: List[dict], classification: str, properties: Dict[str, str]):
    found_classification = False
    for config in configurations:
        cls = config.get('Classification', '')
        if cls == classification:
            found_classification = True
            config['Properties'] = dict(config.get('Properties', {}), **properties)

    if not found_classification:
        configurations.append({
            'Classification': classification,
            'Properties': properties
        })

    return configurations


def handler(event, context):
    try:
        logger.info(f'Lambda metadata: {json.dumps(event)} (type = {type(event)})')
        cluster_configuration = event['ClusterConfiguration']
        task_token = event.get('TaskToken', None)
        cluster_status_lambda = event.get('CheckStatusLambda', None)
        secret_configurations = event.get('SecretConfigurations', None)
        rule_name = event.get('RuleName', None)

        # NoneType values need to be removed from the cluster_configuration
        logger.info(f'Preparing ClusterConfiguration: {json.dumps(cluster_configuration)}')
        cluster_configuration = {k: v for k, v in cluster_configuration.items() if v is not None}
        cluster_configuration['Instances'] = {
            k: v for k, v in cluster_configuration['Instances'].items() if v is not None}
        logger.info(f'Removed NoneType values from ClusterConfiguration: {json.dumps(cluster_configuration)}')

        if secret_configurations:
            logger.info(f'Getting SecretConfigurations: {json.dumps(secret_configurations)}')
            for classification, secret_id in secret_configurations.items():
                properties = get_secret_value(secret_id)
                cluster_configuration['Configurations'] = update_configurations(
                    cluster_configuration['Configurations'], classification, properties)

        logger.info('Calling RunJobFlow')
        response = emr.run_job_flow(**cluster_configuration)

        logger.info(f'Got JobFlow response {json.dumps(response)}')
        cluster_id = response['JobFlowId']

        target_input = {
            'Id': cluster_id,
            'Arn': cluster_status_lambda,
            'Input': json.dumps({
                'ClusterId': cluster_id,
                'TaskToken': task_token,
                'RuleName': rule_name,
                'ExpectedState': 'WAITING'
            })
        }
        logger.info(f'Putting Rule Targets: {json.dumps(target_input)}')
        failed_targets = events.put_targets(Rule=rule_name, Targets=[target_input])
        if failed_targets['FailedEntryCount'] > 0:
            failed_entries = failed_targets['FailedEntries']
            raise Exception(f'Failed Putting Targets: {json.dumps(failed_entries)}')

        logger.info(f'Enabling Rule: {rule_name}')
        events.enable_rule(Name=rule_name)
    except Exception as e:
        log_and_raise(e, event)
