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

from typing import Dict, List
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
emr = boto3.client('emr')

PROFILES_SSM_PARAMETER_PREFIX = '/emr_launch/emr_profiles'
CONFIGURATIONS_SSM_PARAMETER_PREFIX = '/emr_launch/cluster_configurations'

ssm = boto3.client('ssm')


class EMRProfileNotFoundError(Exception):
    pass


class ClusterConfigurationNotFoundError(Exception):
    pass


def get_parameter_value(ssm_parameter_prefix: str, name: str, namespace: str = 'default'):
    configuration_json = ssm.get_parameter(
        Name=f'{ssm_parameter_prefix}/{namespace}/{name}')['Parameter']['Value']
    return json.loads(configuration_json)


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
    logger.info(f'Lambda metadata: {json.dumps(event)} (type = {type(event)})')
    cluster_name = event.get('ClusterName', '')
    tags = event.get('ClusterTags', [])
    profile_namespace = event.get('ProfileNamespace', '')
    profile_name = event.get('ProfileName', '')
    configuration_namespace = event.get('ConfigurationNamespace', '')
    configuration_name = event.get('ConfigurationName', '')

    if not cluster_name:
        cluster_name = configuration_name

    emr_profile = None
    cluster_configuration = None

    try:
        emr_profile = get_parameter_value(
            ssm_parameter_prefix=PROFILES_SSM_PARAMETER_PREFIX,
            namespace=profile_namespace,
            name=profile_name)
        logger.info(f'ProfileFound: {json.dumps(emr_profile)}')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ParameterNotFound':
            log_and_raise(EMRProfileNotFoundError(f'ProfileNotFound: {profile_namespace}/{profile_name}'), event)
        else:
            log_and_raise(e, event)

    try:
        cluster_configuration = get_parameter_value(
            ssm_parameter_prefix=CONFIGURATIONS_SSM_PARAMETER_PREFIX,
            namespace=configuration_namespace,
            name=configuration_name)
        logger.info(f'ConfigurationFound: {json.dumps(cluster_configuration)}')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ParameterNotFound':
            log_and_raise(ClusterConfigurationNotFoundError(
                f'ConfigurationNotFound: {configuration_namespace}/{configuration_name}'), event)
        else:
            log_and_raise(e, event)

    try:
        logs_bucket = emr_profile.get('LogsBucket', None)
        logs_path = emr_profile.get('LogsPath', '')

        cluster_configuration = cluster_configuration['ClusterConfiguration']

        cluster_configuration['Name'] = cluster_name
        cluster_configuration['LogUri'] = \
            os.path.join(f's3://{logs_bucket}', logs_path, cluster_name) if logs_bucket else None
        cluster_configuration['JobFlowRole'] = emr_profile['Roles']['InstanceRole'].split('/')[-1]
        cluster_configuration['ServiceRole'] = emr_profile['Roles']['ServiceRole'].split('/')[-1]
        cluster_configuration['AutoScalingRole'] = emr_profile['Roles']['AutoScalingRole'].split('/')[-1] \
            if len(cluster_configuration['Instances'].get('InstanceGroups', [])) > 0 else None
        cluster_configuration['Tags'] = tags
        cluster_configuration['Instances']['EmrManagedMasterSecurityGroup'] = \
            emr_profile['SecurityGroups']['MasterGroup']
        cluster_configuration['Instances']['EmrManagedSlaveSecurityGroup'] = \
            emr_profile['SecurityGroups']['WorkersGroup']
        cluster_configuration['Instances']['ServiceAccessSecurityGroup'] = \
            emr_profile['SecurityGroups']['ServiceGroup'] \
            if 'ServiceGroup' in emr_profile['SecurityGroups'] else None
        cluster_configuration['SecurityConfiguration'] = emr_profile.get('SecurityConfigurationName', None)

        logger.info(f'ClusterConfig: {json.dumps(cluster_configuration)}')
        return cluster_configuration

    except Exception as e:
        log_and_raise(e, event)
