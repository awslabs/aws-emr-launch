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

from botocore.exceptions import ClientError

emr = boto3.client('emr')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

CONFIGURATIONS_SSM_PARAMETER_PREFIX = '/emr_launch/cluster_configurations'

ssm = boto3.client('ssm')


class ClusterConfigurationNotFoundError(Exception):
    pass


def handler(event, context):
    LOGGER.info('Lambda metadata: {} (type = {})'.format(json.dumps(event), type(event)))
    namespace = event.get('Namespace', '')
    configuration_name = event.get('ConfigurationName', '')

    try:
        configuration_json = ssm.get_parameter(
            Name=f'{CONFIGURATIONS_SSM_PARAMETER_PREFIX}/{namespace}/{configuration_name}')['Parameter']['Value']
        return json.loads(configuration_json)

    except ClientError as e:
        if e.response['Error']['Code'] == 'ParameterNotFound':
            LOGGER.error(f'ConfigurationNotFound: {namespace}/{configuration_name}')
            raise ClusterConfigurationNotFoundError(f'ConfigurationNotFound: {namespace}/{configuration_name}')
        else:
            trc = traceback.format_exc()
            s = 'Error processing event {}: {}\n\n{}'.format(str(event), str(e), trc)
            LOGGER.error(s)
            raise e
    except Exception as e:
        trc = traceback.format_exc()
        s = 'Error processing event {}: {}\n\n{}'.format(str(event), str(e), trc)
        LOGGER.error(s)
        raise e
