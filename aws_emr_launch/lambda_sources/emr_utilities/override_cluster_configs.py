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

from dictor import dictor

emr = boto3.client('emr')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


class InvalidOverrideError(Exception):
    pass


def handler(event, context):
    LOGGER.info('Lambda metadata: {} (type = {})'.format(json.dumps(event), type(event)))
    # This will work with ClusterConfigurationOverrides or ClusterConfigOverrides
    overrides = event.get('ExecutionInput', {}).get('ClusterConfigurationOverrides', None)
    if overrides is None:
        overrides = event.get('ExecutionInput', {}).get('ClusterConfigOverrides', {})

    allowed_overrides = event.get('AllowedClusterConfigOverrides', None)
    cluster_config = event.get('ClusterConfig', {})

    try:
        for path, new_value in overrides.items():
            if allowed_overrides:
                new_path = allowed_overrides.get(path, None)
                if new_path is None:
                    raise InvalidOverrideError(f'Value "{path}" is not an allowed cluster configuration override')
                else:
                    path = new_path

            path_parts = path.split('.')
            update_key = path_parts[-1]
            key_path = '.'.join(path_parts[0:-1])

            update_key = int(update_key) if update_key.isdigit() else update_key
            update_attr = cluster_config \
                if key_path == '' else dictor(cluster_config, key_path)

            if update_attr is None or update_attr.get(update_key, None) is None:
                raise InvalidOverrideError(f'The update path "{path}" was not found in the cluster configuration')

            LOGGER.info(f'Path: "{key_path}" CurrentValue: "{update_attr[update_key]}" NewValue: "{new_value}"')
            update_attr[update_key] = new_value

        return cluster_config

    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed overriding configs {}: {}\n\n{}'.format(str(event), str(e), trc)
        LOGGER.error(s)
        raise e
