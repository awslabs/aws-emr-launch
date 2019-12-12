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


def handler(event, context):
    LOGGER.info('Lambda metadata: {} (type = {})'.format(json.dumps(event), type(event)))
    overrides = event.get('ExecutionInput', {}).get('ClusterConfigOverrides', {})
    allowed_overrides = event.get('AllowedClusterConfigOverrides', None)
    cluster_config = event.get('ClusterConfig', {})

    try:
        for path, new_value in overrides.items():
            if allowed_overrides:
                path = allowed_overrides.get(path, None)
                if path is None:
                    continue

            path_parts = path.split('.')
            update_key = path_parts[-1]
            path = '.'.join(path_parts[0:-1])

            update_key = int(update_key) if update_key.isdigit() else update_key
            update_attr = cluster_config \
                if path == '' else dictor(cluster_config, path)

            LOGGER.info('Path: "{}" CurrentValue: "{}" NewValue: "{}"'.format(path, update_attr[update_key], new_value))
            update_attr[update_key] = new_value

        return cluster_config

    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed overriding configs {}: {}\n\n{}'.format(str(event), str(e), trc)
        LOGGER.error(s)
        raise e
