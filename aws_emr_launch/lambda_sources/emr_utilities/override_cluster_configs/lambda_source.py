import json
import logging

import boto3

from dictor import dictor

logger = logging.getLogger()
logger.setLevel(logging.INFO)
emr = boto3.client('emr')


class InvalidOverrideError(Exception):
    pass


def handler(event, context):
    logger.info(f'Lambda metadata: {json.dumps(event)} (type = {type(event)})')
    # This will work with ClusterConfigurationOverrides or ClusterConfigOverrides
    overrides = event.get('ExecutionInput', {}).get('ClusterConfigurationOverrides', None)
    if overrides is None:
        overrides = event.get('ExecutionInput', {}).get('ClusterConfigOverrides', {})

    allowed_overrides = event.get('AllowedClusterConfigOverrides', None)
    cluster_config = event.get('ClusterConfiguration', {})

    try:
        for path, new_value in overrides.items():
            minimum = None
            maximum = None

            if allowed_overrides:
                new_path = allowed_overrides.get(path, None)
                if new_path is None:
                    raise InvalidOverrideError(f'Value "{path}" is not an allowed cluster configuration override')
                else:
                    path = new_path['JsonPath']
                    minimum = new_path.get('Minimum', None)
                    maximum = new_path.get('Maximum', None)

            path_parts = path.split('.')
            update_key = path_parts[-1]
            key_path = '.'.join(path_parts[0:-1])

            update_key = int(update_key) if update_key.isdigit() else update_key
            update_attr = cluster_config \
                if key_path == '' else dictor(cluster_config, key_path)

            if update_attr is None or update_attr.get(update_key, None) is None:
                raise InvalidOverrideError(f'The update path "{path}" was not found in the cluster configuration')

            logger.info(f'Path: "{key_path}" CurrentValue: "{update_attr[update_key]}" NewValue: "{new_value}"')
            if (minimum or maximum) and (isinstance(new_value, int) or isinstance(new_value, float)):
                if minimum and new_value < minimum:
                    raise InvalidOverrideError(f'The Override Value ({new_value}) '
                                               f'is less than the Minimum allowed ({minimum})')
                if maximum and new_value > maximum:
                    raise InvalidOverrideError(f'The Override Value ({new_value}) '
                                               f'is greater than the Maximum allowed ({maximum})')

            update_attr[update_key] = new_value

        return cluster_config

    except Exception as e:
        logger.error(f'Error processing event {json.dumps(event)}')
        logger.exception(e)
        raise e
