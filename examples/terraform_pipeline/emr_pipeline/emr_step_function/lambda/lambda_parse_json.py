import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.info(f'Lambda metadata: {json.dumps(event)} (type = {type(event)})')
    json_string = event.get('LaunchOutput', {}).get('LaunchOutput', {})

    try:
        parsed = json.loads(json_string)['LaunchClusterResult']
        return parsed

    except Exception as e:
        logger.error(f'Error processing event {json.dumps(event)}')
        logger.exception(e)
        raise e
