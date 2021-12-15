import json
import logging
from typing import Any, Dict, Optional, cast

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    logger.info(f"Lambda metadata: {json.dumps(event)} (type = {type(event)})")
    json_string = event.get("JsonString", {})

    try:
        return cast(Dict[str, Any], json.loads(json_string))

    except Exception as e:
        logger.error(f"Error processing event {json.dumps(event)}")
        logger.exception(e)
        raise e
