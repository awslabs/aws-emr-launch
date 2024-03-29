import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> List[str]:
    logger.info(f"Lambda metadata: {json.dumps(event)} (type = {type(event)})")
    # This will work with StepArgumentOverrides or StepArgOverrides
    overrides = event.get("ExecutionInput", {}).get("StepArgumentOverrides", None)
    if overrides is None:
        overrides = event.get("ExecutionInput", {}).get("StepArgOverrides", {})
    step_name = event.get("StepName", "")
    args = event.get("Args", [])

    try:
        step_overrides = overrides.get(step_name, {})
        overridden_args: List[str] = [step_overrides.get(arg, arg) for arg in args]
        logger.info(f"Overridden Args: {overridden_args}")
        return overridden_args

    except Exception as e:
        logger.error(f"Error processing event {json.dumps(event)}")
        logger.exception(e)
        raise e
