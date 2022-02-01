import json
import logging
import os
from typing import Any, Dict, Optional

import boto3
import botocore

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _get_botocore_config() -> botocore.config.Config:
    product = os.environ.get("AWS_EMR_LAUNCH_PRODUCT", "")
    version = os.environ.get("AWS_EMR_LAUNCH_VERSION", "")
    return botocore.config.Config(
        retries={"max_attempts": 5},
        connect_timeout=10,
        max_pool_connections=10,
        user_agent_extra=f"{product}/{version}",
    )


def _boto3_client(service_name: str) -> boto3.client:
    return boto3.Session().client(service_name=service_name, use_ssl=True, config=_get_botocore_config())


emr = _boto3_client("emr")


def handler(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Any:
    logger.info(f"Lambda metadata: {json.dumps(event)} (type = {type(event)})")
    new_tags = event.get("ExecutionInput", {}).get("Tags", [])
    cluster_config = event.get("Input", {})
    current_tags = cluster_config.get("Tags", [])

    try:
        new_tags_dict = {tag["Key"]: tag["Value"] for tag in new_tags}
        current_tags_dict = {tag["Key"]: tag["Value"] for tag in current_tags}

        merged_tags_dict = dict(current_tags_dict, **new_tags_dict)
        merged_tags = [{"Key": k, "Value": v} for k, v in merged_tags_dict.items()]

        cluster_config["Tags"] = merged_tags
        return cluster_config

    except Exception as e:
        logger.error(f"Error processing event {json.dumps(event)}")
        logger.exception(e)
        raise e
