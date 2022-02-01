import json
import logging
import os
from datetime import date, datetime
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
events = _boto3_client("events")
sfn = _boto3_client("stepfunctions")


def json_serial(obj: object) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def log_exception(e: Exception, event: Dict[str, Any]) -> None:
    logger.error(f"Error processing event {json.dumps(event)}")
    logger.exception(e)


def handler(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> None:
    logger.info(f"Lambda metadata: {json.dumps(event)} (type = {type(event)})")
    cluster_id = event["ClusterId"]
    task_token = event["TaskToken"]
    rule_name = event["RuleName"]
    expected_state = event["ExpectedState"]

    try:
        cluster_description = emr.describe_cluster(ClusterId=cluster_id)
        state = cluster_description["Cluster"]["Status"]["State"]

        if state == expected_state:
            success = True
        elif state in ["TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS"]:
            success = False
        else:
            heartbeat = {
                "ClusterId": cluster_id,
                "TaskToken": task_token,
                "ClusterState": state,
                "ExpectedState": expected_state,
            }
            logger.info(f"Sending Task Heartbeat: {heartbeat}")
            sfn.send_task_heartbeat(taskToken=task_token)
            return

        cluster_description["ClusterId"] = cluster_id

        if success:
            logger.info(
                f"Sending Task Success, TaskToken: {task_token}, "
                f"Output: {json.dumps(cluster_description, default=json_serial)}"
            )
            sfn.send_task_success(taskToken=task_token, output=json.dumps(cluster_description, default=json_serial))
        else:
            logger.info(
                f"Sending Task Failure,TaskToken: {task_token}, "
                f"Output: {json.dumps(cluster_description, default=json_serial)}"
            )
            sfn.send_task_failure(
                taskToken=task_token,
                error="States.TaskFailed",
                cause=json.dumps(cluster_description, default=json_serial),
            )

        task_token = None

        logger.info(f"Removing Rule Targets: {cluster_id}")
        failed_targets = events.remove_targets(Rule=rule_name, Ids=[cluster_id])

        if failed_targets["FailedEntryCount"] > 0:
            failed_entries = failed_targets["FailedEntries"]
            raise Exception(f"Failed Removing Targets: {json.dumps(failed_entries)}")

        targets = events.list_targets_by_rule(Rule=rule_name)["Targets"]
        if len(targets) == 0:
            logger.info(f"Disabling Rule with no Targets: {rule_name}")
            events.disable_rule(Name=rule_name)

    except Exception as e:
        try:
            if task_token:
                logger.error(f"Sending TaskFailure: {task_token}")
                sfn.send_task_failure(taskToken=task_token, error="States.TaskFailed", cause=str(e))
            logger.error(f"Removing Rule Targets: {cluster_id}")
            events.remove_targets(Rule=rule_name, Ids=[cluster_id])
        except Exception as ee:
            logger.exception(ee)
        log_exception(e, event)
        raise e
