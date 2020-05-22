import json
import logging
from datetime import date, datetime

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
emr = boto3.client('emr')
events = boto3.client('events')
sfn = boto3.client('stepfunctions')


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def log_and_raise(e, event):
    logger.error(f'Error processing event {json.dumps(event)}')
    logger.exception(e)
    raise e


def handler(event, context):
    logger.info(f'Lambda metadata: {json.dumps(event)} (type = {type(event)})')
    cluster_id = event['ClusterId']
    task_token = event['TaskToken']
    rule_name = event['RuleName']
    expected_state = event['ExpectedState']

    try:
        cluster_description = emr.describe_cluster(ClusterId=cluster_id)
        state = cluster_description['Cluster']['Status']['State']

        if state == expected_state:
            success = True
        elif state in ['TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS']:
            success = False
        else:
            heartbeat = {
                'ClusterId': cluster_id,
                'TaskToken': task_token,
                'ClusterState': state,
                'ExpectedState': expected_state
            }
            logger.info(f'Sending Task Heartbeat: {heartbeat}')
            sfn.send_task_heartbeat(taskToken=task_token)
            return

        cluster_description['ClusterId'] = cluster_id

        if success:
            logger.info(f'Sending Task Success, TaskToken: {task_token}, '
                        f'Output: {json.dumps(cluster_description, default=json_serial)}')
            sfn.send_task_success(taskToken=task_token, output=json.dumps(cluster_description, default=json_serial))
        else:
            logger.info(f'Sending Task Failure,TaskToken: {task_token}, '
                        f'Output: {json.dumps(cluster_description, default=json_serial)}')
            sfn.send_task_failure(taskToken=task_token, error='States.TaskFailed',
                                  cause=json.dumps(cluster_description, default=json_serial))

        task_token = None

        logger.info(f'Removing Rule Targets: {cluster_id}')
        failed_targets = events.remove_targets(Rule=rule_name, Ids=[cluster_id])

        if failed_targets['FailedEntryCount'] > 0:
            failed_entries = failed_targets['FailedEntries']
            raise Exception(f'Failed Removing Targets: {json.dumps(failed_entries)}')

        targets = events.list_targets_by_rule(Rule=rule_name)['Targets']
        if len(targets) == 0:
            logger.info(f'Disabling Rule with no Targets: {rule_name}')
            events.disable_rule(Name=rule_name)

    except Exception as e:
        try:
            if task_token:
                logger.error(f'Sending TaskFailure: {task_token}')
                sfn.send_task_failure(taskToken=task_token, error='States.TaskFailed', cause=str(e))
            logger.error(f'Removing Rule Targets: {cluster_id}')
            events.remove_targets(Rule=rule_name, Ids=[cluster_id])
        except Exception as ee:
            logger.exception(ee)
        raise log_and_raise(e, event)
