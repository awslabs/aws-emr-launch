import json
import logging
import os
import traceback

import boto3

sfn = boto3.client("stepfunctions")

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def handler(event, context):
    LOGGER.info("Lambda metadata: {} (type = {})".format(json.dumps(event), type(event)))

    try:
        pipeline_arn = os.environ.get("PIPELINE_ARN", "")
        pipeline_input = json.dumps(
            {"ClusterConfigurationOverrides": {"ClusterName": "sns-triggered-pipeline"}, "Tags": []}
        )
        sfn.start_execution(stateMachineArn=pipeline_arn, input=pipeline_input)

        LOGGER.info(f'Started StateMachine {pipeline_arn} with input "{pipeline_input}"')

    except Exception as e:
        trc = traceback.format_exc()
        s = "Failed parsing JSON {}: {}\n\n{}".format(str(event), str(e), trc)
        LOGGER.error(s)
        raise e
