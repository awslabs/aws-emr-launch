import boto3
import json
import logging
import traceback

from . import return_message

emr = boto3.client('emr')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def handler(event, context):

    try:
        LOGGER.info('SFN metadata: {} (type = {})'.format(json.dumps(event), type(event)))
        cluster_id = event.get('ClusterId', None)
        steps = event['Steps']

        LOGGER.info('Submitting steps {} to cluster {}'.format(json.dumps(steps), cluster_id))
        response = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=steps
        )

        LOGGER.info('Got step response {}'.format(json.dumps(response)))
        return return_message(step_ids=response['StepIds'], cluster_id=cluster_id)

    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed adding steps to cluster {}: {}\n\n{}'.format(cluster_id, str(e), trc)
        LOGGER.error(s)
        return return_message(code=1, message=s, cluster_id=cluster_id)
