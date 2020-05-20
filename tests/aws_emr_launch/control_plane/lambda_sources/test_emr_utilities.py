import logging

from botocore.stub import Stubber

from control_plane.lambda_sources.emr_utilities.utils import return_message
from control_plane.lambda_sources.emr_utilities import run_job_flow

# Turn the LOGGER off for the tests
run_job_flow.LOGGER.setLevel(logging.WARN)


def test_add_job_flow_steps():
    event = {
        'ClusterId': 'j-00',
        'Steps': []
    }

    response = {
        'StepIds': ['step-00', 'step-01']
    }

    expected_params = {
        'JobFlowId': event['ClusterId'],
        'Steps': event['Steps']
    }

    message = return_message(step_ids=response['StepIds'], cluster_id=event['ClusterId'])

    with Stubber(add_job_flow_steps.emr) as stubber:
        stubber.add_response('add_job_flow_steps', response, expected_params)
        result = add_job_flow_steps.handler(event, None)

    assert message == result
