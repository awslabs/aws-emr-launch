# Copyright 2019 Amazon.com, Inc. and its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the 'License').
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#   http://aws.amazon.com/asl/
#
# or in the 'license' file accompanying this file. This file is distributed
# on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

import logging

from botocore.stub import Stubber

from control_plane.lambda_sources.emr_utilities import (
    return_message
)
from control_plane.lambda_sources.emr_utilities import add_job_flow_steps

# Turn the LOGGER off for the tests
add_job_flow_steps.LOGGER.setLevel(logging.WARN)


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
