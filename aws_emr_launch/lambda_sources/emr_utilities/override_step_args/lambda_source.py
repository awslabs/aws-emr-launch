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

import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.info(f'Lambda metadata: {json.dumps(event)} (type = {type(event)})')
    # This will work with StepArgumentOverrides or StepArgOverrides
    overrides = event.get('ExecutionInput', {}).get('StepArgumentOverrides', None)
    if overrides is None:
        overrides = event.get('ExecutionInput', {}).get('StepArgOverrides', {})
    step_name = event.get('StepName', '')
    args = event.get('Args', [])

    try:
        step_overrides = overrides.get(step_name, {})
        overridden_args = [step_overrides.get(arg, arg) for arg in args]
        logger.info(f'Overridden Args: {overridden_args}')
        return overridden_args

    except Exception as e:
        logger.error(f'Error processing event {json.dumps(event)}')
        logger.exception(e)
        raise e
