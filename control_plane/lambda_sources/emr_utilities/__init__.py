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
import datetime

from typing import List, Optional

def str2bool(v: str) -> bool:
    return v.lower() in ("yes", "true", "t", "1")


def return_message(code: int = 0, step_ids: Optional[List[str]] = None,
                   message: str = '', cluster_id: str = '') -> dict:
    return {'Code': code, 'StepIds': step_ids, 'Message': message, 'ClusterId': cluster_id}


def get_tag_value(tags: List[dict], key: str) -> Optional[str]:
    for tag in tags:
        if tag['Key'] == key:
            return tag['Value']
    return None


def upsert_tag_value(tags: List[dict], key: str, value: str) -> List[dict]:
    for tag in tags:
        if tag['Key'] == key:
            tag['Key'] = value
            return tags
    tags.append({
        'Key': key,
        'Value': value
    })
    return tags


class JSONDateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


class ClusterEventTags:
    STATE_CHANGE = 'TaskToken-ClusterState'
    STEP_CHANGE = 'TaskToke-StepChange'
