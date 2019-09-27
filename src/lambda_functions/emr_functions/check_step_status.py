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

import boto3
import json
import logging
import traceback

from . import return_message

emr = boto3.client('emr')
cw = boto3.client('cloudwatch')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

CW_METRIC_NAME = 'StepRunTime'
CW_METRIC_NAME_EMR_BOOTSTRAP = 'ClusterBootstrapTime'
CW_METRIC_NAME_EMR_TOTAL = 'ClusterTotalTime'
CW_DIM_NAME = 'JobName'


def put_cluster_metrics(namespace, cluster_id):
    if namespace is None:
        LOGGER.warning('Missing CloudWatch Namespace while attempting to write EMR cluster metrics '
                       'for cluster ' + str(cluster_id))
        return

    LOGGER.info('Writing EMR cluster metrics for cluster ' + str(cluster_id))
    try:
        response = emr.describe_cluster(
            ClusterId=cluster_id
        )
        timeline = response['Cluster']['Status']['Timeline']
        timeline_start = timeline['CreationDateTime']
        timeline_ready = timeline['ReadyDateTime']
        timeline_delta_ready = timeline_ready - timeline_start
        timeline_sec_ready = timeline_delta_ready.total_seconds()
        put_cw_metric(namespace, CW_METRIC_NAME_EMR_BOOTSTRAP, CW_DIM_NAME,
                      response['Cluster']['Name'], timeline_sec_ready)
    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed putting cluster metrics: {}\n\n{}'.format(str(e), trc)
        LOGGER.error(s)


def put_cw_metric(namespace, metric_name, dim_name, dim_value, metric_value, metric_unit='Seconds'):
    if namespace is None:
        LOGGER.warning('Missing CloudWatch Namespace while attempting to write metric {}={} to namespace '
                       '{} using dimension {}={}'.format(metric_name, metric_value, namespace, dim_name, dim_value))
        return

    LOGGER.info('Writing metric {}={} to namespace {} using dimension {}={}'.format(
        metric_name, metric_value, namespace, dim_name, dim_value))
    try:
        cw.put_metric_data(
            Namespace=namespace,
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Dimensions': [
                        {
                            'Name': dim_name,
                            'Value': dim_value
                        },
                    ],
                    'Value': metric_value,
                    'Unit': metric_unit,
                    'StorageResolution': 60
                },
            ]
        )
    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed putting CW metrics: {}\n\n{}'.format(str(e), trc)
        LOGGER.error(s)


def handler(event, context):

    try:
        LOGGER.info('SFN metadata: {} (type = {})'.format(json.dumps(event), type(event)))

        # The following supports input with {'Result': {'ClusterId': ''}} or {'ClusterId': ''}
        cluster_id = event.get('Result', event).get('ClusterId', None)
        # The following supports input with {'Result': {'StepIds': ''}} or {'StepIds': ''}
        step_ids = event.get('Result', event).get('StepIds', None)
        namespace = event.get('CloudWatchNamespace', None)

        LOGGER.info('Checking status of steps {} on cluster {}'.format(json.dumps(step_ids), cluster_id))
        # assume all ok, will set to something else based on job status
        overall_status = 0
        overall_msg = ''
        for step_id in step_ids:
            response = emr.describe_step(
                ClusterId=cluster_id,
                StepId=step_id
            )
            LOGGER.info('Got step response {}'.format(str(response)))
            step_status = response['Step']['Status']['State']
            if step_status in ['PENDING', 'RUNNING']:
                overall_status = 2
                overall_msg = overall_msg + 'Step ' + step_id + ' still in progress;'
            elif step_status in ['CANCEL_PENDING', 'CANCELLED', 'FAILED', 'INTERRUPTED']:
                overall_status = 1
                overall_msg = overall_msg + 'Step ' + step_id + ' failed;'
                if step_status == 'FAILED':
                    failure_detail = response['Step']['Status']['FailureDetails']
                    overall_msg = overall_msg + 'Failure Details: ' + json.dumps(failure_detail)
                break
            else:
                overall_msg = overall_msg + 'Step ' + step_id + ' completed'
                timeline = response['Step']['Status']['Timeline']
                timeline_start = timeline['StartDateTime']
                timeline_end = timeline['EndDateTime']
                timeline_delta = timeline_end - timeline_start
                timeline_sec = timeline_delta.total_seconds()
                if namespace is not None:
                    put_cw_metric(namespace, CW_METRIC_NAME, CW_DIM_NAME, response['Step']['Name'], timeline_sec)

        if overall_status == 0 and namespace is not None:
            put_cluster_metrics(namespace, cluster_id)

        return return_message(code=overall_msg, message=overall_msg)

    except Exception as e:
        trc = traceback.format_exc()
        s = 'Failed checking step status on cluster {}: {}\n\n{}'.format(cluster_id, str(e), trc)
        LOGGER.error(s)
        return return_message(code=1, message=s, cluster_id=cluster_id)
