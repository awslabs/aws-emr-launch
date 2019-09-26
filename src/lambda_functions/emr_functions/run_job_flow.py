import boto3
import json
import logging
import traceback

from . import return_message, str2bool

emr = boto3.client('emr')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def handler(event, context):

    try:
        LOGGER.info("SFN metadata: {0} (type = {1})".format(json.dumps(event), type(event)))
        cluster_input = event['ClusterInput']

        fail_if_job_running = True
        if 'FailIfJobRunning' in event:
            fail_if_job_running = str2bool(event['FailIfJobRunning'])

        # check if job flow already exists
        job_flow_name = cluster_input['Name']
        is_job_running = False
        LOGGER.info("Checking if job flow {0} is running already".format(job_flow_name))
        response = emr.list_clusters(ClusterStates=['STARTING','BOOTSTRAPPING','RUNNING','WAITING'])
        for job_flow_running in response['Clusters']:
            jf_name = job_flow_running['Name']
            if jf_name == job_flow_name:
                LOGGER.info("Job flow {0} is already running: terminate? {1}"
                            .format(job_flow_name, str(fail_if_job_running)))
                is_job_running = True
                break

        if is_job_running and fail_if_job_running:
            return return_message(code=2, message='Job Flow already running')
        else:
            # run job
            LOGGER.info("Submitting new job flow {0} with fail_if_job_running set to {1}"
                        .format(json.dumps(cluster_input), str(fail_if_job_running)))
            response = emr.run_job_flow( **cluster_input )

            LOGGER.info("Got job flow response {0}".format(json.dumps(response)))
            job_flow = response['JobFlowId']

            response = emr.list_steps(
                ClusterId=job_flow
            )

            raw_steps = response['Steps']
            step_ids = [x['Id'] for x in raw_steps]
            LOGGER.info("Got job flow steps {0}".format(",".join(step_ids)))

            return return_message(step_ids=step_ids, cluster_id=job_flow)

    except Exception as e:
        trc = traceback.format_exc()
        s = "Failed running flow {0}: {1}\n\n{2}".format(str(event), str(e), trc)
        LOGGER.error(s)
        return return_message(code=1, message=s)