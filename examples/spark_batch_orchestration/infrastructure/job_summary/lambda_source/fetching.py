import gzip
import io
import json
import boto3
from helpers import *

sfn_client = boto3.client("stepfunctions")
emr_client = boto3.client("emr")
s3_client = boto3.client("s3")


def get_sfn_execution_info(execution_arn):
    response = sfn_client.describe_execution(executionArn=execution_arn)

    info = {
        "sfnExecutionArn": execution_arn,
        "status": response["status"],
        "input": json.loads(response["input"]),
    }

    return info


def get_sfn_execution_events(execution_arn):
    params = {
        "executionArn": execution_arn,
        "maxResults": 1000,
    }

    events = []
    keep_fetching = True

    while keep_fetching:
        response = sfn_client.get_execution_history(**params)
        events += response["events"]
        if "nextToken" in response.keys():
            params["nextToken"] = response["nextToken"]
        else:
            keep_fetching = False

    return events


def get_emr_cluster_info(cluster_id):
    response = emr_client.describe_cluster(ClusterId=cluster_id)

    info = {
        "emrClusterId": response["Cluster"]["Id"],
        "emrClusterName": response["Cluster"]["Name"],
        "status": response["Cluster"]["Status"]["State"],
        "logUri": response["Cluster"]["LogUri"],
    }

    return info


def get_emr_cluster_steps(cluster_id):
    params = {
        "ClusterId": cluster_id,
    }

    steps = []
    keep_fetching = True

    while keep_fetching:
        response = emr_client.list_steps(**params)
        steps += response["Steps"]
        if "Marker" in response.keys():
            params["Marker"] = response["Marker"]
        else:
            keep_fetching = False

    return steps


def download_logs(step_log_uri):
    bucket_name, object_key = parse_s3_uri(step_log_uri)

    # Download the *.gz file
    gz_file = io.BytesIO()
    s3_client.download_fileobj(bucket_name, object_key, gz_file)

    # Decompress
    gz_file.seek(0)
    log_file = gzip.GzipFile(fileobj=gz_file)

    # Decode into text
    log_content = log_file.read().decode("UTF-8")

    return log_content
