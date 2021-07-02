from fetching import *


def extract_sfn_execution_info(execution_arn):
    info = get_sfn_execution_info(execution_arn)
    events = get_sfn_execution_events(execution_arn)

    info["steps"] = extract_sfn_steps(events)

    return info


def extract_sfn_steps(events):
    groups = {}

    # Find step boundaries
    last_entered_step = None
    for e in events:
        if e["type"] == "TaskStateEntered":
            step_name = e["stateEnteredEventDetails"]["name"]
            groups[step_name] = {"fromId": e["id"]}
            last_entered_step = step_name
        if e["type"] == "TaskStateExited":
            step_name = e["stateExitedEventDetails"]["name"]
            groups[step_name]["toId"] = e["id"]
        if e["type"] == "TaskStateAborted":
            step_name = last_entered_step
            groups[step_name]["toId"] = e["id"]

    # Group events by step
    for group in groups.values():
        group["events"] = [e for e in events if group["fromId"] <= e["id"] <= group["toId"]]

    # Keep original order
    sorted_groups = sorted([g for g in groups.items()], key=lambda kv: kv[1]["fromId"])

    return [extract_sfn_step_info(k, v) for k, v in sorted_groups]


def extract_sfn_step_info(step_name, group):
    events = group["events"]
    events_by_id = {e["id"]: e for e in events}

    start_time = events_by_id[group["fromId"]]["timestamp"]
    end_time = events_by_id[group["toId"]]["timestamp"]
    duration = end_time - start_time

    status = "Succeeded" if any([e["type"].endswith("Succeeded") for e in events]) else "Failed"

    step = {
        "stepName": step_name,
        "startTime": start_time.isoformat(),
        "endTime": end_time.isoformat(),
        "duration": str(duration),
        "status": status,
    }

    for e in events:
        if e["type"] == "TaskSubmitted":
            details = e["taskSubmittedEventDetails"]

            if details["resourceType"] == "states" and details["resource"].startswith(
                "startExecution"
            ):
                if "sfnExecutionInfo" in step.keys():
                    raise Exception("Step already has 'sfnExecutionInfo'")
                output = json.loads(details["output"])
                step["sfnExecutionInfo"] = extract_sfn_execution_info(output["ExecutionArn"])

            if details["resourceType"] == "elasticmapreduce" and details["resource"].startswith(
                "createCluster"
            ):
                if "emrClusterInfo" in step.keys():
                    raise Exception("Step already has 'emrClusterInfo'")
                output = json.loads(details["output"])
                step["emrClusterInfo"] = extract_emr_cluster_info(output)

    return step


def extract_emr_cluster_info(output):
    cluster_id = output["ClusterId"]
    cluster_arn = output["ClusterArn"]
    region = extract_region_from_arn(cluster_arn)
    cluster_link = f"https://{region}.console.aws.amazon.com/elasticmapreduce/home?region={region}#cluster-details:{cluster_id}"

    info = get_emr_cluster_info(cluster_id)
    steps = sorted(
        [extract_emr_step_info(s, info) for s in get_emr_cluster_steps(cluster_id)],
        key=lambda s: s["startTime"],
    )

    return {
        "emrClusterId": cluster_id,
        "emrClusterArn": cluster_arn,
        "emrClusterName": info["emrClusterName"],
        "emrClusterLink": cluster_link,
        "status": info["status"],
        "steps": steps,
    }


def extract_emr_step_info(step, cluster_info):
    timeline = step["Status"]["Timeline"]

    info = {
        "stepId": step["Id"],
        "stepName": step["Name"],
        "createTime": timeline["CreationDateTime"].isoformat(),
        "startTime": timeline["StartDateTime"].isoformat(),
        "endTime": timeline["EndDateTime"].isoformat(),
        "duration": str(timeline["EndDateTime"] - timeline["StartDateTime"]),
        "status": step["Status"]["State"],
    }

    if "FailureDetails" in step["Status"]:
        failure = step["Status"]["FailureDetails"]
        info["error"] = failure.get("Reason") or failure.get("Message")
        info["logPath"] = failure["LogFile"].replace("stderr.gz", "")  # File name is not always included
        download_logs(info["logPath"] + "stderr.gz")

    return info
