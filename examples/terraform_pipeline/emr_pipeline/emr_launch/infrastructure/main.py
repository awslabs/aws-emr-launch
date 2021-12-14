#!/usr/bin/env python3

import json
import logging
import os

from aws_cdk import core
from cluster_definition import EMRClusterDefinition

environment_variables = [
    "DEPLOYMENT_STAGE",
    "CDK_DEPLOY_ACCOUNT",
    "CDK_DEPLOY_REGION",
    "VPC_ID",
    "SUBNET_ID",
    "CLUSTER_NAME",
    "MASTER_INSTANCE_TYPE",
    "CORE_INSTANCE_TYPE",
    "CORE_INSTANCE_COUNT",
    "CORE_INSTANCE_MARKET",
    "CORE_INSTANCE_EBS_SIZE",
    "CORE_INSTANCE_EBS_TYPE",
    "CORE_INSTANCE_EBS_IOPS",
    "TASK_INSTANCE_EBS_SIZE",
    "TASK_INSTANCE_TYPE",
    "TASK_INSTANCE_COUNT",
    "TASK_INSTANCE_MARKET",
    "RELEASE_LABEL",
    "LOG_BUCKET",
    "ARTIFACT_BUCKET",
    "INPUT_BUCKETS",
    "OUTPUT_BUCKETS",
    "INPUT_KMS_ARNS",
    "OUTPUT_KMS_ARNS",
    "APPLICATIONS",
    "CONFIGURATION",
]

list_vars = [
    "INPUT_BUCKETS",
    "OUTPUT_BUCKETS",
    "INPUT_KMS_ARNS",
    "OUTPUT_KMS_ARNS",
    "APPLICATIONS",
]

int_vars = ["CORE_INSTANCE_COUNT", "TASK_INSTANCE_COUNT", "TASK_INSTANCE_EBS_SIZE", "CORE_INSTANCE_EBS_SIZE"]

json_vars = ["CONFIGURATION"]

app = core.App()

config = {"CLUSTER_NAME": app.node.try_get_context("cluster-name")}

for v in environment_variables:
    if v in list_vars:
        val = [x for x in os.environ[v].split(",") if x != ""]
        if len(val) > 0:
            config[v] = val
        else:
            config[v] = []
    elif v in int_vars:
        config[v] = int(os.environ[v])
    elif v in json_vars:
        config[v] = json.loads(os.environ[v])
    else:
        config[v] = os.environ[v]

if config["CORE_INSTANCE_EBS_SIZE"] > 0:
    assert (
        config["CORE_INSTANCE_EBS_IOPS"] <= config["CORE_INSTANCE_EBS_SIZE"] * 50
    ), "CORE_INSTANCE_EBS_IOPS must be <=  CORE_INSTANCE_EBS_SIZE (GB) * 50"

print(config)
logging.info(config)

env = core.Environment(
    account=config["CDK_DEPLOY_ACCOUNT"],
    region=config["CDK_DEPLOY_REGION"],
)

emr_cluster_stack = EMRClusterDefinition(app, id=config["CLUSTER_NAME"] + "-stack", env=env, config=config)

app.synth()
