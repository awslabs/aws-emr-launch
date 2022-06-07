#!/usr/bin/env python3

import os

import aws_cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager

from aws_emr_launch.constructs.emr_constructs import emr_code
from aws_emr_launch.constructs.managed_configurations import instance_group_configuration

NAMING_PREFIX = f"emr-launch-{aws_cdk.Aws.ACCOUNT_ID}-{aws_cdk.Aws.REGION}"

app = aws_cdk.App()
stack = aws_cdk.Stack(
    app,
    "ClusterConfigurationsStack",
    env=aws_cdk.Environment(account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"]),
)

vpc = ec2.Vpc.from_lookup(stack, "Vpc", vpc_name="EmrLaunchExamplesEnvStack/EmrLaunchVpc")
artifacts_bucket = s3.Bucket.from_bucket_name(stack, "ArtifactsBucket", f"{NAMING_PREFIX}-artifacts")

# This prepares the project's bootstrap_source/ folder for deployment
# We use the Artifacts bucket configured and authorized on the EMR Profile
bootstrap_code = emr_code.Code.from_path(
    path="./bootstrap_source",
    deployment_bucket=artifacts_bucket,
    deployment_prefix="emr_launch_testing/bootstrap_source",
)

# Define a Bootstrap Action using the bootstrap_source/ folder's deployment location
bootstrap = emr_code.EMRBootstrapAction(
    name="bootstrap-1", path=f"{bootstrap_code.s3_path}/test_bootstrap.sh", args=["Arg1", "Arg2"], code=bootstrap_code
)


# Cluster Configurations that use InstanceGroups are deployed to a Private subnet
subnet = vpc.private_subnets[0]

# Load a SecretsManger Secret with secure RDS Metastore credentials
secret_name = f"{NAMING_PREFIX}-external-metastore"
secret = secretsmanager.Secret.from_secret_complete_arn(
    stack,
    "Secret",
    f"arn:{aws_cdk.Aws.PARTITION}:secretsmanager:{aws_cdk.Aws.REGION}:{aws_cdk.Aws.ACCOUNT_ID}:secret:{secret_name}",
)


# Create a basic Cluster Configuration using InstanceGroups, the Subnet and Bootstrap
# Action defined above, the EMR Profile we loaded, and defaults defined in
# the InstanceGroupConfiguration
basic_cluster_config = instance_group_configuration.ManagedScalingConfiguration(
    stack,
    "BasicClusterConfiguration",
    configuration_name="basic-instance-group-cluster",
    subnet=subnet,
    bootstrap_actions=[bootstrap],
    step_concurrency_level=2,
    secret_configurations={"hive-site": secret},
)

basic_cluster_config.add_spark_package("com.amazon.deequ:deequ:1.0.2")

basic_cluster_config.add_spark_jars(
    emr_code.Code.from_path(
        path="./jars", deployment_bucket=artifacts_bucket, deployment_prefix="emr_launch_testing/jars"
    ),
    emr_code.Code.files_in_path("./jars", "*.jar"),
)


# Here we create another Cluster Configuration using the same subnet, bootstrap, and
# EMR Profile while customizing the default Instance Type and Instance Count
high_mem_cluster_config = instance_group_configuration.InstanceGroupConfiguration(
    stack,
    "HighMemClusterConfiguration",
    configuration_name="high-mem-instance-group-cluster",
    subnet=subnet,
    bootstrap_actions=[bootstrap],
    step_concurrency_level=5,
    core_instance_type="r5.2xlarge",
    core_instance_count=2,
)

app.synth()
