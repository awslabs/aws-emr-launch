#!/usr/bin/env python3

from aws_cdk import (
    aws_ec2 as ec2,
    aws_kms as kms,
    aws_s3 as s3,
    core
)

from aws_emr_launch.constructs.emr_constructs import EMRProfileComponents, InstanceGroupConfiguration
from control_plane.constructs.lambdas import EMRUtilitiesStack


app = core.App()
stack = core.Stack(app, 'test-stack')
vpc = ec2.Vpc(stack, 'test-vpc')
artifacts_bucket = s3.Bucket(stack, 'test-artifacts-bucket')
logs_bucket = s3.Bucket(stack, 'test-logs-bucket')
input_bucket = s3.Bucket(stack, 'test-input-bucket')
output_bucket = s3.Bucket(stack, 'test-output-bucket')
input_key = kms.Key(stack, 'test-input-key')
s3_key = kms.Key(stack, 'test-s3-key')
local_disk_key = kms.Key(stack, 'test-local-disk-key')

emr_components = EMRProfileComponents(
    stack, 'test-emr-components',
    profile_name='TestCluster', environment='test',
    vpc=vpc, artifacts_bucket=artifacts_bucket, logs_bucket=logs_bucket)

emr_components \
    .authorize_input_buckets([input_bucket]) \
    .authorize_output_buckets([output_bucket]) \
    .authorize_input_keys([input_key]) \
    .set_s3_encryption('SSE-KMS', s3_key) \
    .set_local_disk_encryption_key(local_disk_key, ebs_encryption=True)

cluster_config = InstanceGroupConfiguration(
    stack, 'test-instance-group-config',
    cluster_name='test-cluster', profile_components=emr_components)

emr_lambdas_stack = EMRUtilitiesStack(app, 'test-lambdas-stack')
# step_functions_stack = LaunchEMRConfigStack(
#     app,
#     'test-step-functions-stack',
#     run_job_flow_lambda=emr_lambdas_stack.run_job_flow,
#     check_step_status_lambda=emr_lambdas_stack.check_step_status)

app.synth()
