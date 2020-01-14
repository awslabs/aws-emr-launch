# EMR Launch Examples

## Deploying the examples
The examples require an environment with the following (the Buckets can be seperate or the same):

1. A VPC with at least one Private Subnet
2. An S3 Bucket used for EMR Artifacts (Bootstrap scripts, Step scripts, etc)
3. An S3 Bucket used for EMR Logs
4. An S3 Bucket used for EMR input/output data

Before deploying the examples, set up the environment variables:
```sh
export EMR_LAUNCH_EXAMPLES_VPC="YOUR VPC_ID"
export EMR_LAUNCH_EXAMPLES_ARTIFACTS_BUCKET="YOUR ARTIFACTS BUCKET_NAME"
export EMR_LAUNCH_EXAMPLES_LOGS_BUCKET="YOUR LOGS BUCKET_NAME"
export EMR_LAUNCH_EXAMPLES_DATA_BUCKET="YOUR DATA BUCKET_NAME"
```

Create and activate a virtualenv for the examples:
```sh
cd examples/
python3 -m venv .env
source .env/bin/activate
```

Install the `aws-emr-launch` library and dependencies:
```sh
pip install ..
```

Deploy examples in the following order:
1. `control_plane`
2. `emr_profiles`
3. `cluster_configurations`
4. `emr_launch_functions`
5. `transient_cluster_pipeline`
6. `persistent_cluster_pipeline`
7. `sns_triggered_pipeline`

Deploy the `control_plane` (this only needs to be done once or after updates to the `control_plane`):
```sh
cd control_plane/
cdk deploy
```

Deploy an example:
```sh
cd emr_profiles/
cdk deploy
