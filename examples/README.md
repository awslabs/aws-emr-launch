# EMR Launch Examples

## Deploying the examples
The examples require an environment with the following (the Buckets can be seperate or the same):

1. A VPC with at least one Private Subnet
2. An S3 Bucket used for EMR Artifacts (Bootstrap scripts, Step scripts, etc)
3. An S3 Bucket used for EMR Logs
4. An S3 Bucket used for EMR input/output data
5. A SecretsManager Secret with a Kerberos Attributes example
6. A SecretsManager Secret with a secret Cluster Configuration example

Before deploying the examples, set up the environment variables:
```bash
export EMR_LAUNCH_EXAMPLES_VPC="YOUR VPC_ID"
export EMR_LAUNCH_EXAMPLES_ARTIFACTS_BUCKET="YOUR ARTIFACTS BUCKET_NAME"
export EMR_LAUNCH_EXAMPLES_LOGS_BUCKET="YOUR LOGS BUCKET_NAME"
export EMR_LAUNCH_EXAMPLES_DATA_BUCKET="YOUR DATA BUCKET_NAME"
export EMR_LAUNCH_EXAMPLES_KERBEROS_ATTRIBUTES_SECRET="YOUR_SECRET_ARN"
export EMR_LAUNCH_EXAMPLES_SECRET_CONFIGS="YOUR_OTHER_SECRET_ARN"
```

The Lambda Layer packages are required to deploy the examples. If these haven't been installed
see the **Development** section of the top-level README.md.

Create and activate a virtualenv for the examples:
```bash
cd examples/
python3 -m venv .env
source .env/bin/activate
```

Install the `aws-emr-launch` library and dependencies:
```bash
pip install -e ..
```

Deploy examples in the following order:
1. `control_plane`
2. `environment_stack`
3. `emr_profiles`
4. `cluster_configurations`
5. `emr_launch_functions`
6. `transient_cluster_pipeline`
7. `persistent_cluster_pipeline`
8. `sns_triggered_pipeline`

Deploy the `control_plane` (this only needs to be done once or after updates to the `control_plane`):
```bash
cd control_plane/
cdk deploy
```

Deploy an example:
```bash
cd emr_profiles/
cdk deploy
