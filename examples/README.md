# EMR Launch Examples

## Exmaples
The examples require an environment with the following (the Buckets can be seperate or the same):

1. A VPC with at least one Private Subnet
2. An S3 Bucket used for EMR Artifacts (Bootstrap scripts, Step scripts, etc)
3. An S3 Bucket used for EMR Logs
4. An S3 Bucket used for EMR input/output data
5. A SecretsManager Secret with a Kerberos Attributes example
6. A SecretsManager Secret with a secret Cluster Configuration example

To get up and running quickly the `environment_stack` will deploy these into your account. The resources
deployed by this stack are then used in the other examples.

### Lambda Layer packages
The Lambda Layer packages are required to deploy the examples. If these haven't been installed
see the **Development** section of the top-level README.md.

### Deploying the Examples
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

You can use the `deploy_all.sh` script to deploy all the example projects. Or deploy manually in 
in the following order:
1. `control_plane`
2. `environment_stack`
3. `emr_profiles`
4. `cluster_configurations`
5. `emr_launch_functions`
6. `transient_cluster_pipeline`
7. `persistent_cluster_pipeline`
8. `sns_triggered_pipeline`

Deployment of the `control_plane` is optional. It provides some Lambda functions you can use
to investigate the environment.

To deploy the `control_plane`:
```bash
cd control_plane/
cdk deploy
```

Deployment of the `environment_stack` only needs to be done once to prepare the resources used
by the other examples.

To deploy the `envronment_stack`:
```bash
cd environment_stack/
cdk deploy
```

Each of the other examples is deployed in the same way:
1. `cd` into the directory
2. `cdk deploy` to deploy the resources

