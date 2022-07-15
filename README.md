# EMR Launch

> An [AWS Professional Service](https://aws.amazon.com/professional-services/) open source initiative | aws-proserve-opensource@amazon.com

![Python Version](https://img.shields.io/badge/python-3.6%20%7C%203.7%20%7C%203.8%20%7C%203.9-brightgreen.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

[![Coverage](https://img.shields.io/badge/coverage-86%25-brightgreen.svg)](https://pypi.org/project/awswrangler/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

The intent of the EMR Launch library is to simplify the development experience for Builders defining, deploying, managing, and using EMR Clusters by:

- defining reusable Security, Resource, and Launch Configurations enabling developers to __Define Once and Reuse__
- separating the definition of Cluster Security Configurations and Cluster Resource Configurations into reusable and shareable Constructs
- providing a suite of Tools to simplify the construction of Orchestration Pipelines using Step Functions and EMR Clusters

## Concepts (and Constructs)

This library utilizes the AWS CDK for deployment and management of resources. It is recommended that users familiarize themselves with the CDK's basic concepts and usage.

### EMR Profile

An EMR Profile (`emr_profile`) is a reusable definition of the security profile used by an EMR Cluster. This includes:

- __Service Role__: an IAM Role used by the EMR Service to manage the Cluster
- __Instance Role__: an IAM Role used by the EC2 Instances in an EMR Cluster
- __AutoScaling Role__: an IAM Role used to autoscale and resize an EMR Cluster
- __Service Group__: a Security Group granting the EMR Service basic access to EC2 Instances in Cluster. This is required to deploy Instances into a Private Subnet.
- __Master Group__: the Security Group assigned to the EMR Master Instance
- __Workers Group__: the Security Group assigned to the EMR Worker Instances (Core and Task nodes)
- __Security Configuration__: the Security Configuration used by the Cluster
- __Kerberos Attributes__: the attributes required to enable Kerberos authentication

Each `emr_profile` requires a unique `profile_name`. This name and the `namespace` uniquely identify a profile. The `namespace` is a logical grouping of profiles and has a default value of "default".

Deploying an `emr_profile` creates these resources and stores the profile definition and metadata in the Parameter Store. The Profile can either be used immediately in the Stack when it is defined, or reused in other Stacks by loading the Profile definition by `profile_name` and `namespace`.

### Cluster Configuration

A Cluster Configuration (`cluster_configuration`) is a reusable definition of the physical resources in an EMR Cluster. This incudes:

- __EMR Release Label__: the EMR release version (e.g. `emr-5.28.0`)
- __Applications__: the Applications to install on Cluster (e.g. Hadoop, Hive, SparK)
- __Bootstrap Actions__: the Bootstrap Actions to execute on each node after Applications have been installed
- __Configurations__: configuration parameters to set for the various Applications installed
- __Step Concurrency Level__: the number of concurrent Steps the Cluster is configured to run
- __Instances__: the configuration of the Master, Core, and Task nodes in the Cluster (e.g. Master Instance Type, Core Instance Type, Core Instance Count, etc)

Like the `emr_profile`, each `cluster_configuration` requires a unique `configuration_name`. This name and the `namespace` uniquely identify a configuration.

Deploying a `cluster_configuration` stores the configuration definition and metadata in the Parameter Store. The Configuration can either be used immediately in the Stack when it is defined, or reused in other Stacks by loading the Configuration definition by `configuration_name` and `namespace`.

### EMR Launch Function

An EMR Launch Function (`emr_launch_function`) is an AWS Step Functions State Machine that launches an EMR Cluster. The Launch Function is defined with an `emr_profile`, `cluster_configuration`, `cluster_name`, and `tags`. When the function is executed it creates an EMR Cluster with the given name, tags, security profile, and physical resources then synchronously monitors the cluster for successful start.

To be clear, deploying an `emr_launch_function` __does not__ create an EMR Cluster, it only creates the State Machine. The cluster is created when the State Machine is executed.

The `emr_launch_function` is a mechanism for easily combining the reusable `emr_profile` and `cluster_configuration`.

Like the `emr_profile` and `cluster_configuration`, each `emr_launch_function` requires a unique `launch_function_name`. This name and the `namespace` uniquely identify the launch function.

### Chains and Tasks

Chains and Tasks are preconfigured components that simplify the use of AWS Step Function State Machines as orchestrators of data processing pipelines. These components allow the developer to easily build complex, serverless pipelines using EMR Clusters (both Transient and Persistent), Lambdas, and nested State Machines.

### Security

Care is taken to ensure that `emr_launch_functions` and `emr_profiles` can't be used to create clusters with elevated or unintended privileges.

- IAM policies can be used to restrict the Users and Roles that can create EMR Clusters by granting `states:StartExecution` to specific State Machine ARNs.
- By storing the metadata and configuration of `emr_profiles`, `cluster_configurations`, and `emr_launch_functions` in the Systems Manager Parameter Store, IAM Policies can be used to grant or restrict Read/Write access to these
  - Access can be managed for *__ALL__* metadata and configurations, specific __namespaces__, or individual ARNs
- Each `emr_launch_function` uses a specific AWS Lambda function to load and combine its specific `emr_profile` and `cluster_configuration`. The IAM Policy associated with this Lambda allows it to read only these specific ARNs from the Parameter Store.
- Each `emr_launch_function` is granted `iam:PassRole` to the specific EMR Roles defined in the `emr_profile` assigned to the launch function. Attempting to change the Roles used by directly modifying the metadata of the `emr_profile` in the Parameter Store will result in a cluster launch failure.

## Usage

This library acts as a plugin to the [AWS CDK](https://aws.amazon.com/cdk/) providing additional L2 Constructs.
To avoid circular references with CDK dependencies this package will not install CDK and Boto3. These should be
installed manually from one of the `requirements.txt` files (depending on the version of `aws-emr-launch`).

It is recommended that a Python3 `venv` be used for all CDK builds and deployments.

To get up and running quickly:

### Prerequisites

The AWS CDK v2.x utilizes containers to automate some tasks. EMR Launch uses and deploys a CDK `PythonLayerVersion`, this Construct uses a container to create the bundle for the Lambda Layer. As such, a `docker` runtime is required to deploy.

### Deployment

1. Install the [CDK CLI](https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html)

   ```bash
   npm install -g aws-cdk
   ```

2. Use your mechanism of choice to create and activate a Python3 `venv`:

   ```bash
   python3 -m venv .env
   source .env/bin/activate
   ```

3. Install the CDK and Boto3 minimum requirements:

   ```bash
   pip install -r requirements-2.x.txt
   ```

4. Install `aws-emr-launch` package:

   ```bash
   pip install aws-emr-launch
   ```

## Development

Follow Steps 1 - 3 above to configure an environment and install requirements

After activating your `venv`:

1. Install development requirements:

   ```bash
   pip install -r requirements-dev.txt
   ```

2. Install the library locally:

   ```bash
   pip install -e .
   ```

### Managing Layer Packages

Update the `aws_emr_launch/lambda_sources/layers/emr_config_utils/requirements.txt` adding/updating/removing package(s)

### Testing

To run the test suite (from within the `venv`):

```bash
pytest
```

#### After running tests

View test coverage reports by opening `htmlcov/index.html` in your web browser.

#### To write a test

- start a file named test_[the module you want to test].py
- import the module you want to test at the top of the file
- write test case functions that match either `test_*` or `*_test`

For more information refer to [pytest docs](https://docs.pytest.org/en/latest/getting-started.html)

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the terms of the Apache 2.0 license. See `LICENSE`.
Included AWS Lambda functions are licensed under the MIT-0 license. See `LICENSE-LAMBDA`.
