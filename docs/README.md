# EMR Launch Docs
The intent of the EMR Launch library is to simplify the definition, deployment, management, and use of EMR Clusters for developers by:

- defining reusable Security, Resource, and Launch Configurations enabling developers to __Define Once and Reuse__ 
- separating the reusable definition of Cluster Security Configuration and Cluster Resource Configuration enabling these to be manage by the proper responsible parties
- providing a suite of Tools to simplify the construction of Orchestration Pipelines using Step Functions and EMR Clusters


## Concepts (and Constructs)
This library utilizes the AWS CDK for deployement and management of resources. It is recommended that users familiarize themselves with the CDK's basic concepts and usage.


### EMR Profile
An EMR Profile (`emr_profile`) is a reusable definition of the security profile used by an EMR Cluster. This includes:
- __Service Role__: an IAM Role used by the EMR Service to manage the Cluster
- __Instance Role__: an IAM Role used by the EC2 Instances in an EMR Cluster
- __AutoScaling Role__: an IAM Role used to autoscale and resize an EMR Cluster
- __Service Group__: a Security Group granting the EMR Service basic access to EC2 Instances in Cluster. This is required to deploy Instances into a Private Subnet.
- __Master Group__: the Security Group assigned to the EMR Master Instance
- __Workers Group__: the Security Group assigned to the EMR Worker Instances (Core and Task nodes)
- __Security Configuration__: the Security Configuration used by the Cluster 

Each `emr_profile` requires a unique `profile_name`. This name and the `namespace` uniquiely identify a profile. The `namespace` is a logical grouping of profiles and has a default value of "default". 

Deploying an `emr_profile` creates these resources and stores the profile definition and metadata in the Parameter Store. The Profile either be used immediately in the Stack when it is defined, or reused in other Stacks by loading the Profile definition by `profile_name` and `namespace`.

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

The `emr_launch_function` is a mechanism for easily combining the resuable `emr_profile` and `cluster_configuration`.

### Chains and Tasks
Chains and Tasks are preconfigured components that simplify the use of AWS Step Function State Machines as orchestrators of data processing pipelines. These components allow the developer to easily build complex, serverless pipelines using EMR Clusters (both Transient and Persistent), Lambdas, and nested State Machines.

### Security
Care is taken to ensure that `emr_launch_functions` and `emr_profiles` can't be used to create clusters with elevated or unintended privileges. 

- IAM policies can be used to restrict the Users and Roles that can create EMR Clusters with specific `emr_profiles` and `cluster_configurations` by granting `states:StartExecution` to specific State Machine ARNs.
- By storing the metadata and configuration of `emr_profiles`, `cluster_configurations`, and `emr_launch_functions` in the Systems Manager Parameter Store, IAM Policies can be used to grant or restrict Read/Write access to these
    + Access can be managed for all metadata and configurations, specific __nameespaces__, or individual ARNs
- 