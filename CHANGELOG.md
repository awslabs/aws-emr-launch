Changelog for emr-launch
=============================

1.5.0 (unreleased)
------------------

- support CDK >= 1.46.0


1.4.3 (2020-10-05)
------------------

- NEW: instance_fleet_configuration.ManagedScalingConfiguration

- FIX: handle NoneType for some InstanceGroups configs


1.4.2 (2020-09-28)
------------------

- FIX: bug in handling of allowed_cluster_configuration_overrides

- default allowed_cluster_confgiuration_overrides to cluster_configuration's default

- added instance_group_configuration.ManagedScalingConfiguration to managed_configurations

- remove incorrect Optional hint on EMRProfile.profile_name


1.4.1 (2020-08-26)
------------------

- NEW: Added support for ManagedScalingPolicy cluster parameter (thanks to mbadwalgithub)


1.4.0 (2020-08-12)
------------------

- NEW: requirements.txt for specific versions

- FIX: cdk compatibility >= 1.36.0


1.3.2 (2020-07-29)
------------------

- FIX: use hash for SparkJars Construct id

- FIX: use wildcard when authorizing Secrets


1.3.1 (2020-06-25)
------------------

- FIX: author and url in setup.py


1.3.0 (2020-06-05)
------------------

- NEW: add deployment:product tags to emr_launch constructs

- NEW: switch ebs volumes to st1 on managed_clusters

- NEW: enable dict for EMRLaunchFunction Tags

- FIX: typing hint for EMRLaunchFunction allowed_overrides

- exclude tests from bdist

- change software license to Apache 2.0


1.2.1 (2020-05-05)
------------------

- FIX: FIRE_AND_FORGET cluster creation with secrets failed


1.2.0 (2020-05-05)
------------------

- NEW: enable AddStep tasks to "fire and forget" Steps

- NEW: enable LaunchFunction to "fire and forget" cluster start

- FIX: include namespace in Role names for uniqueness

- FIX: authorize output buckets for read_write


1.1.0 (2020-04-21)
------------------

- update package requirements to new package naming convention

- FIX: ClusterArgOverrides -> StepArgOverrides in Lambda

- added PermissionBoundaryAspect


1.0.0 (2020-03-23)
------------------

- Enable overriding Step Args at runtime with new Chain

- Replace cdk StartExecution Task to enable dynamic Input

- Replace cdk AddStep task to enable Args to be passed as a Parameter

- Helper function for LakeFormation enablement

- Helper functions for Kerberos: local kdc, local with cross-realm, external, and external with cross-realm

- Enable EMRFS Role Mappings for prefixes, users, and groups

- BREAKING: add (and enforce) Default, Minimum, and Maximum to cluster_configuration.override_interfaces

- BREAKING: new managed_configurations for cluster_configurations

- cleaner Task/Step integration

- Avoid exposing secure_configurations when launching clusters

- Secret cluster configurations loaded from SecretsManager

- Helper functions to add spark packages and jars

- implement EMR/Step Functions integrations

- Correctly handle immutable instance_profiles


0.5.2 (2020-02-10)
------------------

- public cluster_configuration.update_configurations()

- public cluster_configuration.update_config()

- implement description on InstanceGroupConfiguration

- Use Intelligent-Tiering to manage Advanced SSM Parameters


0.5.1 (2020-02-04)
------------------

- replace relative import paths

- use a LoadClusterConfig lambda per launch_function

- update boto3 and cdk versions

- add default override_interface to InstanceGroupConfiguration

- add override_interfaces to cluster_configuration


0.5.0 (2020-01-10)
------------------

- Tags as core.Tag instead of dict

- InstanceMarketType enum

- more examples!

- BREAKING: move Tags from cluster_configuration to launch_function

- BREAKING: decouple emr_profile and cluster_configuration

- rehydrate EMRLaunchFunction from stored function

- TerminateCluster task waits for actual termination

- BREAKING: wrap TaskToken message in JSON structure

- BREAKING: refactor ClusterConfiguration.profile_components -> ClusterConfiguration.emr_profile


0.4.1 (2020-01-03)
------------------

- enable updating of cluster tags at runtime

- FIX: include namespace when rehydrating saved profiles and configs

- added TerminateCluster task

- add Get/List APIs to control_plane

- add desriptions to emr_profiles, cluster_configurations, and emr_launch_functions

- Fail if supplied cluster_configuration override is invalid

- add library version to control_plane Lambdas

- simplify EmrStep.properties dict


0.4.0 (2019-12-19)
------------------

- refactor FailIfJobRunning, fail_if_job_running -> FailIfClusterRunning, fail_if_cluster_running

- correctly update and store cluster_configuration

- add boto3 to shared lambda layer

- reusable EMRConfigUtilsLayer

- refactor cluster_configuration.cluster_name -> configuration_name

- updated example stacks

- improved module imports

- added EmrCode, EmrStep, and EmrBootstrap constructs

- refactored stepfunction chains and tasks

- add_steps cloudwatch rule and lambda


0.3.0 (2019-12-12)
------------------

- get individual/all profiles, conifurations, and launch_functions

- cleaned up stored profiles, configurations, and functions

- refactor examples

- add namespace to profiles, configs, functions

- single emr_utilities lambdas and layers per stack

- reduce control_plane footprint


0.2.1 (2019-11-26)
------------------

- Refactor launch_config -> launch_function

- enabled StepConcurrencyLevel

- enabled AllowedClusterConfigOverrides


0.2.0 (2019-11-14)
------------------

- refactor control_plane into .wheel

- Lock releases to setup.py


0.1.2 (2019-11-13)
------------------

- state machine fragments

- Named Launch Configs (optional)

- Cleaned Step Function execution chain


0.1.1 (2019-11-01)
------------------

- Store EMR Cluster Configs

- Store and rehydrate EMR Profiles

- EMR state change CloudWatch Event Rules

- removed EMR API polling

- InstanceGroup launch lambda

- cluster_configuration tests

- emr lambda utils

- add cluster configuration construct

- refactor and rename components


0.1.0 (2019-10-01)
------------------
- emr_components.transient: Security Groups

- emr_components.transient: Roles

- emr_components.transient: Topics

- emr_components.transient: Security Configuration

- iam_roles.emr: Standard Roles

- security_groups.emr: Standard Groups

- lambdas.emr: Add Job Flow

- lambdas.emr: Check Status

- lambdas.emr: Add Step
