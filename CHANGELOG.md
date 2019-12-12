Changelog for emr-launch
=============================

0.3.0 (unreleased)
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
