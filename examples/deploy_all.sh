#!/bin/bash
set -x

pushd ./control_plane && cdk deploy && popd
pushd ./environment_stack && cdk deploy && popd
pushd ./emr_profiles && cdk deploy && popd
pushd ./cluster_configurations && cdk deploy && popd
pushd ./emr_launch_function && cdk deploy && popd
pushd ./transient_cluster_pipeline && cdk deploy && popd
pushd ./persistent_cluster_pipeline && cdk deploy && popd
pushd ./sns_triggered_pipeline && cdk deploy && popd
