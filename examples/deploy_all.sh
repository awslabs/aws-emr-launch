#!/bin/bash
set -x

pushd ./control_plane && cdk deploy --require-approval never && popd
pushd ./environment_stack && cdk deploy --require-approval never && popd
pushd ./emr_profiles && cdk deploy --require-approval never && popd
pushd ./cluster_configurations && cdk deploy --require-approval never && popd
pushd ./emr_launch_function && cdk deploy --require-approval never && popd
pushd ./transient_cluster_pipeline && cdk deploy --require-approval never && popd
pushd ./persistent_cluster_pipeline && cdk deploy --require-approval never && popd
pushd ./sns_triggered_pipeline && cdk deploy --require-approval never && popd
