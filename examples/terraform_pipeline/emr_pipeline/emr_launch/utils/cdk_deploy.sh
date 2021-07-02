#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd $DIR
cd ../
rm -r -f ./data
cd ./infrastructure
echo "Deploying emr launch functions with CDK"
export AWS_REGION=$CDK_DEPLOY_REGION
cdk deploy "*" --require-approval never --output ../data/cdk.out --outputs-file ../data/my-outputs.json \
  --context cluster-name=$CLUSTER_NAME


rm ./cdk.context.json
cd ..
rm -r ./data/cdk.out
