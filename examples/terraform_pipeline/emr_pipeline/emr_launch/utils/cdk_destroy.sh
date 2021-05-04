#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd $DIR
cd ../
rm -r -f ./data
echo "Destroying emr launch functions with aws cli delete stack"
export AWS_REGION=$CDK_DEPLOY_REGION
aws cloudformation delete-stack --stack-name $STACK_NAME
