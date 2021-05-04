#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cmd=$1
stage=$2
region=$3

export aws_account_id=$(aws sts get-caller-identity --query Account --output text)

cd $DIR
cd ..

python3 -m venv .env
source .env/bin/activate

cd $DIR
pip install -r ../emr_pipeline/emr_launch/infrastructure/requirements.txt
cdk bootstrap aws://$aws_account_id/$region

cd ../emr_pipeline
terraform init
terraform $cmd -var-file=../environments/$stage/$region.tfvars
