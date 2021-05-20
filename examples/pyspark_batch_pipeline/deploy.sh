#!/bin/bash

cmd=$1
build=$2

python3 -m venv .env
source .env/bin/activate
pip install -r requirements.txt | grep -v 'already satisfied'

cdk $cmd --all