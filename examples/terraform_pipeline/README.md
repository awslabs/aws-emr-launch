### Getting Started
This repository contains a fully integrated Terraform module for the EMR Launch framework. It still relies
on aws's open source emr-launch library under the hood, but allows users who want to continue using Terraform for the
bulk of their infrastructure, to realize the benefits of aws-emr-launch [https://github.com/awslabs/aws-emr-launch]
    
The Launch Function is responsible for provisioning EMR clusters and managing security, resource, and application configurations. 
Any user or service who needs an EMR cluster can trigger this launch step function and receive the cluster id when it is ready.

An example pipeline is provided for running a simple spark application with this EMR Launch framework.

See the emr_pipeline/emr_launch/infrastructure/README.md for more details on cluster configurations

#### First time setup for Terraform (module tested with v0.12.29)
   1. Download and install Terraform v0.12.29
        https://releases.hashicorp.com/terraform/0.12.29/
    
   2. Also helpful for managing different versions of terraform is tfswitch for OSX
        https://github.com/warrensbox/terraform-switcher
        brew install warrensbox/tap/tfswitch
    
   3. For more information on getting started with Terraform, read the official docs here:
        https://www.terraform.io/intro/index.html
        
#### First time set up for cdk 
   1. Install Node Package Manager - https://www.npmjs.com/get-npm
   
   2. Install CDK per machine / client
        
            npm install -g aws-cdk

### Deploy a demo pipeline, test, and clean up
    
   1. Add an environment to environments/<environment-name>/<region>.tfvars Or update the dev environment by adding your own vpc and subnet id's
    
   2. Run the following commands:
   
    Command syntax
        bash bin/deploy.sh <terraform-command> <environment-name> <region>
    
    # Plan
        bash bin/deploy.sh plan dev eu-west-1
    
    # Deploy
        bash bin/deploy.sh apply dev eu-west-1
    
    # Test: 
    Once deployed, trigger the deployed pipeline Step Function to test end-to-end or 
        trigger just the deployed Launch function to spin up a cluster
    
    # Clean up
        bash bin/deploy.sh destroy dev eu-west-1
    
    
#### Add your own spark application code:
    - Option 1: Add your spark code to spark_script.py, and deploy the pipeline as-is
    - Option 2: Replace the variable spark_script with the name of your pyspark script and deploy the pipeline
    - To edit spark settings like shuffle.partitions or executor.cores, add these directly to the spark-submit command:
        - Example - LINE76: emr_pipeline/emr_step_function/pipeline.json
            "--conf", "spark.yarn.maxAppAttempts=1",
            