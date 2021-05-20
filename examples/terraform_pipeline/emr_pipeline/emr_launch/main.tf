terraform {
  backend "local" {}
  required_version = "~> 0.12"
}

provider "aws" {
  region = var.aws_region
}

data "archive_file" "infrastructure" {
  type        = "zip"
  source_dir = "${path.module}/infrastructure"
  output_path = "${path.module}/data/infrastructure.zip"
}

data "archive_file" "utils" {
  type        = "zip"
  source_dir = "${path.module}/utils"
  output_path = "${path.module}/data/utils.zip"
}

resource "null_resource" "launch_function" {

  triggers = {
    always_run = timestamp()
  }

  provisioner "local-exec" {
    command = "bash ${path.module}/utils/cdk_deploy.sh"
    on_failure = fail
    environment = {
      CDK_DEPLOY_ACCOUNT = data.aws_caller_identity.current.account_id
      CDK_DEPLOY_REGION = var.aws_region
      DEPLOYMENT_STAGE = var.stage

      VPC_ID = var.vpc_id
      SUBNET_ID = var.subnet_id
      CLUSTER_NAME = var.emr_cluster_name
      MASTER_INSTANCE_TYPE = var.master_instance_type
      CORE_INSTANCE_TYPE = var.core_instance_type
      CORE_INSTANCE_COUNT = var.core_instance_count
      CORE_INSTANCE_MARKET = var.core_instance_market
      RELEASE_LABEL = var.release_label
      LOG_BUCKET = var.log_bucket
      ARTIFACT_BUCKET = var.artifact_bucket
      INPUT_BUCKETS = join(",", var.input_buckets)
      OUTPUT_BUCKETS = join(",", var.output_buckets)
      INPUT_KMS_ARNS = join(",", var.input_kms_arns)
      OUTPUT_KMS_ARNS = join(",", var.output_kms_arns)
      APPLICATIONS = join(",", var.applications)
      CONFIGURATION = jsonencode(var.configuration)
      TASK_INSTANCE_TYPE = var.task_instance_type
      TASK_INSTANCE_MARKET = var.task_instance_market
      TASK_INSTANCE_COUNT = var.task_instance_count
      CORE_INSTANCE_EBS_SIZE = var.core_instance_ebs_size
      CORE_INSTANCE_EBS_TYPE = var.core_instance_ebs_type
      CORE_INSTANCE_EBS_IOPS = var.core_instance_ebs_iops
      TASK_INSTANCE_EBS_SIZE = var.task_instance_ebs_size
    }
  }
}

resource "null_resource" "destroy_launch_function" {

  provisioner "local-exec" {
    command = "bash ${path.module}/utils/cdk_destroy.sh"
    on_failure = fail
    when = destroy
    environment = {
      STACK_NAME = "${var.emr_cluster_name}-stack"
      CDK_DEPLOY_REGION = var.aws_region
    }
  }
}

locals {
  parserscript = "${path.module}/utils/parse-outputs.py"
  input = "${path.module}/data/my-outputs.json"
}

data "local_file" "cdk-outputs" {
    filename = local.input
    depends_on = [null_resource.launch_function]
}

locals {
  launch_arn_string = jsondecode(data.local_file.cdk-outputs.content)["${var.emr_cluster_name}-stack"]["LaunchFunctionARN"]
  instance_role_string = jsondecode(data.local_file.cdk-outputs.content)["${var.emr_cluster_name}-stack"]["InstanceRoleName"]
}
