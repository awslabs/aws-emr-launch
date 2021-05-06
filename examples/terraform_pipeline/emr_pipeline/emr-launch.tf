
#####################################################################################
# EMR Cluster Configuration - resources, security, permissions
#####################################################################################

module "launch_function" {

  source = "./emr_launch"
  emr_cluster_name = var.emr_cluster_name
  master_instance_type = var.master_instance_type
  core_instance_type = var.core_instance_type
  core_instance_count = var.core_instance_count
  release_label = var.release_label
  applications = var.applications
  configuration = var.configuration

  task_instance_type = var.task_instance_type
  task_instance_market = var.task_instance_market
  task_instance_count = var.task_instance_count

  stage = var.environment
  aws_region = var.aws_region

  vpc_id = var.vpc_id
  subnet_id = var.subnet_id

  log_bucket = aws_s3_bucket.log_bucket.arn
  artifact_bucket = aws_s3_bucket.artifact_bucket.arn

  // Any bucket from which EMR must read data - goes here:
  input_buckets = [aws_s3_bucket.demo_bucket.arn]

  // Any bucket to which EMR must write data - goes here:
  output_buckets = [aws_s3_bucket.demo_bucket.arn]

  // Any KMS key needed for decrypting data on-read goes here
  input_kms_arns = []

  // Any KMS key neededfor encrypting data on-write goes here
  output_kms_arns = []
}

// Pass EMR Launch Function ARN to Orchestration Pipeline via Locals
locals {
  cdk_output_file = module.launch_function.cdk_outputs_file
  launch_function_arn = module.launch_function.launch_function_arn
  instance_role_name = module.launch_function.instance_role_name
}

// Print Launch Function Outputs
resource "null_resource" "print_my_outputs" {
  depends_on = [module.launch_function]
  provisioner "local-exec" {
    command = "terraform output"
  }
}
