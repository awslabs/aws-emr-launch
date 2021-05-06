Terraform wrapper around aws's open source emr-launch library
    * https://github.com/awslabs/aws-emr-launch *
    
    
All CDK related code lives in ./infrastructure including emr-launch usage

./utils contains three scripts for managing the integration between CDK and Terraform


Example Usage:

    module "launch_function" {
    
      source = "./emr_launch"
      emr_cluster_name = var.emr_cluster_name
      security_config = var.security_config // Must exist in ./infrastructure/emr_configs/security_configs/default.py
      resource_config = var.resource_config // Must exist in ./infrastructure/emr_configs/resource_configs/default.py
      launch_function_config = var.launch_function_config // Must exist in ./infrastructure/emr_configs/launch_step_functions/default.py
    
      stage = var.environment
      aws_region = var.aws_region
      vpc_id = var.vpc_id
      subnet_id = var.subnet_id
    
      log_bucket = aws_s3_bucket.demo_bucket.id
      artifact_bucket = aws_s3_bucket.demo_bucket.id
      input_buckets = [aws_s3_bucket.demo_bucket.id]
      output_buckets = [aws_s3_bucket.demo_bucket.id]
    }


