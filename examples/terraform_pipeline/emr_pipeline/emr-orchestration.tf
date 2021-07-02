
// Define EMR Pipeline as Step Function - triggering EMR Launch ARN as first pipeline step
module "orchestration" {
  aws_region = var.aws_region
  stage = var.environment
  source = "./emr_step_function"
  emr-launch-arn = fileexists(module.launch_function.cdk_outputs_file) ? local.launch_function_arn : "fakearn"
  cluster-name = var.emr_cluster_name
  account_id = local.account_id
  output_s3_path = "s3://${aws_s3_bucket.demo_bucket.id}/output/"
  module_depends_on = [module.launch_function.launch_function_arn, null_resource.print_my_outputs]
  sfn_orchestrate_name = "${var.emr_cluster_name}-sfn-pipeline"
  sfn_orchestrate_definition_file = "${path.module}/emr_step_function/pipeline.json"
  artifacts_bucket = aws_s3_bucket.artifact_bucket.id
  artifact_path = aws_s3_bucket_object.emr_pyspark_script.key
}