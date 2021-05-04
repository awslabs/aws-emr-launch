
output "cdk_outputs_file" {
  value = local.input
}

output "launch_function_arn" {
  value = local.launch_arn_string
}

output "instance_role_name" {
  value = local.instance_role_string
}

output "cluster-name" {
  value = var.emr_cluster_name
}