
output "launch_arn" {
  value = local.launch_function_arn
}

output "step_function_arn" {
  value = module.orchestration.step_function_arn
}