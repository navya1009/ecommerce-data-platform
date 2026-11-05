# terraform/modules/iam/outputs.tf

output "airflow_instance_profile_name" {
  value = aws_iam_instance_profile.airflow.name
}

output "spark_instance_profile_name" {
  value = aws_iam_instance_profile.spark.name
}

output "lambda_role_arn" {
  value = aws_iam_role.lambda.arn
}

output "github_actions_role_arn" {
  value = aws_iam_role.github_actions.arn
}