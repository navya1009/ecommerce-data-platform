# terraform/outputs.tf
# -----------------------------------------------------------------------------
# Values Terraform prints after `terraform apply`.
# Useful for connecting to resources and for CI/CD pipelines.
# -----------------------------------------------------------------------------

# --- Networking ---
output "vpc_id" {
  description = "ID of the created VPC."
  value       = module.networking.vpc_id
}

output "public_subnet_ids" {
  description = "IDs of the public subnets."
  value       = module.networking.public_subnet_ids
}

output "private_subnet_ids" {
  description = "IDs of the private subnets."
  value       = module.networking.private_subnet_ids
}

# --- Storage ---
output "raw_data_bucket_name" {
  description = "Name of the S3 bucket for raw data."
  value       = module.storage.raw_data_bucket_name
}

output "processed_data_bucket_name" {
  description = "Name of the S3 bucket for processed data."
  value       = module.storage.processed_data_bucket_name
}

output "scripts_bucket_name" {
  description = "Name of the S3 bucket for scripts."
  value       = module.storage.scripts_bucket_name
}

output "airflow_logs_bucket_name" {
  description = "Name of the S3 bucket for Airflow logs."
  value       = module.storage.airflow_logs_bucket_name
}

# --- Database ---
output "postgres_endpoint" {
  description = "Connection endpoint for the PostgreSQL data warehouse."
  value       = module.database.postgres_endpoint
  sensitive   = true
}

output "mysql_endpoint" {
  description = "Connection endpoint for the MySQL staging database."
  value       = module.database.mysql_endpoint
  sensitive   = true
}

# --- Compute ---
output "airflow_public_ip" {
  description = "Public IP of the Airflow EC2 instance."
  value       = module.compute.airflow_public_ip
}

output "spark_master_public_ip" {
  description = "Public IP of the Spark master EC2 instance."
  value       = module.compute.spark_master_public_ip
}

output "spark_worker_asg_name" {
  description = "Name of the Spark worker Auto Scaling Group."
  value       = module.compute.spark_worker_asg_name
}

# --- Monitoring ---
output "sns_alert_topic_arn" {
  description = "ARN of the SNS topic used for alerts."
  value       = module.monitoring.sns_alert_topic_arn
}

output "github_actions_role_arn" {
  description = "IAM role ARN for GitHub Actions OIDC deployments."
  value       = module.iam.github_actions_role_arn
}