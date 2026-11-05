output "sns_alert_topic_arn" {
  value = var.enabled ? aws_sns_topic.alerts[0].arn : ""
}

output "airflow_log_group_name" {
  value = var.enabled ? aws_cloudwatch_log_group.airflow[0].name : ""
}

output "spark_log_group_name" {
  value = var.enabled ? aws_cloudwatch_log_group.spark[0].name : ""
}