# terraform/modules/monitoring/main.tf

# --- SNS Topic for alerts ---
resource "aws_sns_topic" "alerts" {
  count = var.enabled ? 1 : 0
  name  = "${var.name_prefix}-alerts"
  tags  = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.enabled ? 1 : 0
  topic_arn = aws_sns_topic.alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# --- CloudWatch Log Groups ---
resource "aws_cloudwatch_log_group" "airflow" {
  count             = var.enabled ? 1 : 0
  name              = "/aws/ec2/${var.name_prefix}/airflow"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "spark" {
  count             = var.enabled ? 1 : 0
  name              = "/aws/ec2/${var.name_prefix}/spark"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "pipeline" {
  count             = var.enabled ? 1 : 0
  name              = "/aws/${var.name_prefix}/pipeline"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# --- Alarm: Airflow EC2 CPU high ---
resource "aws_cloudwatch_metric_alarm" "airflow_cpu" {
  count               = var.enabled ? 1 : 0
  alarm_name          = "${var.name_prefix}-airflow-cpu-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "Airflow EC2 CPU above 80% for 10 minutes."

  dimensions = {
    InstanceId = var.airflow_instance_id
  }

  alarm_actions = aws_sns_topic.alerts[*].arn
  ok_actions    = aws_sns_topic.alerts[*].arn

  tags = var.tags
}

# --- Alarm: PostgreSQL CPU high ---
resource "aws_cloudwatch_metric_alarm" "postgres_cpu" {
  count               = var.enabled ? 1 : 0
  alarm_name          = "${var.name_prefix}-postgres-cpu-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/RDS"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "PostgreSQL CPU above 80% for 10 minutes."

  dimensions = {
    DBInstanceIdentifier = var.postgres_identifier
  }

  alarm_actions = aws_sns_topic.alerts[*].arn
  ok_actions    = aws_sns_topic.alerts[*].arn

  tags = var.tags
}

# --- Alarm: MySQL CPU high ---
resource "aws_cloudwatch_metric_alarm" "mysql_cpu" {
  count               = var.enabled ? 1 : 0
  alarm_name          = "${var.name_prefix}-mysql-cpu-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/RDS"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "MySQL CPU above 80% for 10 minutes."

  dimensions = {
    DBInstanceIdentifier = var.mysql_identifier
  }

  alarm_actions = aws_sns_topic.alerts[*].arn
  ok_actions    = aws_sns_topic.alerts[*].arn

  tags = var.tags
}