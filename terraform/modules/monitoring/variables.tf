# terraform/modules/monitoring/variables.tf

variable "enabled" {
  description = "Enable CloudWatch log groups, alarms, and SNS. Set false for local emulators."
  type        = bool
  default     = true
}

variable "name_prefix" { type = string }
variable "environment" { type = string }
variable "alert_email" { type = string }
variable "log_retention_days" { type = number }
variable "airflow_instance_id" { type = string }
variable "postgres_identifier" { type = string }
variable "mysql_identifier" { type = string }
variable "tags" {
  type    = map(string)
  default = {}
}