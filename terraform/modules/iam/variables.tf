# terraform/modules/iam/variables.tf

variable "name_prefix" { type = string }
variable "aws_region" { type = string }
variable "raw_data_bucket_arn" { type = string }
variable "scripts_bucket_arn" { type = string }
variable "airflow_logs_bucket_arn" { type = string }
variable "tags" {
  type    = map(string)
  default = {}
}