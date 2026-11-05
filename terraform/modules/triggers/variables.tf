# terraform/modules/triggers/variables.tf

variable "name_prefix" { type = string }
variable "aws_region" { type = string }
variable "raw_data_bucket_id" { type = string }
variable "raw_data_bucket_arn" { type = string }
variable "airflow_base_url" { type = string }
variable "airflow_dag_id" { type = string }
variable "lambda_role_arn" { type = string }
variable "tags" {
  type    = map(string)
  default = {}
}