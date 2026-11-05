# terraform/modules/compute/variables.tf

variable "name_prefix" { type = string }
variable "environment" { type = string }
variable "vpc_id" { type = string }
variable "public_subnet_ids" { type = list(string) }
variable "ec2_security_group_id" { type = string }
variable "airflow_instance_profile" { type = string }
variable "spark_instance_profile" { type = string }
variable "airflow_instance_type" { type = string }
variable "spark_master_instance_type" { type = string }
variable "spark_worker_instance_type" { type = string }
variable "spark_worker_min_size" { type = number }
variable "spark_worker_max_size" { type = number }
variable "spark_worker_desired_size" { type = number }
variable "key_pair_name" { type = string }
variable "tags" {
  type    = map(string)
  default = {}
}