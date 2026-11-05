# terraform/modules/database/variables.tf

variable "name_prefix" { type = string }
variable "environment" { type = string }
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "rds_security_group_id" { type = string }
variable "postgres_instance_class" { type = string }
variable "mysql_instance_class" { type = string }
variable "postgres_allocated_storage" { type = number }
variable "mysql_allocated_storage" { type = number }
variable "db_multi_az" { type = bool }
variable "db_username" {
  type      = string
  sensitive = true
}
variable "db_password" {
  type      = string
  sensitive = true
}
variable "tags" {
  type    = map(string)
  default = {}
}