# terraform/modules/networking/variables.tf

variable "name_prefix" {
  description = "Prefix for resource names (e.g. ecommerce-data-platform-dev)."
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
}

variable "availability_zones" {
  description = "List of AZs to use."
  type        = list(string)
}

variable "public_subnet_cidrs" {
  description = "CIDRs for public subnets."
  type        = list(string)
}

variable "private_subnet_cidrs" {
  description = "CIDRs for private subnets."
  type        = list(string)
}

variable "ssh_allowed_cidr" {
  description = "CIDR allowed to SSH into EC2 instances."
  type        = string
}

variable "tags" {
  description = "Common tags applied to all resources."
  type        = map(string)
  default     = {}
}