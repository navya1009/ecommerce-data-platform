# terraform/modules/storage/variables.tf

variable "name_prefix" {
  description = "Prefix for bucket names."
  type        = string
}

variable "environment" {
  description = "Environment name (dev/staging/prod)."
  type        = string
}

variable "tags" {
  description = "Common tags."
  type        = map(string)
  default     = {}
}