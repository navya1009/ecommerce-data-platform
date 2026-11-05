# terraform/variables.tf
# -----------------------------------------------------------------------------
# Root input variables for the ecommerce-data-platform infrastructure.
# These are supplied via environments/dev.tfvars or environments/prod.tfvars.
# -----------------------------------------------------------------------------

variable "aws_region" {
  description = "AWS region where all resources will be provisioned."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short project identifier used in resource names and tags."
  type        = string
  default     = "ecommerce-data-platform"
}

variable "environment" {
  description = "Deployment environment. One of: dev, staging, prod."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

# -----------------------------------------------------------------------------
# Networking
# -----------------------------------------------------------------------------
variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "List of AZs to spread subnets across. Use 2 for prod, 1 for dev."
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets (one per AZ)."
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets (one per AZ)."
  type        = list(string)
  default     = ["10.0.11.0/24", "10.0.12.0/24"]
}

# -----------------------------------------------------------------------------
# Compute
# -----------------------------------------------------------------------------
variable "airflow_instance_type" {
  description = "EC2 instance type for the Airflow server."
  type        = string
  default     = "t3.medium"
}

variable "spark_master_instance_type" {
  description = "EC2 instance type for the Spark master node."
  type        = string
  default     = "t3.medium"
}

variable "spark_worker_instance_type" {
  description = "EC2 instance type for Spark worker nodes."
  type        = string
  default     = "t3.medium"
}

variable "spark_worker_min_size" {
  description = "Minimum number of Spark worker instances in the ASG."
  type        = number
  default     = 1
}

variable "spark_worker_max_size" {
  description = "Maximum number of Spark worker instances in the ASG."
  type        = number
  default     = 2
}

variable "spark_worker_desired_size" {
  description = "Desired number of Spark worker instances in the ASG."
  type        = number
  default     = 1
}

variable "key_pair_name" {
  description = "Name of an existing EC2 Key Pair for SSH access. Leave empty to skip."
  type        = string
  default     = ""
}

variable "ssh_allowed_cidr" {
  description = "CIDR allowed to SSH into EC2 instances. Restrict to your IP in prod."
  type        = string
  default     = "0.0.0.0/0"
}

# -----------------------------------------------------------------------------
# Database
# -----------------------------------------------------------------------------
variable "postgres_instance_class" {
  description = "RDS instance class for PostgreSQL data warehouse."
  type        = string
  default     = "db.t3.medium"
}

variable "mysql_instance_class" {
  description = "RDS instance class for MySQL staging database."
  type        = string
  default     = "db.t3.medium"
}

variable "postgres_allocated_storage" {
  description = "Allocated storage (GB) for PostgreSQL."
  type        = number
  default     = 20
}

variable "mysql_allocated_storage" {
  description = "Allocated storage (GB) for MySQL."
  type        = number
  default     = 20
}

variable "db_multi_az" {
  description = "Enable Multi-AZ for RDS. true for prod, false for dev."
  type        = bool
  default     = false
}

variable "db_username" {
  description = "Master username for RDS databases."
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "db_password" {
  description = "Master password for RDS databases. Supply via TF_VAR_db_password env var."
  type        = string
  sensitive   = true
  default     = ""
}

# -----------------------------------------------------------------------------
# Monitoring / Alerts
# -----------------------------------------------------------------------------
variable "alert_email" {
  description = "Email address to receive CloudWatch alarm notifications."
  type        = string
  default     = "data-team@company.com"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days."
  type        = number
  default     = 14
}

# -----------------------------------------------------------------------------
# Tags
# -----------------------------------------------------------------------------
variable "owner" {
  description = "Team or person responsible for these resources."
  type        = string
  default     = "data-engineering"
}

variable "enable_monitoring" {
  description = "Enable CloudWatch + SNS monitoring resources. Set false for fakecloud."
  type        = bool
  default     = true
}

