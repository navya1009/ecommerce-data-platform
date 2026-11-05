# terraform/environments/dev.tfvars
# -----------------------------------------------------------------------------
# Development environment. Single-AZ, small instances, no Multi-AZ RDS.
# Usage: terraform plan -var-file="environments/dev.tfvars"
# -----------------------------------------------------------------------------

aws_region   = "us-east-1"
environment  = "dev"
project_name = "ecommerce-data-platform"
owner        = "data-engineering"

# Networking
vpc_cidr             = "10.0.0.0/16"
availability_zones   = ["us-east-1a", "us-east-1b"]
public_subnet_cidrs  = ["10.0.1.0/24", "10.0.2.0/24"]
private_subnet_cidrs = ["10.0.11.0/24", "10.0.12.0/24"]
ssh_allowed_cidr     = "0.0.0.0/0"

# Compute
airflow_instance_type      = "t3.medium"
spark_master_instance_type = "t3.medium"
spark_worker_instance_type = "t3.medium"
spark_worker_min_size      = 1
spark_worker_max_size      = 2
spark_worker_desired_size  = 1

# Database
postgres_instance_class    = "db.t3.medium"
mysql_instance_class       = "db.t3.medium"
postgres_allocated_storage = 20
mysql_allocated_storage    = 20
db_multi_az                = false

# Monitoring
alert_email        = "data-team@company.com"
log_retention_days = 7

enable_monitoring = false
