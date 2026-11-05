# terraform/environments/prod.tfvars
# -----------------------------------------------------------------------------
# Production environment. Multi-AZ RDS, larger instances, restricted SSH.
# Usage: terraform apply -var-file="environments/prod.tfvars"
# -----------------------------------------------------------------------------

aws_region    = "us-east-1"
environment   = "prod"
project_name  = "ecommerce-data-platform"
owner         = "data-engineering"

# Networking
vpc_cidr              = "10.1.0.0/16"
availability_zones    = ["us-east-1a", "us-east-1b"]
public_subnet_cidrs   = ["10.1.1.0/24", "10.1.2.0/24"]
private_subnet_cidrs  = ["10.1.11.0/24", "10.1.12.0/24"]
ssh_allowed_cidr      = "203.0.113.10/32"  # Replace with your office/VPN IP

# Compute: larger instances for prod
airflow_instance_type      = "t3.large"
spark_master_instance_type = "t3.large"
spark_worker_instance_type = "t3.xlarge"
spark_worker_min_size      = 2
spark_worker_max_size      = 6
spark_worker_desired_size  = 2

# Database: Multi-AZ, larger
postgres_instance_class     = "db.r6g.large"
mysql_instance_class        = "db.r6g.large"
postgres_allocated_storage  = 100
mysql_allocated_storage     = 100
db_multi_az                 = true

# Monitoring
alert_email        = "data-team@company.com"
log_retention_days = 30