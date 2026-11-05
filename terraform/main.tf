# terraform/main.tf
# -----------------------------------------------------------------------------
# Root configuration for ecommerce-data-platform.
# Declares the AWS provider, remote backend, and calls all modules.
# -----------------------------------------------------------------------------

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # ---------------------------------------------------------------------------
  # Remote backend: state file lives in S3, locking via DynamoDB.
  # The bucket and table MUST exist before running `terraform init`.
  # See docs/terraform-bootstrap.md for one-time setup commands.
  # ---------------------------------------------------------------------------
  # backend "s3" {
  #  bucket         = "ecommerce-data-platform-terraform-state"
  #  key            = "terraform/state/terraform.tfstate"
  #  region         = "us-east-1"
  #  dynamodb_table = "ecommerce-data-platform-terraform-locks"
  #  encrypt        = true
  #}
}

provider "aws" {
  region                      = var.aws_region
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  # Force path-style URLs for S3 (required for local emulators like fakecloud)
  s3_use_path_style = true

  endpoints {
    s3          = "http://localhost:4566"
    rds         = "http://localhost:4566"
    iam         = "http://localhost:4566"
    lambda      = "http://localhost:4566"
    logs        = "http://localhost:4566"
    sns         = "http://localhost:4566"
    ec2         = "http://localhost:4566"
    dynamodb    = "http://localhost:4566"
    autoscaling = "http://localhost:4566"
  }

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      Owner       = var.owner
    }
  }
}

# -----------------------------------------------------------------------------
# Locals: values derived from variables, reused across modules.
# -----------------------------------------------------------------------------
locals {
  name_prefix = "${var.project_name}-${var.environment}"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    Owner       = var.owner
  }
}

# =============================================================================
# Module: Networking
# Creates VPC, public/private subnets, NAT gateway, route tables, security groups.
# =============================================================================
module "networking" {
  source = "./modules/networking"

  name_prefix          = local.name_prefix
  vpc_cidr             = var.vpc_cidr
  availability_zones   = var.availability_zones
  public_subnet_cidrs  = var.public_subnet_cidrs
  private_subnet_cidrs = var.private_subnet_cidrs
  ssh_allowed_cidr     = var.ssh_allowed_cidr
  tags                 = local.common_tags
}

# =============================================================================
# Module: Storage
# Creates S3 buckets: raw-data, processed-data, scripts, airflow-logs.
# The terraform-state bucket is created manually (bootstrap) so it is not here.
# =============================================================================
module "storage" {
  source = "./modules/storage"

  name_prefix = local.name_prefix
  environment = var.environment
  tags        = local.common_tags
}

# =============================================================================
# Module: IAM
# Creates roles and policies for EC2, Lambda, and GitHub Actions OIDC.
# =============================================================================
module "iam" {
  source = "./modules/iam"

  name_prefix          = local.name_prefix
  aws_region           = var.aws_region
  raw_data_bucket_arn  = module.storage.raw_data_bucket_arn
  scripts_bucket_arn   = module.storage.scripts_bucket_arn
  airflow_logs_bucket_arn = module.storage.airflow_logs_bucket_arn
  tags                 = local.common_tags
}

# =============================================================================
# Module: Database
# Creates RDS PostgreSQL (DWH) and RDS MySQL (staging).
# =============================================================================
module "database" {
  source = "./modules/database"

  name_prefix              = local.name_prefix
  environment              = var.environment
  vpc_id                   = module.networking.vpc_id
  private_subnet_ids       = module.networking.private_subnet_ids
  rds_security_group_id    = module.networking.rds_security_group_id
  postgres_instance_class  = var.postgres_instance_class
  mysql_instance_class     = var.mysql_instance_class
  postgres_allocated_storage = var.postgres_allocated_storage
  mysql_allocated_storage  = var.mysql_allocated_storage
  db_multi_az              = var.db_multi_az
  db_username              = var.db_username
  db_password              = var.db_password
  tags                     = local.common_tags
}

# =============================================================================
# Module: Compute
# Creates EC2 for Airflow, EC2 for Spark master, ASG for Spark workers.
# =============================================================================
module "compute" {
  source = "./modules/compute"

  name_prefix                = local.name_prefix
  environment                = var.environment
  vpc_id                     = module.networking.vpc_id
  public_subnet_ids          = module.networking.public_subnet_ids
  ec2_security_group_id      = module.networking.ec2_security_group_id
  airflow_instance_profile   = module.iam.airflow_instance_profile_name
  spark_instance_profile     = module.iam.spark_instance_profile_name
  airflow_instance_type      = var.airflow_instance_type
  spark_master_instance_type = var.spark_master_instance_type
  spark_worker_instance_type = var.spark_worker_instance_type
  spark_worker_min_size      = var.spark_worker_min_size
  spark_worker_max_size      = var.spark_worker_max_size
  spark_worker_desired_size  = var.spark_worker_desired_size
  key_pair_name              = var.key_pair_name
  tags                       = local.common_tags
}

# =============================================================================
# Module: Monitoring
# Creates CloudWatch log groups, metric alarms, and SNS topic for alerts.
# =============================================================================
module "monitoring" {
  source = "./modules/monitoring"

  enabled             = var.enable_monitoring
  name_prefix         = local.name_prefix
  environment         = var.environment
  alert_email         = var.alert_email
  log_retention_days  = var.log_retention_days
  airflow_instance_id = module.compute.airflow_instance_id
  postgres_identifier = module.database.postgres_identifier
  mysql_identifier    = module.database.mysql_identifier
  tags                = local.common_tags
}

# =============================================================================
# Module: Triggers
# Lambda + S3 event notification to trigger Airflow DAG on new S3 object.
# =============================================================================
module "triggers" {
  source = "./modules/triggers"

  name_prefix          = local.name_prefix
  aws_region           = var.aws_region
  raw_data_bucket_id   = module.storage.raw_data_bucket_id
  raw_data_bucket_arn  = module.storage.raw_data_bucket_arn
  airflow_base_url     = "http://${module.compute.airflow_public_ip}:8080"
  airflow_dag_id       = "ecommerce_daily_pipeline"
  lambda_role_arn      = module.iam.lambda_role_arn
  tags                 = local.common_tags
}