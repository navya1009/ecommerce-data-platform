# terraform/modules/database/main.tf
# -----------------------------------------------------------------------------
# RDS PostgreSQL (data warehouse) and RDS MySQL (staging).
# Multi-AZ controlled by var.db_multi_az (false in dev, true in prod).
# Both live in private subnets, accessible only from the EC2 security group.
# -----------------------------------------------------------------------------

# --- Subnet Group (tells RDS which subnets it can use) ---
resource "aws_db_subnet_group" "main" {
  name       = "${var.name_prefix}-db-subnet-group"
  subnet_ids = var.private_subnet_ids

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-db-subnet-group"
  })
}

# --- Parameter Group: PostgreSQL (tuned for analytics) ---
resource "aws_db_parameter_group" "postgres" {
  name   = "${var.name_prefix}-postgres-params"
  family = "postgres15"

  parameter {
    name  = "max_connections"
    value = "200"
  }

  parameter {
    name  = "shared_buffers"
    value = "{DBInstanceClassMemory/4}"
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-postgres-params"
  })
}

# --- Parameter Group: MySQL ---
resource "aws_db_parameter_group" "mysql" {
  name   = "${var.name_prefix}-mysql-params"
  family = "mysql8.0"

  parameter {
    name  = "max_connections"
    value = "200"
  }

  parameter {
    name  = "character_set_server"
    value = "utf8mb4"
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-mysql-params"
  })
}

# --- RDS PostgreSQL (Data Warehouse) ---
resource "aws_db_instance" "postgres" {
  identifier = "${var.name_prefix}-postgres"

  engine         = "postgres"
  engine_version = "15"
  instance_class = var.postgres_instance_class

  allocated_storage     = var.postgres_allocated_storage
  max_allocated_storage = var.postgres_allocated_storage * 2
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = "warehouse"
  username = var.db_username
  password = var.db_password
  port     = 5432

  multi_az               = var.db_multi_az
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [var.rds_security_group_id]
  parameter_group_name   = aws_db_parameter_group.postgres.name

  backup_retention_period = var.environment == "prod" ? 7 : 1
  backup_window           = "03:00-04:00"
  maintenance_window      = "sun:04:00-sun:05:00"

  skip_final_snapshot       = var.environment != "prod"
  final_snapshot_identifier = var.environment == "prod" ? "${var.name_prefix}-postgres-final" : null

  deletion_protection = var.environment == "prod"

  tags = merge(var.tags, {
    Name    = "${var.name_prefix}-postgres"
    Purpose = "data-warehouse"
  })

  lifecycle {
    ignore_changes = [
      port,
      max_allocated_storage,
      maintenance_window,
      tags,
      tags_all,
    ]
  }
}

# --- RDS MySQL (Staging) ---
resource "aws_db_instance" "mysql" {
  identifier = "${var.name_prefix}-mysql"

  engine         = "mysql"
  engine_version = "8.0"
  instance_class = var.mysql_instance_class

  allocated_storage     = var.mysql_allocated_storage
  max_allocated_storage = var.mysql_allocated_storage * 2
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = "ecommerce_staging"
  username = var.db_username
  password = var.db_password
  port     = 3306

  multi_az               = var.db_multi_az
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [var.rds_security_group_id]
  parameter_group_name   = aws_db_parameter_group.mysql.name

  backup_retention_period = var.environment == "prod" ? 7 : 1
  backup_window           = "03:00-04:00"
  maintenance_window      = "sun:04:00-sun:05:00"

  skip_final_snapshot       = var.environment != "prod"
  final_snapshot_identifier = var.environment == "prod" ? "${var.name_prefix}-mysql-final" : null

  deletion_protection = var.environment == "prod"

  tags = merge(var.tags, {
    Name    = "${var.name_prefix}-mysql"
    Purpose = "staging"
  })

  lifecycle {
    ignore_changes = [
      port,
      max_allocated_storage,
      maintenance_window,
      tags,
      tags_all,
    ]
  }
}