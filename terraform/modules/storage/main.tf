# terraform/modules/storage/main.tf
# -----------------------------------------------------------------------------
# S3 buckets for raw data, processed data, scripts, and Airflow logs.
# All buckets: versioning on, encryption on, lifecycle rules applied,
# public access blocked.
# -----------------------------------------------------------------------------

# Reusable lifecycle rule: IA after 30d, Glacier after 90d, delete after 365d.
locals {
  lifecycle_rules = [
    {
      id      = "archive-and-expire"
      enabled = true
      transition = [
        { days = 30, storage_class = "STANDARD_IA" },
        { days = 90, storage_class = "GLACIER" },
      ]
      expiration_days = 365
    }
  ]
}

# --- Raw Data Bucket ---
resource "aws_s3_bucket" "raw_data" {
  bucket = "${var.name_prefix}-raw-data"

  tags = merge(var.tags, {
    Name    = "${var.name_prefix}-raw-data"
    Purpose = "raw-data"
  })
}

resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw_data" {
  bucket                  = aws_s3_bucket.raw_data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "archive-and-expire"
    status = "Enabled"

    filter {}

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    expiration {
      days = 365
    }
  }
}

# --- Processed Data Bucket ---
resource "aws_s3_bucket" "processed_data" {
  bucket = "${var.name_prefix}-processed-data"

  tags = merge(var.tags, {
    Name    = "${var.name_prefix}-processed-data"
    Purpose = "processed-data"
  })
}

resource "aws_s3_bucket_versioning" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "processed_data" {
  bucket                  = aws_s3_bucket.processed_data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id

  rule {
    id     = "archive-and-expire"
    status = "Enabled"

    filter {}

    transition {  
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    expiration {
      days = 365
    }
  }
}

# --- Scripts Bucket ---
resource "aws_s3_bucket" "scripts" {
  bucket = "${var.name_prefix}-scripts"

  tags = merge(var.tags, {
    Name    = "${var.name_prefix}-scripts"
    Purpose = "scripts"
  })
}

resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket                  = aws_s3_bucket.scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- Airflow Logs Bucket ---
resource "aws_s3_bucket" "airflow_logs" {
  bucket = "${var.name_prefix}-airflow-logs"

  tags = merge(var.tags, {
    Name    = "${var.name_prefix}-airflow-logs"
    Purpose = "airflow-logs"
  })
}

resource "aws_s3_bucket_versioning" "airflow_logs" {
  bucket = aws_s3_bucket.airflow_logs.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "airflow_logs" {
  bucket = aws_s3_bucket.airflow_logs.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "airflow_logs" {
  bucket                  = aws_s3_bucket.airflow_logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "airflow_logs" {
  bucket = aws_s3_bucket.airflow_logs.id

  rule {
    id     = "expire-logs"
    status = "Enabled"

    filter {}

    expiration {
      days = 90
    }
  }
}