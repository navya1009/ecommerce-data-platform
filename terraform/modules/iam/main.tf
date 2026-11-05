# terraform/modules/iam/main.tf
# -----------------------------------------------------------------------------
# IAM roles for:
#   - EC2 (Airflow): access S3 for logs/scripts, CloudWatch for logs
#   - EC2 (Spark): access S3 for data
#   - Lambda: read S3 events, call Airflow REST API
#   - GitHub Actions: OIDC-based deploy (no long-lived keys)
# -----------------------------------------------------------------------------

# =============================================================================
# EC2 role: Airflow
# =============================================================================
resource "aws_iam_role" "airflow" {
  name = "${var.name_prefix}-airflow-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "airflow" {
  name = "${var.name_prefix}-airflow-policy"
  role = aws_iam_role.airflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          var.raw_data_bucket_arn,
          "${var.raw_data_bucket_arn}/*",
          var.scripts_bucket_arn,
          "${var.scripts_bucket_arn}/*",
          var.airflow_logs_bucket_arn,
          "${var.airflow_logs_bucket_arn}/*"
        ]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "airflow" {
  name = "${var.name_prefix}-airflow-profile"
  role = aws_iam_role.airflow.name
}

# =============================================================================
# EC2 role: Spark
# =============================================================================
resource "aws_iam_role" "spark" {
  name = "${var.name_prefix}-spark-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "spark" {
  name = "${var.name_prefix}-spark-policy"
  role = aws_iam_role.spark.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          var.raw_data_bucket_arn,
          "${var.raw_data_bucket_arn}/*",
          var.scripts_bucket_arn,
          "${var.scripts_bucket_arn}/*"
        ]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "spark" {
  name = "${var.name_prefix}-spark-profile"
  role = aws_iam_role.spark.name
}

# =============================================================================
# Lambda role (S3 -> Airflow trigger from Phase 5.2)
# =============================================================================
resource "aws_iam_role" "lambda" {
  name = "${var.name_prefix}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_s3" {
  name = "${var.name_prefix}-lambda-s3-policy"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:GetBucketNotification"]
      Resource = [
        var.raw_data_bucket_arn,
        "${var.raw_data_bucket_arn}/*"
      ]
    }]
  })
}

# =============================================================================
# GitHub Actions OIDC role (no long-lived access keys)
# Allows GitHub Actions workflows to assume a role via OIDC.
# =============================================================================
resource "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"

  client_id_list = ["sts.amazonaws.com"]

  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]

  tags = var.tags
}

resource "aws_iam_role" "github_actions" {
  name = "${var.name_prefix}-github-actions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = aws_iam_openid_connect_provider.github.arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
        StringLike = {
          # Restrict to your repo. Replace with your actual org/repo.
          "token.actions.githubusercontent.com:sub" = "repo:your-org/ecommerce-data-platform:*"
        }
      }
    }]
  })

  tags = var.tags
}

# Minimal deploy permissions: S3 sync + Terraform state read/write.
resource "aws_iam_role_policy" "github_actions" {
  name = "${var.name_prefix}-github-actions-policy"
  role = aws_iam_role.github_actions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Deploy"
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
        Resource = [
          var.scripts_bucket_arn,
          "${var.scripts_bucket_arn}/*"
        ]
      },
      {
        Sid    = "TerraformState"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::ecommerce-data-platform-terraform-state",
          "arn:aws:s3:::ecommerce-data-platform-terraform-state/*"
        ]
      },
      {
        Sid    = "TerraformLock"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem"
        ]
        Resource = "arn:aws:dynamodb:${var.aws_region}:*:table/ecommerce-data-platform-terraform-locks"
      }
    ]
  })
}