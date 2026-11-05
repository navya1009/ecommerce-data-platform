# terraform/modules/triggers/main.tf
# -----------------------------------------------------------------------------
# Lambda function that reacts to S3 ObjectCreated events and triggers the
# Airflow DAG via its REST API (Phase 5.2 architecture).
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "s3_trigger" {
  function_name = "${var.name_prefix}-s3-airflow-trigger"
  role          = var.lambda_role_arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.11"
  timeout       = 30
  memory_size   = 128

  filename         = "${path.module}/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/lambda.zip")

  environment {
    variables = {
      AIRFLOW_BASE_URL = var.airflow_base_url
      AIRFLOW_DAG_ID   = var.airflow_dag_id
    }
  }

  tags = var.tags
}

# Allow S3 to invoke the Lambda.
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = var.raw_data_bucket_arn

  lifecycle {
    ignore_changes = [source_arn, statement_id_prefix]
  }
}

# Wire the S3 bucket notification to the Lambda.
resource "aws_s3_bucket_notification" "raw_data_trigger" {
  bucket = var.raw_data_bucket_id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_trigger.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3]
}