# terraform/modules/storage/outputs.tf

output "raw_data_bucket_name" {
  value = aws_s3_bucket.raw_data.bucket
}

output "raw_data_bucket_arn" {
  value = aws_s3_bucket.raw_data.arn
}

output "raw_data_bucket_id" {
  value = aws_s3_bucket.raw_data.id
}

output "processed_data_bucket_name" {
  value = aws_s3_bucket.processed_data.bucket
}

output "processed_data_bucket_arn" {
  value = aws_s3_bucket.processed_data.arn
}

output "scripts_bucket_name" {
  value = aws_s3_bucket.scripts.bucket
}

output "scripts_bucket_arn" {
  value = aws_s3_bucket.scripts.arn
}

output "airflow_logs_bucket_name" {
  value = aws_s3_bucket.airflow_logs.bucket
}

output "airflow_logs_bucket_arn" {
  value = aws_s3_bucket.airflow_logs.arn
}