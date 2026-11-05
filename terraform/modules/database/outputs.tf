# terraform/modules/database/outputs.tf

output "postgres_endpoint" {
  value     = aws_db_instance.postgres.endpoint
  sensitive = true
}

output "postgres_identifier" {
  value = aws_db_instance.postgres.identifier
}

output "mysql_endpoint" {
  value     = aws_db_instance.mysql.endpoint
  sensitive = true
}

output "mysql_identifier" {
  value = aws_db_instance.mysql.identifier
}