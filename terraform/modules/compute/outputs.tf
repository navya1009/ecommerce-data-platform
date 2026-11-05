# terraform/modules/compute/outputs.tf

output "airflow_instance_id" {
  value = aws_instance.airflow.id
}

output "airflow_public_ip" {
  value = aws_instance.airflow.public_ip
}

output "spark_master_instance_id" {
  value = aws_instance.spark_master.id
}

output "spark_master_public_ip" {
  value = aws_instance.spark_master.public_ip
}

output "spark_worker_asg_name" {
  value = aws_autoscaling_group.spark_worker.name
}