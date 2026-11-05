# terraform/modules/compute/main.tf
# -----------------------------------------------------------------------------
# EC2 instances for Airflow and Spark master, plus an Auto Scaling Group for
# Spark workers. All run Amazon Linux 2023.
# -----------------------------------------------------------------------------

data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

# =============================================================================
# EC2: Airflow
# =============================================================================
resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.airflow_instance_type
  subnet_id              = var.public_subnet_ids[0]
  vpc_security_group_ids = [var.ec2_security_group_id]
  iam_instance_profile   = var.airflow_instance_profile
  key_name               = var.key_pair_name != "" ? var.key_pair_name : null

    user_data = templatefile("${path.root}/../scripts/bootstrap_airflow.sh", {
    ENVIRONMENT = var.environment
    SECRET_ID   = "${var.name_prefix}-app-secrets"
    REPO_URL    = "https://github.com/your-org/ecommerce-data-platform.git"
    BRANCH      = "main"
  })

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
    encrypted   = true
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-airflow"
    Role = "airflow"
  })
}

# =============================================================================
# EC2: Spark Master
# =============================================================================
resource "aws_instance" "spark_master" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.spark_master_instance_type
  subnet_id              = var.public_subnet_ids[0]
  vpc_security_group_ids = [var.ec2_security_group_id]
  iam_instance_profile   = var.spark_instance_profile
  key_name               = var.key_pair_name != "" ? var.key_pair_name : null

    user_data = templatefile("${path.root}/../scripts/bootstrap_spark.sh", {
    ENVIRONMENT = var.environment
    S3_BUCKET   = "${var.name_prefix}-processed-data"
  })

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
    encrypted   = true
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-spark-master"
    Role = "spark-master"
  })
}

# =============================================================================
# Launch Template + Auto Scaling Group: Spark Workers
# =============================================================================
resource "aws_launch_template" "spark_worker" {
  name_prefix   = "${var.name_prefix}-spark-worker-"
  image_id      = data.aws_ami.amazon_linux_2023.id
  instance_type = var.spark_worker_instance_type
  key_name      = var.key_pair_name != "" ? var.key_pair_name : null

  iam_instance_profile {
    name = var.spark_instance_profile
  }

  vpc_security_group_ids = [var.ec2_security_group_id]

  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size = 30
      volume_type = "gp3"
      encrypted   = true
    }
  }

    user_data = base64encode(templatefile("${path.root}/../scripts/bootstrap_spark_worker.sh", {
    ENVIRONMENT      = var.environment
    SPARK_MASTER_URL = "spark://${aws_instance.spark_master.private_ip}:7077"
  }))

  tag_specifications {
    resource_type = "instance"
    tags = merge(var.tags, {
      Name = "${var.name_prefix}-spark-worker"
      Role = "spark-worker"
    })
  }

  tags = var.tags
}

resource "aws_autoscaling_group" "spark_worker" {
  name                = "${var.name_prefix}-spark-worker-asg"
  vpc_zone_identifier = var.public_subnet_ids

  min_size         = var.spark_worker_min_size
  max_size         = var.spark_worker_max_size
  desired_capacity = var.spark_worker_desired_size

  launch_template {
    id      = aws_launch_template.spark_worker.id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "${var.name_prefix}-spark-worker"
    propagate_at_launch = true
  }

  tag {
    key                 = "Environment"
    value               = var.environment
    propagate_at_launch = true
  }
}