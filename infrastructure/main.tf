# Real-time fraud detection platform on AWS.
#
#   network.tf   VPC, subnets, NAT, S3 gateway endpoint, security groups
#   storage.tf   S3 lake bucket (lake, checkpoints, MLflow artefacts), ECR repositories
#   kafka.tf     Amazon MSK with SASL/SCRAM credentials in Secrets Manager
#   database.tf  RDS PostgreSQL (serving store + MLflow backend)
#   compute.tf   ECS Fargate: Spark streaming, API, dashboard, MLflow; ALB; IAM
#   monitoring.tf Amazon Managed Service for Prometheus, CloudWatch logs

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = merge(
      {
        Project     = var.project
        Environment = var.environment
        ManagedBy   = "terraform"
      },
      var.tags,
    )
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

locals {
  name       = "${var.project}-${var.environment}"
  azs        = slice(data.aws_availability_zones.available.names, 0, var.az_count)
  account_id = data.aws_caller_identity.current.account_id
  images     = toset(["spark", "api", "dashboard", "producer", "airflow", "mlflow"])
}
