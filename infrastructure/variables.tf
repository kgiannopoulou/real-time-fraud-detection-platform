variable "project" {
  description = "Name prefix for every resource."
  type        = string
  default     = "fraud-platform"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "eu-central-1"
}

variable "vpc_cidr" {
  description = "CIDR block of the platform VPC."
  type        = string
  default     = "10.40.0.0/16"
}

variable "az_count" {
  description = "Number of availability zones (MSK needs one broker subnet per AZ)."
  type        = number
  default     = 2

  validation {
    condition     = var.az_count >= 2 && var.az_count <= 3
    error_message = "Use 2 or 3 availability zones."
  }
}

variable "msk_instance_type" {
  description = "MSK broker instance type."
  type        = string
  default     = "kafka.t3.small"
}

variable "msk_kafka_version" {
  description = "Kafka version on MSK."
  type        = string
  default     = "3.9.x"
}

variable "db_instance_class" {
  description = "RDS instance class for the serving store and MLflow backend."
  type        = string
  default     = "db.t4g.micro"
}

variable "db_multi_az" {
  description = "Run RDS Multi-AZ (recommended for production)."
  type        = bool
  default     = false
}

variable "image_tag" {
  description = "Tag of the images in ECR to deploy."
  type        = string
  default     = "latest"
}

variable "spark_cpu" {
  description = "Fargate CPU units for the Spark streaming task."
  type        = number
  default     = 2048
}

variable "spark_memory" {
  description = "Fargate memory (MiB) for the Spark streaming task."
  type        = number
  default     = 8192
}

variable "api_desired_count" {
  description = "Number of API tasks."
  type        = number
  default     = 2
}

variable "allowed_ingress_cidrs" {
  description = "CIDR blocks allowed to reach the public load balancer."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "tags" {
  description = "Extra tags applied to every resource."
  type        = map(string)
  default     = {}
}
