output "dashboard_url" {
  description = "Streamlit fraud monitor."
  value       = "http://${aws_lb.main.dns_name}"
}

output "api_url" {
  description = "Prediction API (OpenAPI docs at /docs)."
  value       = "http://${aws_lb.main.dns_name}:8000"
}

output "msk_bootstrap_brokers" {
  description = "SASL/SCRAM TLS bootstrap brokers for KAFKA_BOOTSTRAP_SERVERS."
  value       = aws_msk_cluster.main.bootstrap_brokers_sasl_scram
}

output "rds_endpoint" {
  description = "PostgreSQL endpoint (credentials in Secrets Manager)."
  value       = aws_db_instance.main.address
}

output "lake_bucket" {
  description = "S3 bucket holding lake/, checkpoints/ and mlflow/."
  value       = aws_s3_bucket.lake.bucket
}

output "ecr_repositories" {
  description = "Image repositories to push to."
  value       = { for name, repo in aws_ecr_repository.images : name => repo.repository_url }
}

output "prometheus_remote_write_url" {
  description = "Amazon Managed Service for Prometheus remote-write endpoint."
  value       = "${aws_prometheus_workspace.main.prometheus_endpoint}api/v1/remote_write"
}

output "secrets" {
  description = "Secrets Manager ARNs for Kafka and PostgreSQL credentials."
  value = {
    kafka    = aws_secretsmanager_secret.kafka.arn
    postgres = aws_secretsmanager_secret.db.arn
  }
}
