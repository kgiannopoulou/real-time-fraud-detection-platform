resource "random_password" "db" {
  length  = 32
  special = false
}

resource "aws_db_subnet_group" "main" {
  name       = local.name
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_db_parameter_group" "main" {
  name   = "${local.name}-pg16"
  family = "postgres16"

  parameter {
    name  = "log_min_duration_statement"
    value = "500"
  }
}

# Serving store (database "fraud") and MLflow backend store (database "mlflow").
resource "aws_db_instance" "main" {
  identifier                   = local.name
  engine                       = "postgres"
  engine_version               = "16"
  instance_class               = var.db_instance_class
  allocated_storage            = 20
  max_allocated_storage        = 100
  storage_type                 = "gp3"
  storage_encrypted            = true
  db_name                      = "fraud"
  username                     = "fraud"
  password                     = random_password.db.result
  db_subnet_group_name         = aws_db_subnet_group.main.name
  vpc_security_group_ids       = [aws_security_group.rds.id]
  parameter_group_name         = aws_db_parameter_group.main.name
  multi_az                     = var.db_multi_az
  backup_retention_period      = 7
  deletion_protection          = var.environment == "prod"
  skip_final_snapshot          = var.environment != "prod"
  final_snapshot_identifier    = var.environment == "prod" ? "${local.name}-final" : null
  performance_insights_enabled = true
}

resource "aws_secretsmanager_secret" "db" {
  name                    = "${local.name}/postgres"
  recovery_window_in_days = 0
}

# Ready-made connection strings injected into the containers.
resource "aws_secretsmanager_secret_version" "db" {
  secret_id = aws_secretsmanager_secret.db.id
  secret_string = jsonencode({
    username   = "fraud"
    password   = random_password.db.result
    host       = aws_db_instance.main.address
    dsn        = "postgresql://fraud:${random_password.db.result}@${aws_db_instance.main.address}:5432/fraud"
    mlflow_dsn = "postgresql://fraud:${random_password.db.result}@${aws_db_instance.main.address}:5432/mlflow"
  })
}
