# ECS Fargate: the same images as docker compose, configured for AWS through
# environment variables and Secrets Manager.

resource "aws_ecs_cluster" "main" {
  name = local.name
  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_service_discovery_private_dns_namespace" "main" {
  name = "${local.name}.local"
  vpc  = aws_vpc.main.id
}

locals {
  registry     = "${local.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com"
  image        = { for name in local.images : name => "${local.registry}/${var.project}/${name}:${var.image_tag}" }
  mlflow_host  = "mlflow.${aws_service_discovery_private_dns_namespace.main.name}"
  mlflow_uri   = "http://${local.mlflow_host}:5000"
  lake_s3a     = "s3a://${aws_s3_bucket.lake.bucket}"
  log_group    = { for name in ["spark", "api", "dashboard", "mlflow", "producer", "retrain"] : name => "/ecs/${local.name}/${name}" }
  kafka_secret = aws_secretsmanager_secret.kafka.arn
  db_secret    = aws_secretsmanager_secret.db.arn

  # Kafka over SASL/SCRAM + TLS: see producer/kafka_config.py.
  kafka_env = [
    { name = "KAFKA_BOOTSTRAP_SERVERS", value = aws_msk_cluster.main.bootstrap_brokers_sasl_scram },
    { name = "KAFKA_SECURITY_PROTOCOL", value = "SASL_SSL" },
    { name = "KAFKA_SASL_MECHANISM", value = "SCRAM-SHA-512" },
    { name = "KAFKA_SASL_USERNAME", value = var.project },
  ]
  kafka_secrets = [{ name = "KAFKA_SASL_PASSWORD", valueFrom = "${local.kafka_secret}:password::" }]
  db_secrets    = [{ name = "POSTGRES_DSN", valueFrom = "${local.db_secret}:dsn::" }]

  # ADOT collector sidecar: scrapes the app's Prometheus endpoint and
  # remote-writes to Amazon Managed Service for Prometheus.
  adot = {
    for name, target in { spark = "localhost:9108", api = "localhost:8000" } : name => {
      name                      = "adot-collector"
      image                     = "public.ecr.aws/aws-observability/aws-otel-collector:v0.43.3"
      essential                 = false
      environment = [{
        name = "AOT_CONFIG_CONTENT"
        value = yamlencode({
          extensions = { sigv4auth = { region = var.aws_region, service = "aps" } }
          receivers = {
            prometheus = {
              config = {
                scrape_configs = [{
                  job_name        = name
                  scrape_interval = "15s"
                  metrics_path    = name == "api" ? "/metrics/" : "/metrics"
                  static_configs  = [{ targets = [target] }]
                }]
              }
            }
          }
          exporters = {
            prometheusremotewrite = {
              endpoint = "${aws_prometheus_workspace.main.prometheus_endpoint}api/v1/remote_write"
              auth     = { authenticator = "sigv4auth" }
            }
          }
          service = {
            extensions = ["sigv4auth"]
            pipelines  = { metrics = { receivers = ["prometheus"], exporters = ["prometheusremotewrite"] } }
          }
        })
      }]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group[name]
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "adot"
        }
      }
    }
  }
}

# --------------------------------------------------------------------------- #
# IAM
# --------------------------------------------------------------------------- #

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# Execution role: pull images, write logs, read the two secrets.
resource "aws_iam_role" "execution" {
  name               = "${local.name}-ecs-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "execution_secrets" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [local.kafka_secret, local.db_secret]
  }
  statement {
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.msk_secrets.arn]
  }
}

resource "aws_iam_role_policy" "execution_secrets" {
  name   = "read-platform-secrets"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution_secrets.json
}

# Task role: what the application itself may do (lake bucket, metrics).
resource "aws_iam_role" "task" {
  name               = "${local.name}-ecs-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

data "aws_iam_policy_document" "task" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    actions   = ["aps:RemoteWrite"]
    resources = [aws_prometheus_workspace.main.arn]
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "lake-and-metrics"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}

# --------------------------------------------------------------------------- #
# Task definitions
# --------------------------------------------------------------------------- #

resource "aws_ecs_task_definition" "spark" {
  family                   = "${local.name}-spark"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.spark_cpu
  memory                   = var.spark_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name      = "spark"
      image     = local.image["spark"]
      essential = true
      environment = concat(local.kafka_env, [
        { name = "LAKE_PATH", value = "${local.lake_s3a}/lake" },
        { name = "CHECKPOINT_ROOT", value = "${local.lake_s3a}/checkpoints" },
        { name = "MLFLOW_TRACKING_URI", value = local.mlflow_uri },
        { name = "STARTING_OFFSETS", value = "latest" },
      ])
      secrets          = concat(local.kafka_secrets, local.db_secrets)
      portMappings     = [{ containerPort = 9108 }, { containerPort = 4040 }]
      logConfiguration = { logDriver = "awslogs", options = { awslogs-group = local.log_group["spark"], awslogs-region = var.aws_region, awslogs-stream-prefix = "spark" } }
    },
    local.adot["spark"],
  ])
}

resource "aws_ecs_task_definition" "api" {
  family                   = "${local.name}-api"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name             = "api"
      image            = local.image["api"]
      essential        = true
      environment      = [{ name = "MLFLOW_TRACKING_URI", value = local.mlflow_uri }]
      secrets          = local.db_secrets
      portMappings     = [{ containerPort = 8000 }]
      logConfiguration = { logDriver = "awslogs", options = { awslogs-group = local.log_group["api"], awslogs-region = var.aws_region, awslogs-stream-prefix = "api" } }
    },
    local.adot["api"],
  ])
}

resource "aws_ecs_task_definition" "dashboard" {
  family                   = "${local.name}-dashboard"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([{
    name             = "dashboard"
    image            = local.image["dashboard"]
    essential        = true
    secrets          = local.db_secrets
    portMappings     = [{ containerPort = 8501 }]
    logConfiguration = { logDriver = "awslogs", options = { awslogs-group = local.log_group["dashboard"], awslogs-region = var.aws_region, awslogs-stream-prefix = "dashboard" } }
  }])
}

resource "aws_ecs_task_definition" "mlflow" {
  family                   = "${local.name}-mlflow"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([{
    name      = "mlflow"
    image     = local.image["mlflow"]
    essential = true
    environment = [
      { name = "MLFLOW_ARTIFACTS_DESTINATION", value = "s3://${aws_s3_bucket.lake.bucket}/mlflow" },
      { name = "MLFLOW_ALLOWED_HOSTS", value = "${local.mlflow_host},${local.mlflow_host}:5000,localhost:5000" },
    ]
    secrets          = [{ name = "MLFLOW_BACKEND_STORE_URI", valueFrom = "${local.db_secret}:mlflow_dsn::" }]
    portMappings     = [{ containerPort = 5000 }]
    logConfiguration = { logDriver = "awslogs", options = { awslogs-group = local.log_group["mlflow"], awslogs-region = var.aws_region, awslogs-stream-prefix = "mlflow" } }
  }])
}

# Synthetic traffic for demos (desired_count = 0 by default).
resource "aws_ecs_task_definition" "producer" {
  family                   = "${local.name}-producer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([{
    name             = "producer"
    image            = local.image["producer"]
    essential        = true
    environment      = local.kafka_env
    secrets          = local.kafka_secrets
    command          = ["--rate", "20", "--quiet"]
    logConfiguration = { logDriver = "awslogs", options = { awslogs-group = local.log_group["producer"], awslogs-region = var.aws_region, awslogs-stream-prefix = "producer" } }
  }])
}

# Weekly retraining without MWAA: the full ml.retrain pipeline as one task.
resource "aws_ecs_task_definition" "retrain" {
  family                   = "${local.name}-retrain"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 2048
  memory                   = 8192
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([{
    name             = "retrain"
    image            = local.image["airflow"]
    essential        = true
    entryPoint       = ["/opt/ml-venv/bin/python", "-m", "ml.retrain", "pipeline", "--run-dir", "/tmp/run"]
    workingDirectory = "/opt/fraud"
    environment      = [{ name = "MLFLOW_TRACKING_URI", value = local.mlflow_uri }]
    secrets          = local.db_secrets
    logConfiguration = { logDriver = "awslogs", options = { awslogs-group = local.log_group["retrain"], awslogs-region = var.aws_region, awslogs-stream-prefix = "retrain" } }
  }])
}

# --------------------------------------------------------------------------- #
# Services
# --------------------------------------------------------------------------- #

locals {
  services = {
    spark     = { task = aws_ecs_task_definition.spark.arn, count = 1, port = null }
    api       = { task = aws_ecs_task_definition.api.arn, count = var.api_desired_count, port = 8000 }
    dashboard = { task = aws_ecs_task_definition.dashboard.arn, count = 1, port = 8501 }
    mlflow    = { task = aws_ecs_task_definition.mlflow.arn, count = 1, port = null }
    producer  = { task = aws_ecs_task_definition.producer.arn, count = 0, port = null }
  }
}

resource "aws_service_discovery_service" "mlflow" {
  name = "mlflow"
  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id
    dns_records {
      type = "A"
      ttl  = 10
    }
  }
}

resource "aws_ecs_service" "main" {
  for_each        = local.services
  name            = each.key
  cluster         = aws_ecs_cluster.main.id
  task_definition = each.value.task
  desired_count   = each.value.count
  launch_type     = "FARGATE"

  # The streaming job must never run twice against the same checkpoints.
  deployment_minimum_healthy_percent = each.key == "spark" ? 0 : 50
  deployment_maximum_percent         = each.key == "spark" ? 100 : 200

  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.tasks.id]
  }

  dynamic "load_balancer" {
    for_each = each.value.port == null ? [] : [each.value.port]
    content {
      target_group_arn = aws_lb_target_group.web[each.key].arn
      container_name   = each.key
      container_port   = load_balancer.value
    }
  }

  dynamic "service_registries" {
    for_each = each.key == "mlflow" ? [1] : []
    content {
      registry_arn = aws_service_discovery_service.mlflow.arn
    }
  }

  depends_on = [aws_lb_listener.web]
}

# --------------------------------------------------------------------------- #
# Load balancer: dashboard on :80, API on :8000
# --------------------------------------------------------------------------- #

locals {
  web = {
    dashboard = { port = 8501, listener = 80, health = "/_stcore/health" }
    api       = { port = 8000, listener = 8000, health = "/health" }
  }
}

resource "aws_lb" "main" {
  name               = local.name
  load_balancer_type = "application"
  subnets            = aws_subnet.public[*].id
  security_groups    = [aws_security_group.alb.id]
}

resource "aws_lb_target_group" "web" {
  for_each    = local.web
  name        = "${local.name}-${each.key}"
  port        = each.value.port
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = aws_vpc.main.id

  health_check {
    path    = each.value.health
    matcher = "200"
  }
}

resource "aws_lb_listener" "web" {
  for_each          = local.web
  load_balancer_arn = aws_lb.main.arn
  port              = each.value.listener
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.web[each.key].arn
  }
}

resource "aws_vpc_security_group_ingress_rule" "alb_api" {
  security_group_id = aws_security_group.alb.id
  description       = "API listener"
  from_port         = 8000
  to_port           = 8000
  ip_protocol       = "tcp"
  cidr_ipv4         = var.allowed_ingress_cidrs[0]
}

# --------------------------------------------------------------------------- #
# Weekly retraining schedule (EventBridge Scheduler -> ECS RunTask)
# --------------------------------------------------------------------------- #

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${local.name}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

data "aws_iam_policy_document" "scheduler" {
  statement {
    actions   = ["ecs:RunTask"]
    resources = [aws_ecs_task_definition.retrain.arn_without_revision, "${aws_ecs_task_definition.retrain.arn_without_revision}:*"]
  }
  statement {
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.execution.arn, aws_iam_role.task.arn]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  name   = "run-retraining"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler.json
}

resource "aws_scheduler_schedule" "retrain" {
  name                = "${local.name}-retrain"
  schedule_expression = "cron(0 3 ? * SUN *)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_ecs_cluster.main.arn
    role_arn = aws_iam_role.scheduler.arn
    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.retrain.arn
      launch_type         = "FARGATE"
      network_configuration {
        subnets          = aws_subnet.private[*].id
        security_groups  = [aws_security_group.tasks.id]
        assign_public_ip = false
      }
    }
  }
}
