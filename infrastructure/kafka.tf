locals {
  kafka_replication = var.az_count >= 3 ? 3 : 2
  kafka_min_isr     = var.az_count >= 3 ? 2 : 1
}

resource "aws_msk_configuration" "main" {
  name           = "${local.name}-kafka"
  kafka_versions = [var.msk_kafka_version]
  server_properties = join("\n", [
    "auto.create.topics.enable=false",
    "default.replication.factor=${local.kafka_replication}",
    "min.insync.replicas=${local.kafka_min_isr}",
    "num.partitions=6",
    "log.retention.hours=24",
  ])
}

resource "aws_kms_key" "msk_secrets" {
  description             = "Encrypts the MSK SCRAM credentials (MSK requires a customer managed key)"
  deletion_window_in_days = 7
  enable_key_rotation     = true
}

resource "aws_msk_cluster" "main" {
  cluster_name           = local.name
  kafka_version          = var.msk_kafka_version
  number_of_broker_nodes = var.az_count

  broker_node_group_info {
    instance_type   = var.msk_instance_type
    client_subnets  = aws_subnet.private[*].id
    security_groups = [aws_security_group.msk.id]
    storage_info {
      ebs_storage_info {
        volume_size = 50
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  client_authentication {
    sasl {
      scram = true
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }
}

resource "random_password" "kafka" {
  length  = 32
  special = false
}

# MSK SCRAM secrets must be named AmazonMSK_... and encrypted with a CMK.
resource "aws_secretsmanager_secret" "kafka" {
  name                    = "AmazonMSK_${local.name}"
  kms_key_id              = aws_kms_key.msk_secrets.key_id
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "kafka" {
  secret_id     = aws_secretsmanager_secret.kafka.id
  secret_string = jsonencode({ username = var.project, password = random_password.kafka.result })
}

resource "aws_msk_scram_secret_association" "main" {
  cluster_arn     = aws_msk_cluster.main.arn
  secret_arn_list = [aws_secretsmanager_secret.kafka.arn]
  depends_on      = [aws_secretsmanager_secret_version.kafka]
}
