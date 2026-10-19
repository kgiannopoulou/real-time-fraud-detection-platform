resource "aws_cloudwatch_log_group" "ecs" {
  for_each          = local.log_group
  name              = each.value
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/msk/${local.name}"
  retention_in_days = 14
}

# Prometheus-compatible metrics store; the ADOT sidecars in compute.tf
# remote-write the Spark and API metrics here, and the alert rules from
# monitoring/alerts.yml are loaded as a rule group.
resource "aws_prometheus_workspace" "main" {
  alias = local.name
}

resource "aws_prometheus_rule_group_namespace" "alerts" {
  name         = "fraud-platform"
  workspace_id = aws_prometheus_workspace.main.id
  data         = file("${path.module}/../monitoring/alerts.yml")
}
