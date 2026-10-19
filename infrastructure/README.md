# Infrastructure (Terraform, AWS)

Infrastructure as code for the cloud deployment described in
[docs/cloud-deployment.md](../docs/cloud-deployment.md).

| File | Resources |
| --- | --- |
| `versions.tf` | Terraform >= 1.9, AWS provider 6.x, optional S3 remote state |
| `main.tf` | Provider with default tags, AZ lookup, naming |
| `network.tf` | VPC, public/private subnets per AZ, NAT gateway, S3 gateway endpoint, least-privilege security groups (ALB → tasks, tasks → MSK :9096, tasks → RDS :5432) |
| `storage.tf` | S3 lake bucket (`lake/`, `checkpoints/`, `mlflow/`): encrypted, versioned, public access blocked, IA tiering; ECR repositories (scan on push, immutable tags, lifecycle) |
| `kafka.tf` | Amazon MSK across AZs: SASL/SCRAM + TLS, cluster configuration (no topic auto-creation, replication), SCRAM secret encrypted with a CMK |
| `database.tf` | RDS PostgreSQL 16 (gp3, encrypted, backups, Performance Insights), credentials + ready-made DSNs in Secrets Manager |
| `compute.tf` | ECS Fargate cluster, task definitions and services for Spark streaming, API, dashboard, MLflow (Cloud Map DNS), producer (scaled to 0); ALB (dashboard :80, API :8000); IAM execution/task roles; weekly retraining as an EventBridge-scheduled ECS task |
| `monitoring.tf` | CloudWatch log groups, Amazon Managed Service for Prometheus workspace with the alert rules from `monitoring/alerts.yml`; ADOT sidecars remote-write Spark/API metrics |
| `outputs.tf` | Dashboard/API URLs, MSK brokers, RDS endpoint, bucket, ECR URLs, secrets |

Notable choices:

* **Configuration, not code, changes between local and cloud.** Containers
  get MSK SASL settings, `s3a://` lake paths, the RDS DSN and the MLflow URL
  from environment variables and Secrets Manager references.
* **The Spark service never runs twice** (`minimum_healthy_percent = 0`,
  `maximum_percent = 100`): two drivers on the same checkpoints would corrupt
  streaming state.
* **Retraining without MWAA** for cost: `python -m ml.retrain pipeline` runs
  every DAG step in one scheduled Fargate task. The Airflow DAG still works
  unchanged on MWAA if an orchestrator UI is wanted.
* **No static AWS keys**: tasks use IAM roles; S3 access goes through the
  task role (hadoop-aws container credentials).

## Usage

```bash
cd infrastructure
cp terraform.tfvars.example terraform.tfvars   # adjust region, sizes, ingress CIDR
terraform init
terraform plan
terraform apply
terraform destroy                               # everything is reproducible
```

CI runs `terraform fmt -check`, `terraform init -backend=false` and
`terraform validate` on every push.
