terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.60"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.7"
    }
  }

  # Remote state for team use (create the bucket/table once, then uncomment):
  # backend "s3" {
  #   bucket       = "fraud-platform-tfstate"
  #   key          = "fraud-platform/terraform.tfstate"
  #   region       = "eu-central-1"
  #   use_lockfile = true
  # }
}
