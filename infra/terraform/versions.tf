# P5.6: the AWS side of GridPulse as code. Everything here existed before (created by
# hand in P0.2 and P0.6) and is taken over with `import` blocks (imports.tf), so the
# first apply changes settings, never recreates anything.
#
#   $env:AWS_PROFILE = "gridpulse"
#   cd infra/terraform; terraform init; terraform plan; terraform apply

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Remote state in its own bucket (created by bootstrap/). use_lockfile: S3-native
  # locking (Terraform >= 1.10), so no DynamoDB table is needed.
  backend "s3" {
    bucket       = "gridpulse-tfstate-cybtgm"
    key          = "gridpulse/aws.tfstate"
    region       = "eu-central-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = "eu-central-1"
  default_tags {
    tags = { project = "gridpulse", managed_by = "terraform" }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
}
