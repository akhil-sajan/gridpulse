# Step 1 of P5.6: the bucket that stores Terraform's state for infra/terraform.
# It can't hold its own state, so this small config keeps a local state file
# (bootstrap/terraform.tfstate, git-ignored). Run once:
#   cd infra/terraform/bootstrap; terraform init; terraform apply

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = "eu-central-1"
  default_tags {
    tags = { project = "gridpulse", managed_by = "terraform-bootstrap" }
  }
}

resource "aws_s3_bucket" "state" {
  bucket = "gridpulse-tfstate-cybtgm"
  # The state is the only record of what Terraform manages: never delete it by accident.
  lifecycle {
    prevent_destroy = true
  }
}

# Every state write keeps the previous version, so a broken state can be rolled back.
resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Old state versions are tiny, but don't keep them forever.
resource "aws_s3_bucket_lifecycle_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    id     = "expire-old-state-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days           = 90
      newer_noncurrent_versions = 10
    }
  }
}

output "state_bucket" {
  value = aws_s3_bucket.state.bucket
}
