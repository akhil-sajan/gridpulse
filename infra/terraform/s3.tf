# Landing zone: raw files from the ingesters, read by Databricks through Unity Catalog
# external volumes (/Volumes/<catalog>/bronze/landing -> s3://<bucket>/<env>/).

resource "aws_s3_bucket" "landing" {
  bucket = var.landing_bucket
  lifecycle {
    prevent_destroy = true
  }
}

# Versioning: an overwritten or deleted raw file can be recovered (old versions expire
# after 30 days, see the lifecycle rules).
resource "aws_s3_bucket_versioning" "landing" {
  bucket = aws_s3_bucket.landing.id
  versioning_configuration {
    status = "Enabled"
  }
}

# SSE-S3 (AES256), already the bucket's setting. KMS would add cost for no gain here.
resource "aws_s3_bucket_server_side_encryption_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = false
  }
}

resource "aws_s3_bucket_public_access_block" "landing" {
  bucket                  = aws_s3_bucket.landing.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id

  # Objects under 128 KB never move to a cheaper class: Standard-IA bills every object as
  # at least 128 KB, so for the many small SMARD/weather files it would cost more, not
  # less. (This is also S3's default; it is stated here on purpose.)
  transition_default_minimum_object_size = "all_storage_classes_128K"

  # Raw files are read once by Auto Loader, then only for reprocessing: after 30 days the
  # larger ones (monthly weather, reference CSVs) move to Standard-IA (about 45% cheaper).
  rule {
    id     = "raw-files-to-standard-ia-after-30-days"
    status = "Enabled"
    filter {}
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }

  # Versioning housekeeping: old versions go after 30 days, failed uploads after 7.
  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
    expiration {
      expired_object_delete_marker = true
    }
  }

  depends_on = [aws_s3_bucket_versioning.landing]
}
