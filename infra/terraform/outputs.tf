output "landing_bucket_arn" {
  value = aws_s3_bucket.landing.arn
}

output "databricks_role_arn" {
  description = "Role ARN used by the Databricks storage credential gridpulse_landing_cred"
  value       = aws_iam_role.databricks_landing.arn
}
