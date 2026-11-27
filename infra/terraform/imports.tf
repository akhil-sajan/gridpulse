# Existing resources taken over by Terraform (created by hand in P0.2 / P0.6).
# After the first apply these blocks do nothing; they stay as a record of the import.

import {
  to = aws_s3_bucket.landing
  id = var.landing_bucket
}

import {
  to = aws_s3_bucket_server_side_encryption_configuration.landing
  id = var.landing_bucket
}

import {
  to = aws_s3_bucket_public_access_block.landing
  id = var.landing_bucket
}

import {
  to = aws_iam_role.databricks_landing
  id = var.databricks_role_name
}

import {
  to = aws_iam_role_policy.databricks_landing_s3
  id = "${var.databricks_role_name}:gridpulse-landing-s3"
}

import {
  to = aws_budgets_budget.monthly
  id = "${local.account_id}:GridPulseBudget"
}
