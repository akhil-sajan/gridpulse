# ----------------------------------------------------------------- Databricks access role
# Unity Catalog's storage credential gridpulse_landing_cred assumes this role to read and
# write the landing bucket (P0.6). Trust: Databricks' UC master role with our external ID,
# plus the role itself (Databricks requires self-assume).

resource "aws_iam_role" "databricks_landing" {
  name                 = var.databricks_role_name
  max_session_duration = 3600
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        AWS = [
          var.databricks_uc_role_arn,
          "arn:aws:iam::${local.account_id}:role/${var.databricks_role_name}",
        ]
      }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "sts:ExternalId" = var.databricks_external_id } }
    }]
  })
}

# Least privilege: the same actions as before, but bucket-level actions only on the
# bucket and object actions only on its objects (no other bucket, no admin actions).
resource "aws_iam_role_policy" "databricks_landing_s3" {
  name = "gridpulse-landing-s3"
  role = aws_iam_role.databricks_landing.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListLandingBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketMultipartUploads",
        ]
        Resource = aws_s3_bucket.landing.arn
      },
      {
        Sid    = "ReadWriteLandingObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload",
        ]
        Resource = "${aws_s3_bucket.landing.arn}/*"
      },
      {
        Sid      = "SelfAssume"
        Effect   = "Allow"
        Action   = "sts:AssumeRole"
        Resource = aws_iam_role.databricks_landing.arn
      },
    ]
  })
}

# ----------------------------------------------------------------- Terraform's own user
# Replaces the broad AmazonS3FullAccess + IAMFullAccess from P0.2 with exactly what this
# configuration needs. After the first apply the broad policies are detached by hand
# (see README); a clean `terraform plan` afterwards proves these rights are enough.

data "aws_iam_user" "terraform" {
  user_name = var.terraform_user_name
}

resource "aws_iam_user_policy" "terraform_least_privilege" {
  name = "gridpulse-terraform-least-privilege"
  user = data.aws_iam_user.terraform.user_name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ManageGridpulseBucketsConfig"
        Effect = "Allow"
        Action = [
          "s3:Get*",
          "s3:List*",
          "s3:PutBucket*",
          "s3:PutLifecycleConfiguration",
          "s3:PutEncryptionConfiguration",
        ]
        Resource = [
          aws_s3_bucket.landing.arn,
          "arn:aws:s3:::${var.state_bucket}",
        ]
      },
      {
        # Terraform's state file and its lock file. No access to landing *objects*.
        Sid      = "TerraformState"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "arn:aws:s3:::${var.state_bucket}/gridpulse/*"
      },
      {
        Sid    = "ManageDatabricksRole"
        Effect = "Allow"
        Action = [
          "iam:GetRole",
          "iam:UpdateRole",
          "iam:UpdateAssumeRolePolicy",
          "iam:TagRole",
          "iam:UntagRole",
          "iam:ListRoleTags",
          "iam:GetRolePolicy",
          "iam:PutRolePolicy",
          "iam:DeleteRolePolicy",
          "iam:ListRolePolicies",
          "iam:ListAttachedRolePolicies",
          "iam:ListInstanceProfilesForRole",
        ]
        Resource = aws_iam_role.databricks_landing.arn
      },
      {
        # Only its own inline policies: it can't attach AWS managed policies (no
        # iam:AttachUserPolicy), so it can't give itself admin rights again.
        Sid    = "ManageOwnInlinePolicy"
        Effect = "Allow"
        Action = [
          "iam:GetUser",
          "iam:ListUserTags",
          "iam:GetUserPolicy",
          "iam:PutUserPolicy",
          "iam:DeleteUserPolicy",
          "iam:ListUserPolicies",
          "iam:ListAttachedUserPolicies",
          "iam:DetachUserPolicy",
        ]
        Resource = data.aws_iam_user.terraform.arn
      },
      {
        Sid    = "ManageBudget"
        Effect = "Allow"
        Action = [
          "budgets:ViewBudget",
          "budgets:ModifyBudget",
          "budgets:DescribeBudgetActionsForBudget",
          "budgets:ListTagsForResource",
          "budgets:TagResource",
          "budgets:UntagResource",
        ]
        Resource = "arn:aws:budgets::${local.account_id}:budget/GridPulseBudget"
      },
    ]
  })
}
