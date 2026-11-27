# GridPulse AWS infrastructure (Terraform, P5.6)

What it manages (all in eu-central-1, account of the `gridpulse` CLI profile):

| resource | what it is |
|---|---|
| `gridpulse-landing-cybtgm` | landing bucket behind the Unity Catalog external volumes: versioning, SSE-S3, public access blocked, lifecycle (files ≥ 128 KB to Standard-IA after 30 days, old versions expire after 30 days) |
| `gridpulse-databricks-landing` | IAM role Databricks assumes; S3 access to the landing bucket only |
| `gridpulse-terraform-least-privilege` | inline policy of the Terraform user: only what this configuration needs |
| `GridPulseBudget` | $5/month cost budget, e-mail at 85 % / 100 % actual and 100 % forecast |
| `gridpulse-tfstate-cybtgm` | remote state (created by `bootstrap/`, S3-native locking) |

Everything except the state bucket existed before and is taken over with `import` blocks.

## First run (Windows PowerShell, from the repo root)

```powershell
$env:AWS_PROFILE = "gridpulse"

# 1. State bucket (local state, once)
cd infra\terraform\bootstrap
terraform init
terraform apply

# 2. Main configuration
cd ..
copy terraform.tfvars.example terraform.tfvars   # then fill in the three values
terraform init
terraform plan        # imports + intended changes only, no "destroy"
terraform apply

# 3. Drop the broad P0.2 policies; the narrow one is now in place
aws iam detach-user-policy --user-name gridpulse-terraform --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam detach-user-policy --user-name gridpulse-terraform --policy-arn arn:aws:iam::aws:policy/IAMFullAccess
aws iam delete-user-policy --user-name gridpulse-terraform --policy-name gridpulse-budgets-bootstrap

# 4. Proof: with only the narrow rights, nothing differs
terraform plan        # -> "No changes. Your infrastructure matches the configuration."
```

If the narrow policy ever lacks a permission, sign in as root and re-attach a broad
policy temporarily, add the missing action in `iam.tf`, apply, detach again.
