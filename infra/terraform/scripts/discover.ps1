# P5.6: read the AWS resources that were created by hand (P0.2, P0.6), so Terraform can
# import them unchanged. Read-only, except one small inline policy that lets the
# gridpulse-terraform user read and edit budgets (it has only S3 and IAM rights today).
#
#   cd C:\Users\akhil\gridpulse
#   powershell -ExecutionPolicy Bypass -File infra\terraform\scripts\discover.ps1
#
# Prints JSON to paste back. No secrets in it (account ID, names, policies, budget email).

$ErrorActionPreference = "Stop"
$env:AWS_PROFILE = "gridpulse"
$role = "gridpulse-databricks-landing"
$bucket = "gridpulse-landing-cybtgm"
$user = "gridpulse-terraform"

# Budgets rights for the Terraform user (narrowed again by Terraform itself later).
$budgetPolicy = @'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["budgets:ViewBudget", "budgets:ModifyBudget", "budgets:DescribeBudgetActionsForBudget",
               "budgets:ListTagsForResource", "budgets:TagResource", "budgets:UntagResource"],
    "Resource": "*"
  }]
}
'@
$policyFile = Join-Path $env:TEMP "gridpulse-budgets-policy.json"
[System.IO.File]::WriteAllText($policyFile, $budgetPolicy)
aws iam put-user-policy --user-name $user --policy-name gridpulse-budgets-bootstrap `
    --policy-document "file://$policyFile" | Out-Null
Start-Sleep -Seconds 10   # IAM changes take a few seconds to apply

$account = aws sts get-caller-identity --query Account --output text
$rolePolicies = (aws iam list-role-policies --role-name $role | ConvertFrom-Json).PolicyNames
$result = [ordered]@{
    account_id             = $account
    caller                 = (aws sts get-caller-identity --query Arn --output text)
    role_trust             = (aws iam get-role --role-name $role --query Role.AssumeRolePolicyDocument | ConvertFrom-Json)
    role_max_session       = (aws iam get-role --role-name $role --query Role.MaxSessionDuration --output text)
    role_description       = (aws iam get-role --role-name $role --query Role.Description --output text)
    role_inline_policies   = @{}
    role_attached_policies = (aws iam list-attached-role-policies --role-name $role | ConvertFrom-Json).AttachedPolicies
    user_attached_policies = (aws iam list-attached-user-policies --user-name $user | ConvertFrom-Json).AttachedPolicies
    user_inline_policies   = (aws iam list-user-policies --user-name $user | ConvertFrom-Json).PolicyNames
    bucket_versioning      = (aws s3api get-bucket-versioning --bucket $bucket | ConvertFrom-Json)
    bucket_encryption      = (aws s3api get-bucket-encryption --bucket $bucket | ConvertFrom-Json)
    bucket_public_access   = (aws s3api get-public-access-block --bucket $bucket | ConvertFrom-Json)
    bucket_tags            = $null
    budgets                = (aws budgets describe-budgets --account-id $account | ConvertFrom-Json).Budgets
    budget_notifications   = @{}
}
foreach ($p in $rolePolicies) {
    $result.role_inline_policies[$p] = (aws iam get-role-policy --role-name $role --policy-name $p --query PolicyDocument | ConvertFrom-Json)
}
try { $result.bucket_tags = (aws s3api get-bucket-tagging --bucket $bucket 2>$null | ConvertFrom-Json).TagSet } catch { }
foreach ($b in $result.budgets) {
    $n = aws budgets describe-notifications-for-budget --account-id $account --budget-name $b.BudgetName | ConvertFrom-Json
    $list = @()
    foreach ($x in $n.Notifications) {
        # The API leaves ThresholdType out when it is the default (PERCENTAGE).
        if (-not $x.ThresholdType) { $x | Add-Member -NotePropertyName ThresholdType -NotePropertyValue "PERCENTAGE" -Force }
        $subs = aws budgets describe-subscribers-for-notification --account-id $account `
            --budget-name $b.BudgetName --notification ("NotificationType=" + $x.NotificationType + ",ComparisonOperator=" + $x.ComparisonOperator + ",Threshold=" + $x.Threshold + ",ThresholdType=" + $x.ThresholdType) | ConvertFrom-Json
        $list += [ordered]@{ notification = $x; subscribers = $subs.Subscribers }
    }
    $result.budget_notifications[$b.BudgetName] = $list
}
$result | ConvertTo-Json -Depth 12
