# Monthly cost budget with e-mail alerts (P0.2): actual spend above 85 % and 100 %, and a
# forecast above 100 % of the limit.

locals {
  budget_alerts = [
    { type = "ACTUAL", threshold = 85 },
    { type = "ACTUAL", threshold = 100 },
    { type = "FORECASTED", threshold = 100 },
  ]
}

resource "aws_budgets_budget" "monthly" {
  name              = "GridPulseBudget"
  budget_type       = "COST"
  limit_amount      = var.budget_limit_usd
  limit_unit        = "USD"
  time_unit         = "MONTHLY"
  time_period_start = "2026-11-01_00:00"

  dynamic "notification" {
    for_each = local.budget_alerts
    content {
      notification_type          = notification.value.type
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value.threshold
      threshold_type             = "PERCENTAGE"
      subscriber_email_addresses = [var.budget_email]
    }
  }
}
