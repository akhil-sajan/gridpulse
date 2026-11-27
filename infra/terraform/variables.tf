variable "landing_bucket" {
  description = "Landing-zone bucket behind the Unity Catalog external volumes (P0.6)"
  type        = string
  default     = "gridpulse-landing-cybtgm"
}

variable "state_bucket" {
  description = "Bucket holding this configuration's state (bootstrap/)"
  type        = string
  default     = "gridpulse-tfstate-cybtgm"
}

variable "databricks_role_name" {
  description = "IAM role Databricks assumes to read and write the landing bucket"
  type        = string
  default     = "gridpulse-databricks-landing"
}

variable "terraform_user_name" {
  description = "IAM user that runs Terraform (AWS CLI profile gridpulse)"
  type        = string
  default     = "gridpulse-terraform"
}

variable "databricks_uc_role_arn" {
  description = "Databricks' Unity Catalog master role that may assume our role (from the storage credential page)"
  type        = string
}

variable "databricks_external_id" {
  description = "External ID of the Databricks storage credential gridpulse_landing_cred"
  type        = string
}

variable "budget_email" {
  description = "Address that receives the budget alerts"
  type        = string
}

variable "budget_limit_usd" {
  description = "Monthly cost budget"
  type        = string
  default     = "5.0"
}
