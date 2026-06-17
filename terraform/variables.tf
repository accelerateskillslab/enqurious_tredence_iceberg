variable "aws_region" {
  description = "AWS region for the lab workload."
  type        = string
  default     = "us-east-1"
}

variable "bucket_suffix" {
  description = "Unique suffix used for the test S3 bucket name."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,46}[a-z0-9]$", var.bucket_suffix))
    error_message = "bucket_suffix must be 3-48 characters, lowercase letters, numbers, or hyphens, and must start and end with a letter or number."
  }
}
