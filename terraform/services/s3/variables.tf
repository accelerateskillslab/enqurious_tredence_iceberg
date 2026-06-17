variable "bucket_suffix" {
  description = "Unique suffix used for the test S3 bucket name."
  type        = string
}

variable "common_tags" {
  description = "Common tags applied to all resources."
  type        = map(string)
}
