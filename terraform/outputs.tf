output "test_bucket_name" {
  description = "Name of the test S3 bucket."
  value       = module.test_s3_bucket.bucket_name
}

output "test_bucket_arn" {
  description = "ARN of the test S3 bucket."
  value       = module.test_s3_bucket.bucket_arn
}
