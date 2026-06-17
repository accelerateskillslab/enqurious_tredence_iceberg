output "bucket_name" {
  description = "Name of the test S3 bucket."
  value       = aws_s3_bucket.test.bucket
}

output "bucket_arn" {
  description = "ARN of the test S3 bucket."
  value       = aws_s3_bucket.test.arn
}
