module "test_s3_bucket" {
  source = "./services/s3"

  bucket_suffix = var.bucket_suffix
  common_tags   = local.common_tags
}
