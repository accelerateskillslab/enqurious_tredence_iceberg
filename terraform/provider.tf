provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}

locals {
  common_tags = {
    Program      = "iceberg-masterclass"
    Environment  = "lab"
    ManagedBy    = "terraform"
    AutoShutdown = "true"
  }
}
