terraform {
  backend "local" {}
  required_version = "~> 0.12"
}

provider "aws" {
  region = var.aws_region
}

locals {
  account_id = data.aws_caller_identity.current.account_id
}
