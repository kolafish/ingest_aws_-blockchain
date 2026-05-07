terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  required_version = ">= 1.3.0"
}

provider "aws" {
  profile = var.aws_profile
  region  = var.region

  default_tags {
    tags = {
      Usage = var.namespace
    }
  }
}
