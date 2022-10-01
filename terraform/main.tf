terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "4.22.0"
    }
  }
  required_version = "1.3.1"

  cloud {
    organization = "tnorlund"

    workspaces {
      name = "quest"
    }
  }
}

variable "aws_region" {
  type        = string
  description = "The AWS region"
  default     = "us-west-1"
}

variable "developer" {
  type        = string
  description = "The name of the person adding the infra"
  default     = "Tyler"
}

/**
 * The AWS provider should be handled by ENV vars. 
 */
provider "aws" {
  region = "us-west-1"
  # region = var.aws_region
  # access_key = var.AWS_ACCESS_KEY_ID
  # secret_key = var.AWS_SECRET_ACCESS_KEY
}

# Create an S3 Bucket to store the Kinesis data
resource "aws_s3_bucket" "bucket" {
  bucket = "quest_data"
  tags = {
    Project   = "quest"
    Developer = var.developer
  }
}
