
resource "aws_s3_bucket" "demo_bucket" {
  bucket = "${var.emr_cluster_name}-emr-demo-bucket"
  acl = "private"
  force_destroy = true # Set this to false if this is not for testing purposes
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "aws:kms"
      }
    }
  }
}

resource "aws_s3_bucket" "artifact_bucket" {
  bucket = "${var.emr_cluster_name}-emr-demo-artifact-bucket"
  acl = "private"
  force_destroy = true # Set this to false if this is not for testing purposes
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "aws:kms"
      }
    }
  }
}

resource "aws_s3_bucket" "log_bucket" {
  bucket = "${var.emr_cluster_name}-emr-demo-log-bucket"
  acl = "private"
  force_destroy = true # Set this to false if this is not for testing purposes
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "aws:kms"
      }
    }
  }
}



