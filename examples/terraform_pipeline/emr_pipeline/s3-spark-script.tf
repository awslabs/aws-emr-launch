
resource "aws_s3_bucket_object" "emr_pyspark_script" {
  bucket = aws_s3_bucket.artifact_bucket.bucket
  key = "emr_jars/${var.spark_script}"
  source = "./${var.spark_script}"
  etag = filemd5("./${var.spark_script}")
  tags = {
    env = var.environment
    region = var.aws_region
  }
}