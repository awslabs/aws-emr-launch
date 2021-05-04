variable "aws_region" {}

variable "stage" {}

data "aws_caller_identity" "current" {}

variable "emr_cluster_name" {
  description = "Unique name for the EMR Cluster's running this batch job"
  type = string
}

variable "vpc_id" {
  description = "VPC to deploy EMR cluster into"
  type = string
}

variable "subnet_id" {
  description = "Subnet Id to deploy EMR cluster into"
  type = string
}

variable "master_instance_type" {
  type = string
}

variable "core_instance_type" {
  type = string
}

variable "core_instance_market" {
  description = "ON_DEMAND or SPOT"
  type = string
  default = "ON_DEMAND"
}

variable "core_instance_count" {
  type = number
}

variable "release_label" {
  type = string
}

variable "log_bucket" {
  description = "The S3 bucket name EMR will write logs to"
}

variable "artifact_bucket" {
  description = "The S3 bucket name EMR will read Artifacts from"
}

variable "input_buckets" {
  description = "A list of S3 Bucket names that EMR will read data from"
  type = list(string)
}

variable "output_buckets" {
  description = "A list of S3 Bucket names that EMR will write data to"
  type = list(string)
}

variable "input_kms_arns" {
  description = "A list of KMS Key ARNs used for decrypting data being read into the cluster"
  type = list(string)
  default = []
}

variable "output_kms_arns" {
  description = "A list of KMS Key ARNs used for encrypting data being written by the cluster"
  type = list(string)
  default = []
}

variable "applications" {
  description = "Applications to install on EMR"
  default = ["Hadoop", "Hive", "Spark", "Ganglia"]
  type = list(string)
}

variable "configuration" {
  default = [
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            }
        }
    ]
}

variable "task_instance_type" {
  default = "m5.xlarge"
}

variable "task_instance_count" {
  default = 0
  type = number
}

variable "task_instance_market" {
  description = "ON_DEMAND or SPOT"
  type = string
  default = "SPOT"
}

variable "core_instance_ebs_size" {
  description = "GB of Core EBS Storage"
  default = 0
  type = number
}

variable "core_instance_ebs_type" {
  description = "should be one of 'gp2', 'io1', 'io2', 'st1', 'sc1'"
  default = "io1"
  type = string
}

variable "core_instance_ebs_iops" {
  description = "should be one of 'gp2', 'io1', 'io2', 'st1', 'sc1'"
  default = 10000
  type = number
}

variable "task_instance_ebs_size" {
  description = "EBS Volume size to add to task nodes, used when spark spills or cachces to disk - recommended 64-500GB"
  default = 64
  type = number
}

