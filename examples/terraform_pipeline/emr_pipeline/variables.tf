
data "aws_caller_identity" "current" {}

variable "environment" {
  default = "dev"
}

variable "spark_script" {
  description = "Name of the local pyspark file to upload and run on EMR"
  default = "spark_script.py"
}

variable "aws_region" {}

variable "stage" {}

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

