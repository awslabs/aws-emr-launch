
aws_region = "eu-west-1"
stage = "dev"

vpc_id = "vpc-0ad86fc2cfdd36384"
subnet_id = "subnet-01df671b3ac7cdf95"
emr_cluster_name = "knowledgemine"


# EMR Configuration
master_instance_type = "m5.xlarge"

core_instance_type = "m5d.xlarge"
core_instance_count = 2
core_instance_market = "ON_DEMAND"
core_instance_ebs_size = 0

task_instance_type = "m5.xlarge"
task_instance_count = 2
task_instance_market = "SPOT"
task_instance_ebs_size = 64

release_label = "emr-5.30.1"

applications = ["Hadoop", "Spark", "Ganglia"]

configuration = [
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            }
        }
    ]
