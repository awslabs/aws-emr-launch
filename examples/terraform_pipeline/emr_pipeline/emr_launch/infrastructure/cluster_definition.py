
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_kms as kms
from aws_cdk import core
from aws_cdk.aws_ec2 import Subnet

from aws_emr_launch.constructs.emr_constructs import emr_profile
from instance_group_config import TaskInstanceGroupConfiguration
from aws_emr_launch.constructs.step_functions import emr_launch_function
from aws_emr_launch.constructs.emr_constructs.cluster_configuration import InstanceMarketType


class EMRClusterDefinition(core.Stack):
    """
    Stack to define a Standard EMR Cluster Configuration:
    """

    def __init__(self, scope: core.Construct, id: str, config: dict, **kwargs):
        super().__init__(scope, id, **kwargs)

        for k,v in config.items():
            setattr(self, k, v)

        vpc = ec2.Vpc.from_lookup(
            self,
            'Vpc',
            vpc_id=self.VPC_ID,
        )

        self._cluster_name = self.CLUSTER_NAME

        self._emr_profile = self.init_emr_profile(
            vpc=vpc,
            log_bucket_arn=self.LOG_BUCKET,
            artifact_bucket_arn=self.ARTIFACT_BUCKET,
            input_kms_arns=self.INPUT_KMS_ARNS,
            output_kms_arns=self.OUTPUT_KMS_ARNS
        )

        self.authorize_buckets(
            input_data_bucket_arns=self.INPUT_BUCKETS,
            output_data_bucket_arns=self.OUTPUT_BUCKETS,
        )

        self._cluster_configuration = self.emr_resource_config(
            subnet_id=self.SUBNET_ID,
            master_instance_type=self.MASTER_INSTANCE_TYPE,
            core_instance_type=self.CORE_INSTANCE_TYPE,
            core_instance_count=self.CORE_INSTANCE_COUNT,
            release_label=self.RELEASE_LABEL,
            applications=self.APPLICATIONS,
            configuration=self.CONFIGURATION,
            core_instance_market=self.CORE_INSTANCE_MARKET,
            task_instance_type=self.TASK_INSTANCE_TYPE,
            task_instance_market=self.TASK_INSTANCE_MARKET,
            task_instance_count=self.TASK_INSTANCE_COUNT,
            core_instance_ebs_size=self.CORE_INSTANCE_EBS_SIZE,
            core_instance_ebs_type=self.CORE_INSTANCE_EBS_TYPE,
            core_instance_ebs_iops=self.CORE_INSTANCE_EBS_IOPS,
            task_instance_ebs_size=self.TASK_INSTANCE_EBS_SIZE,
        )

        self._launch_function = self.launch_function_config(
            emr_profile=self.emr_profile,
            cluster_configuration=self.cluster_configuration,
            default_fail_if_cluster_running=True
        )

        self.outputs()

    def outputs(self):
        """
        Extend the values here to add additional outputs of the module
        :return:
        """
        core.CfnOutput(
            self, "LaunchFunctionARN",
            value=self.launch_function_arn,
        )

        core.CfnOutput(
            self, "InstanceRoleName",
            value=self.instance_role_name,
        )

    def emr_resource_config(self, subnet_id, master_instance_type: str, core_instance_type: str,
                 core_instance_count: int, release_label: str, applications: [str], configuration: dict,
                 core_instance_market: str, task_instance_type: str, task_instance_market: str,
                 task_instance_count: int, core_instance_ebs_size: int, core_instance_ebs_type: str,
                 core_instance_ebs_iops: int, task_instance_ebs_size: int):

        subnet = Subnet.from_subnet_id(self, 'emr-subnet', subnet_id=subnet_id)

        default_configurations = configuration

        assert core_instance_ebs_type in ['gp2', 'io1', 'io2', 'st1', 'sc1'], \
            f"{core_instance_ebs_type} should be one of 'gp2', 'io1', 'io2', 'st1', 'sc1'"

        if core_instance_market == 'SPOT':
            core_market = InstanceMarketType.SPOT
        elif core_instance_market == 'ON_DEMAND':
            core_market = InstanceMarketType.ON_DEMAND
        else:
            raise Exception(f"{core_instance_market} must be one of 'SPOT' or 'ON_DEMAND'")

        if task_instance_market == 'SPOT':
            task_market = InstanceMarketType.SPOT
        elif task_instance_market == 'ON_DEMAND':
            task_market = InstanceMarketType.ON_DEMAND
        else:
            raise Exception(f"{task_instance_market} must be one of 'SPOT' or 'ON_DEMAND'")

        return TaskInstanceGroupConfiguration(
            self, 'ClusterResourceConfiguration',
            configuration_name=self._cluster_name + '-resources',
            subnet=subnet,
            master_instance_type=master_instance_type,
            core_instance_type=core_instance_type,
            core_instance_count=core_instance_count,
            release_label=release_label,
            configurations=default_configurations,
            applications=applications,
            core_instance_market=core_market,
            task_instance_type=task_instance_type,
            task_instance_market=task_market,
            task_instance_count=task_instance_count,
            core_instance_ebs_size=core_instance_ebs_size,
            core_instance_ebs_type=core_instance_ebs_type,
            core_instance_ebs_iops=core_instance_ebs_iops,
            task_instance_ebs_size=task_instance_ebs_size
        )

    def init_emr_profile(self, vpc, log_bucket_arn, artifact_bucket_arn, input_kms_arns, output_kms_arns):
        # Logs and Artifacts
        log_bucket = s3.Bucket.from_bucket_arn(
            self, 'LogBucket', bucket_arn=log_bucket_arn
        )

        artifact_bucket = s3.Bucket.from_bucket_arn(
            self, 'ArtifactBucket', bucket_arn=artifact_bucket_arn
        )

        profile = self.security_profile_config(
            vpc=vpc,
            logs_bucket=log_bucket,
            artifacts_bucket=artifact_bucket,
            input_kms_keys=input_kms_arns,
            output_kms_keys=output_kms_arns
        )

        return profile

    def authorize_buckets(self, input_data_bucket_arns, output_data_bucket_arns):

        input_data_buckets = {
            b_name: s3.Bucket.from_bucket_arn(
                self, f'In-{b_name}', bucket_arn=b_name
            ) for b_name in input_data_bucket_arns
        }

        output_data_buckets = {
            b_name: s3.Bucket.from_bucket_arn(
                self, f'Out-{b_name}', bucket_arn=b_name
            ) for b_name in output_data_bucket_arns
        }

        for b_name, bucket in input_data_buckets.items():
            self._emr_profile.authorize_input_bucket(bucket)

        for b_name, bucket in output_data_buckets.items():
            self._emr_profile.authorize_output_bucket(bucket)

    def security_profile_config(self, vpc, logs_bucket, artifacts_bucket,
                                input_kms_keys=None, output_kms_keys=None):

        profile_name = self._cluster_name + '-security'

        profile = emr_profile.EMRProfile(
            self, profile_name,
            profile_name=profile_name,
            vpc=vpc,
            logs_bucket=logs_bucket,
            artifacts_bucket=artifacts_bucket,
        )

        if input_kms_keys:
            for i, k in enumerate(input_kms_keys):
                kms_key = kms.Key.from_key_arn(self, id=f'{profile_name}_input_key_{i}', key_arn=k)
                profile.authorize_input_key(kms_key)
        if output_kms_keys:
            for i, k in enumerate(output_kms_keys):
                kms_key = kms.Key.from_key_arn(self, id=f'{profile_name}_output_key_{i}', key_arn=k)
                profile.authorize_output_key(kms_key)

        profile.security_groups.service_group.add_ingress_rule(profile.security_groups.master_group, ec2.Port.tcp(9443))

        return profile

    def launch_function_config(self, emr_profile, cluster_configuration, default_fail_if_cluster_running):

        return emr_launch_function.EMRLaunchFunction(
            self,
            self._cluster_name,
            namespace=self._cluster_name,
            launch_function_name="launch-fn",
            emr_profile=emr_profile,
            cluster_configuration=cluster_configuration,
            cluster_name=self._cluster_name,
            default_fail_if_cluster_running=default_fail_if_cluster_running,
            allowed_cluster_config_overrides=cluster_configuration.override_interfaces['default'],
            cluster_tags=[
                core.Tag(key='Group', value='AWSDemo')
            ]
        )

    @property
    def emr_profile(self) -> emr_profile.EMRProfile:
        return self._emr_profile

    @property
    def cluster_configuration(self) -> TaskInstanceGroupConfiguration:
        return self._cluster_configuration

    @property
    def launch_function(self) -> emr_launch_function.EMRLaunchFunction:
        return self._launch_function

    @property
    def launch_function_arn(self) -> str:
        return self._launch_function.state_machine.state_machine_arn

    @property
    def instance_role_name(self) -> str:
        return self._emr_profile._roles.instance_role.role_name

