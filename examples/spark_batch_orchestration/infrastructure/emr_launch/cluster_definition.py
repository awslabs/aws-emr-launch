from os import listdir

import aws_cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_kms as kms
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3d

from aws_emr_launch.constructs.emr_constructs import emr_code, emr_profile
from aws_emr_launch.constructs.emr_constructs.cluster_configuration import InstanceMarketType
from aws_emr_launch.constructs.step_functions import emr_launch_function

from .instance_group_config import TaskInstanceGroupConfiguration


class EMRClusterDefinition(aws_cdk.Stack):
    """
    Stack to define a Standard EMR Cluster Configuration:
    """

    def __init__(self, scope: aws_cdk.Construct, id: str, config: dict, **kwargs):
        super().__init__(scope, id, **kwargs)

        for k, v in config.items():
            setattr(self, k, v)

        vpc = ec2.Vpc(self, "MyEMRVpc", max_azs=3)

        subnet = vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE).subnets[0]

        self._cluster_name = self.CLUSTER_NAME

        log_bucket = s3.Bucket(self, "emr_log_bucket", encryption=s3.BucketEncryption.S3_MANAGED)
        artifact_bucket = s3.Bucket(self, "emr_artifact_bucket", encryption=s3.BucketEncryption.S3_MANAGED)
        self.output_bucket = s3.Bucket(self, "emr_output_bucket", encryption=s3.BucketEncryption.S3_MANAGED)

        self._emr_profile = self.init_emr_profile(
            vpc=vpc,
            log_bucket=log_bucket,
            artifact_bucket=artifact_bucket,
        )

        self.artifact_bucket = artifact_bucket

        self.authorize_buckets(
            input_data_bucket_arns=self.INPUT_BUCKETS,
            output_data_bucket_arns=[self.output_bucket.bucket_arn],
        )

        s3d.BucketDeployment(
            self,
            id="bootstrap_actions",
            destination_bucket=artifact_bucket,
            destination_key_prefix="bootstrap_actions",
            sources=[s3d.Source.asset("infrastructure/emr_launch/bootstrap_actions/")],
        )

        bootstrap_actions = []
        for f in listdir("infrastructure/emr_launch/bootstrap_actions/"):
            bootstrap_actions.append(f"s3://{artifact_bucket.bucket_name}/bootstrap_actions/{f}")

        self._cluster_configuration = self.emr_resource_config(
            subnet=subnet,
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
            bootstrap_action_script_paths=bootstrap_actions,
        )

        self._launch_function = self.launch_function_config(
            emr_profile=self.emr_profile,
            cluster_configuration=self.cluster_configuration,
            default_fail_if_cluster_running=True,
        )

        self.outputs()

    def outputs(self):
        """
        Extend the values here to add additional outputs of the module
        :return:
        """
        aws_cdk.CfnOutput(
            self,
            "LaunchFunctionARN",
            value=self.launch_function_arn,
        )

        aws_cdk.CfnOutput(
            self,
            "InstanceRoleName",
            value=self.instance_role_name,
        )

    def emr_resource_config(
        self,
        subnet,
        master_instance_type: str,
        core_instance_type: str,
        core_instance_count: int,
        release_label: str,
        applications: [str],
        configuration: dict,
        core_instance_market: str,
        task_instance_type: str,
        task_instance_market: str,
        task_instance_count: int,
        bootstrap_action_script_paths: [str],
    ):

        default_configurations = configuration

        if core_instance_market == "SPOT":
            core_market = InstanceMarketType.SPOT
        elif core_instance_market == "ON_DEMAND":
            core_market = InstanceMarketType.ON_DEMAND
        else:
            raise Exception(f"{core_instance_market} must be one of 'SPOT' or 'ON_DEMAND'")

        if task_instance_market == "SPOT":
            task_market = InstanceMarketType.SPOT
        elif task_instance_market == "ON_DEMAND":
            task_market = InstanceMarketType.ON_DEMAND
        else:
            raise Exception(f"{task_instance_market} must be one of 'SPOT' or 'ON_DEMAND'")

        if bootstrap_action_script_paths:
            bootstrap_actions = []
            for idx, script_path in enumerate(bootstrap_action_script_paths):
                assert "s3://" in script_path, f"{script_path} must be a full s3 path like s3://bucket/script.sh"
                bootstrap_action = emr_code.EMRBootstrapAction(
                    name=f"bootstrap_{idx}",
                    path=script_path,
                )
                bootstrap_actions.append(bootstrap_action)

        else:
            bootstrap_actions = None

        return TaskInstanceGroupConfiguration(
            self,
            "ClusterResourceConfiguration",
            configuration_name=self._cluster_name + "-resources",
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
            bootstrap_actions=bootstrap_actions,
        )

    def init_emr_profile(self, vpc, log_bucket, artifact_bucket):

        profile = self.security_profile_config(
            vpc=vpc, logs_bucket=log_bucket, artifacts_bucket=artifact_bucket, input_kms_keys=[], output_kms_keys=[]
        )

        return profile

    def authorize_buckets(self, input_data_bucket_arns, output_data_bucket_arns):

        input_data_buckets = {
            b_name: s3.Bucket.from_bucket_arn(self, f"In-bucket{i}", bucket_arn=b_name)
            for i, b_name in enumerate(input_data_bucket_arns)
        }

        output_data_buckets = {
            b_name: s3.Bucket.from_bucket_arn(self, f"Out-bucket{i}", bucket_arn=b_name)
            for i, b_name in enumerate(output_data_bucket_arns)
        }

        for b_name, bucket in input_data_buckets.items():
            self._emr_profile.authorize_input_bucket(bucket)

        for b_name, bucket in output_data_buckets.items():
            self._emr_profile.authorize_output_bucket(bucket)

    def security_profile_config(self, vpc, logs_bucket, artifacts_bucket, input_kms_keys=None, output_kms_keys=None):

        profile_name = self._cluster_name + "-security"

        profile = emr_profile.EMRProfile(
            self,
            profile_name,
            profile_name=profile_name,
            vpc=vpc,
            logs_bucket=logs_bucket,
            artifacts_bucket=artifacts_bucket,
        )

        if input_kms_keys:
            for i, k in enumerate(input_kms_keys):
                kms_key = kms.Key.from_key_arn(self, id=f"{profile_name}_input_key_{i}", key_arn=k)
                profile.authorize_input_key(kms_key)
        if output_kms_keys:
            for i, k in enumerate(output_kms_keys):
                kms_key = kms.Key.from_key_arn(self, id=f"{profile_name}_output_key_{i}", key_arn=k)
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
            allowed_cluster_config_overrides=cluster_configuration.override_interfaces["default"],
            cluster_tags=[aws_cdk.Tag(key="Group", value="AWSDemo")],
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

    @property
    def instance_role_arn(self) -> str:
        return self._emr_profile._roles.instance_role.role_arn
