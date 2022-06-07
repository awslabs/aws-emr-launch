from typing import Any, Dict, List, Optional

from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_secretsmanager as secretsmanager

import constructs
from aws_emr_launch.constructs.emr_constructs import emr_code
from aws_emr_launch.constructs.emr_constructs.cluster_configuration import ClusterConfiguration, InstanceMarketType


class InstanceGroupConfiguration(ClusterConfiguration):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        *,
        configuration_name: str,
        subnet: ec2.ISubnet,
        namespace: str = "default",
        release_label: str = "emr-5.29.0",
        master_instance_type: str = "m5.2xlarge",
        master_instance_market: InstanceMarketType = InstanceMarketType.ON_DEMAND,
        core_instance_type: str = "m5.xlarge",
        core_instance_market: InstanceMarketType = InstanceMarketType.ON_DEMAND,
        core_instance_count: int = 2,
        applications: Optional[List[str]] = None,
        bootstrap_actions: Optional[List[emr_code.EMRBootstrapAction]] = None,
        configurations: Optional[List[Dict[str, Any]]] = None,
        use_glue_catalog: bool = True,
        step_concurrency_level: int = 1,
        description: Optional[str] = None,
        secret_configurations: Optional[Dict[str, secretsmanager.ISecret]] = None,
    ):

        super().__init__(
            scope,
            id,
            configuration_name=configuration_name,
            namespace=namespace,
            release_label=release_label,
            applications=applications,
            bootstrap_actions=bootstrap_actions,
            configurations=configurations,
            use_glue_catalog=use_glue_catalog,
            step_concurrency_level=step_concurrency_level,
            description=description,
            secret_configurations=secret_configurations,
        )

        config = self.config
        config["Instances"]["Ec2SubnetId"] = subnet.subnet_id
        config["Instances"]["InstanceGroups"] = [
            {
                "Name": "Master",
                "InstanceRole": "MASTER",
                "InstanceType": master_instance_type,
                "Market": master_instance_market.name,
                "InstanceCount": 1,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {"VolumeSpecification": {"SizeInGB": 500, "VolumeType": "st1"}, "VolumesPerInstance": 1}
                    ],
                    "EbsOptimized": True,
                },
            },
            {
                "Name": "Core",
                "InstanceRole": "CORE",
                "InstanceType": core_instance_type,
                "Market": core_instance_market.name,
                "InstanceCount": core_instance_count,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {"VolumeSpecification": {"SizeInGB": 500, "VolumeType": "st1"}, "VolumesPerInstance": 1}
                    ],
                    "EbsOptimized": True,
                },
            },
        ]
        self.override_interfaces["default"].update(
            {
                "MasterInstanceType": {
                    "JsonPath": "Instances.InstanceGroups.0.InstanceType",
                    "Default": master_instance_type,
                },
                "MasterInstanceMarket": {
                    "JsonPath": "Instances.InstanceGroups.0.Market",
                    "Default": master_instance_market.value,
                },
                "CoreInstanceCount": {
                    "JsonPath": "Instances.InstanceGroups.1.InstanceCount",
                    "Default": core_instance_count,
                },
                "CoreInstanceType": {
                    "JsonPath": "Instances.InstanceGroups.1.InstanceType",
                    "Default": core_instance_type,
                },
                "CoreInstanceMarket": {
                    "JsonPath": "Instances.InstanceGroups.1.Market",
                    "Default": core_instance_market.value,
                },
                "Subnet": {"JsonPath": "Instances.Ec2SubnetId", "Default": subnet.subnet_id},
            }
        )

        self.update_config(config)


class ManagedScalingConfiguration(InstanceGroupConfiguration):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        *,
        configuration_name: str,
        subnet: ec2.ISubnet,
        namespace: str = "default",
        release_label: str = "emr-5.30.0",
        master_instance_type: str = "m5.2xlarge",
        master_instance_market: InstanceMarketType = InstanceMarketType.ON_DEMAND,
        core_instance_type: str = "m5.xlarge",
        core_instance_market: InstanceMarketType = InstanceMarketType.ON_DEMAND,
        core_instance_count: int = 2,
        applications: Optional[List[str]] = None,
        bootstrap_actions: Optional[List[emr_code.EMRBootstrapAction]] = None,
        configurations: Optional[List[Dict[str, Any]]] = None,
        use_glue_catalog: bool = True,
        step_concurrency_level: int = 1,
        description: Optional[str] = None,
        secret_configurations: Optional[Dict[str, secretsmanager.ISecret]] = None,
        minimum_instances: int = 2,
        maximum_instances: int = 10,
    ):

        super().__init__(
            scope=scope,
            id=id,
            configuration_name=configuration_name,
            subnet=subnet,
            namespace=namespace,
            release_label=release_label,
            master_instance_type=master_instance_type,
            master_instance_market=master_instance_market,
            core_instance_type=core_instance_type,
            core_instance_market=core_instance_market,
            core_instance_count=core_instance_count,
            applications=applications,
            bootstrap_actions=bootstrap_actions,
            configurations=configurations,
            use_glue_catalog=use_glue_catalog,
            step_concurrency_level=step_concurrency_level,
            description=description,
            secret_configurations=secret_configurations,
        )

        config = self.config
        config["ManagedScalingPolicy"] = {
            "ComputeLimits": {
                "MinimumCapacityUnits": minimum_instances,
                "MaximumCapacityUnits": maximum_instances,
                "UnitType": "Instances",
            }
        }

        self.override_interfaces["default"].update(
            {
                "MinimumInstances": {
                    "JsonPath": "ManagedScalingPolicy.ComputeLimits.MinimumCapacityUnits",
                    "Default": minimum_instances,
                },
                "MaximumInstances": {
                    "JsonPath": "ManagedScalingPolicy.ComputeLimits.MaximumCapacityUnits",
                    "Default": maximum_instances,
                },
            }
        )

        self.update_config(config)
