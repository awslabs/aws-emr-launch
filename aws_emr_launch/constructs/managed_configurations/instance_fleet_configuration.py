from typing import Dict, List, Optional

from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import core

from aws_emr_launch.constructs.emr_constructs import emr_code
from aws_emr_launch.constructs.emr_constructs.cluster_configuration import (
    ClusterConfiguration, InstanceMarketType)


class InstanceFleetConfiguration(ClusterConfiguration):

    def __init__(self, scope: core.Construct, id: str, *,
                 configuration_name: str,
                 subnets: [ec2.Subnet],
                 namespace: str = 'default',
                 release_label: Optional[str] = 'emr-5.29.0',
                 master_instance_type: Optional[str] = 'm5.2xlarge',
                 master_instance_market: Optional[InstanceMarketType] = InstanceMarketType.ON_DEMAND,
                 core_instance_type: Optional[str] = 'm5.xlarge',
                 core_instance_on_demand_count: Optional[int] = 2,
                 core_instance_spot_count: Optional[int] = 0,
                 applications: Optional[List[str]] = None,
                 bootstrap_actions: Optional[List[emr_code.EMRBootstrapAction]] = None,
                 configurations: Optional[List[dict]] = None,
                 use_glue_catalog: Optional[bool] = True,
                 step_concurrency_level: Optional[int] = 1,
                 description: Optional[str] = None,
                 secret_configurations: Optional[Dict[str, secretsmanager.Secret]] = None):

        super().__init__(scope, id,
                         configuration_name=configuration_name,
                         namespace=namespace,
                         release_label=release_label,
                         applications=applications,
                         bootstrap_actions=bootstrap_actions,
                         configurations=configurations,
                         use_glue_catalog=use_glue_catalog,
                         step_concurrency_level=step_concurrency_level,
                         description=description,
                         secret_configurations=secret_configurations)

        config = self.config
        config['Instances']['Ec2SubnetIds'] = [s.subnet_id for s in subnets]
        config['Instances']['InstanceFleets'] = [
            {
                'Name': 'Master',
                'InstanceFleetType': 'MASTER',
                'InstanceTypeConfigs': [
                    {
                        'InstanceType': master_instance_type,
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [{
                                'VolumeSpecification': {
                                    'SizeInGB': 500,
                                    'VolumeType': 'st1'
                                },
                                'VolumesPerInstance': 1
                            }],
                            'EbsOptimized': True
                        }
                    }
                ],
            },
            {
                'Name': 'Core',
                'InstanceFleetType': 'CORE',
                'TargetOnDemandCapacity': core_instance_on_demand_count,
                'TargetSpotCapacity': core_instance_spot_count,
                'InstanceTypeConfigs': [
                    {
                        'InstanceType': core_instance_type,
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [{
                                'VolumeSpecification': {
                                    'SizeInGB': 500,
                                    'VolumeType': 'st1'
                                },
                                'VolumesPerInstance': 1
                            }],
                            'EbsOptimized': True
                        }
                    }
                ],
            }
        ]

        if master_instance_market == InstanceMarketType.ON_DEMAND:
            config['Instances']['InstanceFleets'][0]['TargetOnDemandCapacity'] = 1
        else:
            config['Instances']['InstanceFleets'][0]['TargetSpotCapacity'] = 1

        self.override_interfaces['default'].update({
            'MasterInstanceType': {
                'JsonPath': 'Instances.InstanceFleets.0.InstanceTypeConfigs.0.InstanceType',
                'Default': master_instance_type
            },
            'CoreInstanceType': {
                'JsonPath': 'Instances.InstanceFleets.1.InstanceTypeConfigs.0.InstanceType',
                'Default': core_instance_type
            },
            'CoreInstanceOnDemandCount': {
                'JsonPath': 'Instances.InstanceFleets.1.TargetOnDemandCapacity',
                'Default': core_instance_on_demand_count
            },
            'CoreInstanceSpotCount': {
                'JsonPath': 'Instances.InstanceFleets.1.TargetSpotCapacity',
                'Default': core_instance_spot_count
            }
        })

        self.update_config(config)
