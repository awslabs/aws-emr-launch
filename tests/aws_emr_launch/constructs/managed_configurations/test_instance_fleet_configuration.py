import copy

from aws_cdk import aws_ec2 as ec2
from aws_cdk import core

from aws_emr_launch.constructs.managed_configurations import \
    instance_fleet_configuration

app = core.App()
stack = core.Stack(app, 'test-stack')
vpc = ec2.Vpc(stack, 'test-vpc')
default_config = {
    'ConfigurationName': 'test-cluster',
    'Namespace': 'default',
    'ClusterConfiguration': {
        'Applications': [
            {'Name': 'Hadoop'},
            {'Name': 'Hive'},
            {'Name': 'Spark'}
        ],
        'BootstrapActions': [],
        'Configurations': [
            {
                'Classification': 'hive-site',
                'Properties': {
                    'hive.metastore.client.factory.class':
                        'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                }
            }, {
                'Classification': 'spark-hive-site',
                'Properties': {
                    'hive.metastore.client.factory.class':
                        'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                }
            }
        ],
        'Instances': {
            'Ec2SubnetIds': [
                {'Ref': 'testvpcPrivateSubnet1Subnet865FB50A'},
                {'Ref': 'testvpcPrivateSubnet2Subnet23D3396F'}
            ],
            'InstanceFleets': [
                {
                    'Name': 'Master',
                    'InstanceFleetType': 'MASTER',
                    'TargetOnDemandCapacity': 1,
                    'InstanceTypeConfigs': [
                        {
                            'InstanceType': 'm5.2xlarge',
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
                    'TargetOnDemandCapacity': 2,
                    'TargetSpotCapacity': 0,
                    'InstanceTypeConfigs': [
                        {
                            'InstanceType': 'm5.xlarge',
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
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
        },
        'Name': 'test-cluster',
        'ReleaseLabel': 'emr-5.29.0',
        'StepConcurrencyLevel': 1,
        'Tags': [],
        'VisibleToAllUsers': True,
    },
    'OverrideInterfaces':  {
        'default': {
            'ClusterName': {
                'JsonPath': 'Name',
                'Default': 'test-cluster'
            },
            'ReleaseLabel': {
                'JsonPath': 'ReleaseLabel',
                'Default': 'emr-5.29.0'
            },
            'StepConcurrencyLevel': {
                'JsonPath': 'StepConcurrencyLevel',
                'Default': 1
            },
            'MasterInstanceType': {
                'JsonPath': 'Instances.InstanceFleets.0.InstanceTypeConfigs.0.InstanceType',
                'Default': 'm5.2xlarge'
            },
            'CoreInstanceType': {
                'JsonPath': 'Instances.InstanceFleets.1.InstanceTypeConfigs.0.InstanceType',
                'Default': 'm5.xlarge'
            },
            'CoreInstanceOnDemandCount': {
                'JsonPath': 'Instances.InstanceFleets.1.TargetOnDemandCapacity',
                'Default': 2
            },
            'CoreInstanceSpotCount': {
                'JsonPath': 'Instances.InstanceFleets.1.TargetSpotCapacity',
                'Default': 0
            }

        }
    },
    'ConfigurationArtifacts': []
}


def test_default_configuration():
    cluster_config = instance_fleet_configuration.InstanceFleetConfiguration(
        stack, 'test-instance-group-config',
        configuration_name='test-cluster',
        subnets=vpc.private_subnets)

    config = copy.deepcopy(default_config)

    resolved_config = stack.resolve(cluster_config.to_json())
    print(config)
    print(resolved_config)
    assert resolved_config == config
