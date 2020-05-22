import copy

from aws_cdk import aws_ec2 as ec2
from aws_cdk import core

from aws_emr_launch.constructs.managed_configurations import \
    instance_group_configuration

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
            'Ec2SubnetId': {'Ref': 'testvpcPrivateSubnet1Subnet865FB50A'},
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.2xlarge',
                    'Market': 'ON_DEMAND',
                    'InstanceCount': 1,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'SizeInGB': 500,
                                    'VolumeType': 'st1'
                                },
                                'VolumesPerInstance': 1
                            }
                        ],
                        'EbsOptimized': True
                    }
                }, {
                    'Name': 'Core',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'Market': 'ON_DEMAND',
                    'InstanceCount': 2,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'SizeInGB': 500,
                                    'VolumeType': 'st1'
                                },
                                'VolumesPerInstance': 1
                            }
                        ],
                        'EbsOptimized': True
                    }
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
    'OverrideInterfaces': {
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
                'JsonPath': 'Instances.InstanceGroups.0.InstanceType',
                'Default': 'm5.2xlarge'
            },
            'MasterInstanceMarket': {
                'JsonPath': 'Instances.InstanceGroups.0.Market',
                'Default': 'ON_DEMAND'
            },
            'CoreInstanceCount': {
                'JsonPath': 'Instances.InstanceGroups.1.InstanceCount',
                'Default': 2
            },
            'CoreInstanceType': {
                'JsonPath': 'Instances.InstanceGroups.1.InstanceType',
                'Default': 'm5.xlarge'
            },
            'CoreInstanceMarket': {
                'JsonPath': 'Instances.InstanceGroups.1.Market',
                'Default': 'ON_DEMAND'
            },
            'Subnet': {
                'JsonPath': 'Instances.Ec2SubnetId',
                'Default': {'Ref': 'testvpcPrivateSubnet1Subnet865FB50A'}
            }
        }
    },
    'ConfigurationArtifacts': []
}


def test_default_configuration():
    cluster_config = instance_group_configuration.InstanceGroupConfiguration(
        stack, 'test-instance-group-config',
        configuration_name='test-cluster',
        subnet=vpc.private_subnets[0])

    config = copy.deepcopy(default_config)

    resolved_config = stack.resolve(cluster_config.to_json())
    print(config)
    print(resolved_config)
    assert resolved_config == config
