# Copyright 2019 Amazon.com, Inc. and its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the 'License').
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#   http://aws.amazon.com/asl/
#
# or in the 'license' file accompanying this file. This file is distributed
# on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

import copy

from aws_cdk import (
    aws_ec2 as ec2,
    core
)

from aws_emr_launch.constructs.managed_configurations import instance_group_configuration

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
                                    'SizeInGB': 128,
                                    'VolumeType': 'gp2'
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
                                    'SizeInGB': 128,
                                    'VolumeType': 'gp2'
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
            'ClusterName': 'Name',
            'MasterInstanceType': 'Instances.InstanceGroups.0.InstanceType',
            'MasterInstanceMarket': 'Instances.InstanceGroups.0.Market',
            'CoreInstanceCount': 'Instances.InstanceGroups.1.InstanceCount',
            'CoreInstanceType': 'Instances.InstanceGroups.1.InstanceType',
            'CoreInstanceMarket': 'Instances.InstanceGroups.1.Market',
            'Subnet': 'Instances.Ec2SubnetId'
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
