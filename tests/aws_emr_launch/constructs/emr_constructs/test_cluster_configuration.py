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
    aws_s3 as s3,
    core
)

from aws_emr_launch.constructs.emr_constructs import (
    cluster_configuration,
    emr_code
)

app = core.App()
stack = core.Stack(app, 'test-stack')
vpc = ec2.Vpc(stack, 'test-vpc')
default_config = {
    'ConfigurationName': 'test-cluster',
    'Namespace': 'default',
    'ClusterConfiguration': {
        'Name': 'test-cluster',
        'ReleaseLabel': 'emr-5.28.0',
        'Applications': [
            {'Name': 'Hadoop'},
            {'Name': 'Hive'},
            {'Name': 'Spark'}
        ],
        'BootstrapActions': [],
        'Tags': [],
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
        'VisibleToAllUsers': True,
        'Instances': {
            'TerminationProtected': False,
            'KeepJobFlowAliveWhenNoSteps': True,
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
                    'InstanceType': 'm5.2xlarge',
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
            ]
        },
        'StepConcurrencyLevel': 1
    }
}


def test_default_configuration():
    cluster_config = cluster_configuration.InstanceGroupConfiguration(
        stack, 'test-instance-group-config',
        configuration_name='test-cluster',
        subnet=vpc.private_subnets[0])

    config = copy.deepcopy(default_config)

    resolved_config = stack.resolve(cluster_config.to_json())
    print(config)
    print(resolved_config)
    assert resolved_config == config


def test_disabling_glue_metastore():
    cluster_config = cluster_configuration.InstanceGroupConfiguration(
        stack, 'test-disable-glue-metastore',
        configuration_name='test-cluster',
        subnet=vpc.private_subnets[0],
        use_glue_catalog=False)

    config = copy.deepcopy(default_config)
    config['ClusterConfiguration']['Configurations'] = [
        {
            'Classification': 'hive-site',
            'Properties': {}
        }, {
            'Classification': 'spark-hive-site',
            'Properties': {}
        }
    ]

    resolved_config = stack.resolve(cluster_config.to_json())
    print(config)
    print(resolved_config)
    assert resolved_config == config


def test_bootstrap_action_config():
    bucket = s3.Bucket(stack, 'test-bucket')
    bootstrap_code = emr_code.Code.from_path(
        path='./docs',
        deployment_bucket=bucket,
        deployment_prefix='prefix')
    bootstrap_action = emr_code.EMRBootstrapAction(
        name='Bootstrap',
        path=f'{bootstrap_code.s3_path}/bootstrap_action',
        args=['Arg1', 'Arg2'],
        code=bootstrap_code)

    cluster_config = cluster_configuration.InstanceGroupConfiguration(
        stack, 'test-bootstrap-action-config',
        configuration_name='test-cluster',
        subnet=vpc.private_subnets[0],
        bootstrap_actions=[bootstrap_action])

    config = copy.deepcopy(default_config)
    config['ClusterConfiguration']['BootstrapActions'] = [
        {
            'Name': 'Bootstrap',
            'ScriptBootstrapAction': {
                'Path': {
                    'Fn::Join': ['', ['s3://', {'Ref': 'testbucketE6E05ABE'}, '/prefix/bootstrap_action']]
                },
                'Args': ['Arg1', 'Arg2']
            }
        }
    ]

    resolved_config = stack.resolve(cluster_config.to_json())
    print(config)
    print(resolved_config)
    assert resolved_config == config


