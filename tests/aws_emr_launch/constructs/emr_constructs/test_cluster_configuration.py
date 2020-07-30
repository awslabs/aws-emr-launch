import copy

from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_s3 as s3
from aws_cdk import core

from aws_emr_launch.constructs.emr_constructs import (cluster_configuration,
                                                      emr_code)

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
            }
        }
    },
    'ConfigurationArtifacts': []
}


def test_default_configuration():
    cluster_config = cluster_configuration.ClusterConfiguration(
        stack, 'test-instance-group-config',
        configuration_name='test-cluster')

    config = copy.deepcopy(default_config)

    resolved_config = stack.resolve(cluster_config. to_json())
    print(config)
    print(resolved_config)
    assert resolved_config == config


def test_disabling_glue_metastore():
    cluster_config = cluster_configuration.ClusterConfiguration(
        stack, 'test-disable-glue-metastore',
        configuration_name='test-cluster',
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
        path='./examples',
        deployment_bucket=bucket,
        deployment_prefix='prefix')
    bootstrap_action = emr_code.EMRBootstrapAction(
        name='Bootstrap',
        path=f'{bootstrap_code.s3_path}/bootstrap_action',
        args=['Arg1', 'Arg2'],
        code=bootstrap_code)

    cluster_config = cluster_configuration.ClusterConfiguration(
        stack, 'test-bootstrap-action-config',
        configuration_name='test-cluster',
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
    config['ConfigurationArtifacts'] = [
        {
            'Bucket': {'Ref': 'testbucketE6E05ABE'},
            'Path': 'prefix/*'
        }
    ]

    resolved_config = stack.resolve(cluster_config.to_json())
    print(config)
    print(resolved_config)
    assert resolved_config == config
