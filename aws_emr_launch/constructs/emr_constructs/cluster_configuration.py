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

import os
import json
import boto3

from enum import Enum
from botocore.exceptions import ClientError

from typing import Optional, List, Dict
from aws_cdk import (
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    aws_ssm as ssm,
    core
)

from aws_emr_launch.constructs.emr_constructs.emr_code import EMRBootstrapAction

SSM_PARAMETER_PREFIX = '/emr_launch/cluster_configurations'


class ClusterConfigurationNotFoundError(Exception):
    pass


class InstanceMarketType(Enum):
    ON_DEMAND = 'ON_DEMAND'
    SPOT = 'SPOT'


class ClusterConfiguration(core.Construct):

    def __init__(self, scope: core.Construct, id: str, *,
                 configuration_name: str,
                 namespace: str = 'default',
                 release_label: Optional[str] = 'emr-5.29.0',
                 applications: Optional[List[str]] = None,
                 bootstrap_actions: Optional[List[EMRBootstrapAction]] = None,
                 configurations: Optional[List[dict]] = None,
                 use_glue_catalog: Optional[bool] = True,
                 step_concurrency_level: Optional[int] = 1,
                 description: Optional[str] = None):

        super().__init__(scope, id)
        self._override_interfaces = {}

        if configuration_name is None:
            return

        self._configuration_name = configuration_name
        self._namespace = namespace
        self._description = description
        self._bootstrap_actions = bootstrap_actions
        self._config = {
            'AdditionalInfo': None,
            'AmiVersion': None,
            'Applications': self._get_applications(applications),
            'AutoScalingRole': None,
            'BootstrapActions': [b.resolve(self) for b in bootstrap_actions] if bootstrap_actions else [],
            'Configurations': self._get_configurations(configurations, use_glue_catalog),
            'CustomAmiId': None,
            'EbsRootVolumeSize': None,
            'Instances': {
                'AdditionalMasterSecurityGroups': None,
                'AdditionalSlaveSecurityGroups': None,
                'Ec2KeyName': None,
                'Ec2SubnetId': None,
                'Ec2SubnetIds': None,
                'EmrManagedMasterSecurityGroup': None,
                'EmrManagedSlaveSecurityGroup': None,
                'HadoopVersion': None,
                'InstanceCount': None,
                'InstanceFleets': None,
                'InstanceGroups': None,
                'KeepJobFlowAliveWhenNoSteps': True,
                'MasterInstanceType': None,
                'Placement': None,
                'ServiceAccessSecurityGroup': None,
                'SlaveInstanceType': None,
                'TerminationProtected': False,
            },
            'JobFlowRole': None,
            'KerberosAttributes': None,
            'LogUri': None,
            'Name': configuration_name,
            'NewSupportedProducts': None,
            'ReleaseLabel': release_label,
            'RepoUpgradeOnBoot': None,
            'ScaleDownBehavior': None,
            'SecurityConfiguration': None,
            'ServiceRole': None,
            'StepConcurrencyLevel': step_concurrency_level,
            'SupportedProducts': None,
            'Tags': [],
            'VisibleToAllUsers': True,
        }

        self._configuration_artifacts = []
        for bootstrap_action in bootstrap_actions:
            if bootstrap_action.code is not None:
                self._configuration_artifacts.append({
                    'Bucket': bootstrap_action.code.deployment_bucket.bucket_name,
                    'Path': os.path.join(bootstrap_action.code.deployment_prefix, '*')
                })

        self._ssm_parameter = ssm.CfnParameter(
            self, 'SSMParameter',
            type='String',
            value=json.dumps(self.to_json()),
            tier='Intelligent-Tiering',
            name=f'{SSM_PARAMETER_PREFIX}/{namespace}/{configuration_name}')

    def to_json(self):
        return {
            'ConfigurationName': self._configuration_name,
            'Description': self._description,
            'Namespace': self._namespace,
            'ClusterConfiguration': self._config,
            'OverrideInterfaces': self._override_interfaces,
            'ConfigurationArtifacts': self._configuration_artifacts
        }

    def from_json(self, property_values):
        self._configuration_name = property_values['ConfigurationName']
        self._namespace = property_values['Namespace']
        self._config = property_values['ClusterConfiguration']
        self._description = property_values.get('Description', None)
        self._override_interfaces = property_values['OverrideInterfaces']
        self._configuration_artifacts = property_values['ConfigurationArtifacts']

    def update_config(self, new_config: dict = None):
        if new_config is not None:
            self._config = new_config
        self._ssm_parameter.value = json.dumps(self.to_json())

    @staticmethod
    def _get_applications(applications: Optional[List[str]]) -> List[dict]:
        return [{'Name': app} for app in (applications if applications else ['Hadoop', 'Hive', 'Spark'])]

    @staticmethod
    def _get_configurations(configurations: Optional[List[dict]], use_glue_catalog: bool) -> List[dict]:
        configurations = [] if configurations is None else configurations
        metastore_property = {} if not use_glue_catalog else {
            'hive.metastore.client.factory.class':
                'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
        }

        configurations = ClusterConfiguration.update_configurations(
            configurations, 'hive-site', metastore_property)
        configurations = ClusterConfiguration.update_configurations(
            configurations, 'spark-hive-site', metastore_property)

        return configurations

    @staticmethod
    def update_configurations(configurations: List[dict], classification: str, properties: Dict[str, str]):
        found_classification = False
        for config in configurations:
            cls = config.get('Classification', '')
            if cls == classification:
                found_classification = True
                config['Properties'] = dict(config.get('Properties', {}), **properties)

        if not found_classification:
            configurations.append({
                'Classification': classification,
                'Properties': properties
            })

        return configurations

    @property
    def configuration_name(self) -> str:
        return self._configuration_name

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def description(self) -> str:
        return self._description

    @property
    def config(self) -> dict:
        return self._config

    @property
    def override_interfaces(self) -> Dict[str, Dict[str, str]]:
        return self._override_interfaces

    @property
    def configuration_artifacts(self) -> List[Dict[str, str]]:
        return self._configuration_artifacts

    @staticmethod
    def get_configurations(namespace: str = 'default', next_token: Optional[str] = None,
                           ssm_client=None) -> Dict[str, any]:
        ssm_client = boto3.client('ssm') if ssm_client is None else ssm_client
        params = {
            'Path': f'{SSM_PARAMETER_PREFIX}/{namespace}/'
        }
        if next_token:
            params['NextToken'] = next_token
        result = ssm_client.get_parameters_by_path(**params)

        configurations = {
            'ClusterConfigurations': [json.loads(p['Value']) for p in result['Parameters']]
        }
        if 'NextToken' in result:
            configurations['NextToken'] = result['NextToken']
        return configurations

    @staticmethod
    def get_configuration(configuration_name: str, namespace: str = 'default',
                          ssm_client=None) -> Dict[str, any]:
        ssm_client = boto3.client('ssm') if ssm_client is None else ssm_client
        try:
            configuration_json = ssm_client.get_parameter(
                Name=f'{SSM_PARAMETER_PREFIX}/{namespace}/{configuration_name}')['Parameter']['Value']
            return json.loads(configuration_json)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ParameterNotFound':
                raise ClusterConfigurationNotFoundError()
            else:
                raise e

    @staticmethod
    def from_stored_configuration(scope: core.Construct, id: str, configuration_name: str, namespace: str = 'default'):
        stored_config = ClusterConfiguration.get_configuration(configuration_name, namespace)
        cluster_config = ClusterConfiguration(scope, id, configuration_name=None)
        cluster_config.from_json(stored_config)
        return cluster_config


class InstanceGroupConfiguration(ClusterConfiguration):

    def __init__(self, scope: core.Construct, id: str, *,
                 configuration_name: str,
                 subnet: ec2.Subnet,
                 namespace: str = 'default',
                 release_label: Optional[str] = 'emr-5.28.0',
                 master_instance_type: Optional[str] = 'm5.2xlarge',
                 master_instance_market: Optional[InstanceMarketType] = InstanceMarketType.ON_DEMAND,
                 core_instance_type: Optional[str] = 'm5.2xlarge',
                 core_instance_market: Optional[InstanceMarketType] = InstanceMarketType.ON_DEMAND,
                 core_instance_count: Optional[int] = 2,
                 applications: Optional[List[str]] = None,
                 bootstrap_actions: Optional[List[EMRBootstrapAction]] = None,
                 configurations: Optional[List[dict]] = None,
                 use_glue_catalog: Optional[bool] = True,
                 step_concurrency_level: Optional[int] = 1,
                 description: Optional[str] = None):

        super().__init__(scope, id,
                         configuration_name=configuration_name,
                         namespace=namespace,
                         release_label=release_label,
                         applications=applications,
                         bootstrap_actions=bootstrap_actions,
                         configurations=configurations,
                         use_glue_catalog=use_glue_catalog,
                         step_concurrency_level=step_concurrency_level,
                         description=description)

        config = self.config
        config['Instances']['Ec2SubnetId'] = subnet.subnet_id
        config['Instances']['InstanceGroups'] = [
            {
                'Name': 'Master',
                'InstanceRole': 'MASTER',
                'InstanceType': master_instance_type,
                'Market': master_instance_market.name,
                'InstanceCount': 1,
                'EbsConfiguration': {
                    'EbsBlockDeviceConfigs': [{
                        'VolumeSpecification': {
                            'SizeInGB': 128,
                            'VolumeType': 'gp2'
                        },
                        'VolumesPerInstance': 1
                    }],
                    'EbsOptimized': True
                }
            },
            {
                'Name': 'Core',
                'InstanceRole': 'CORE',
                'InstanceType': core_instance_type,
                'Market': core_instance_market.name,
                'InstanceCount': core_instance_count,
                'EbsConfiguration': {
                    'EbsBlockDeviceConfigs': [{
                        'VolumeSpecification': {
                            'SizeInGB': 128,
                            'VolumeType': 'gp2'
                        },
                        'VolumesPerInstance': 1
                    }],
                    'EbsOptimized': True
                }
            }
        ]
        self.override_interfaces['default'] = {
            'ClusterName': 'Name',
            'MasterInstanceType': 'Instances.InstanceGroups.0.InstanceType',
            'MasterInstanceMarket': 'Instances.InstanceGroups.0.Market',
            'CoreInstanceCount': 'Instances.InstanceGroups.1.InstanceCount',
            'CoreInstanceType': 'Instances.InstanceGroups.1.InstanceType',
            'CoreInstanceMarket': 'Instances.InstanceGroups.1.Market',
            'Subnet': 'Instances.Ec2SubnetId'
        }

        self.update_config(config)
