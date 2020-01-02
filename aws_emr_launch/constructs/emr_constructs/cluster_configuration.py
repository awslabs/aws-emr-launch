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

import json
import boto3

from typing import Mapping
from botocore.exceptions import ClientError

from typing import Optional, List
from aws_cdk import (
    aws_ec2 as ec2,
    aws_ssm as ssm,
    core
)

from .emr_profile import EMRProfile
from .emr_code import EMRBootstrapAction

SSM_PARAMETER_PREFIX = '/emr_launch/cluster_configurations'


class ClusterConfigurationNotFoundError(Exception):
    pass


class ClusterConfiguration(core.Construct):

    def __init__(self, scope: core.Construct, id: str, *,
                 configuration_name: str,
                 namespace: str = 'default',
                 profile_components: Optional[EMRProfile] = None,
                 release_label: Optional[str] = 'emr-5.28.0',
                 applications: Optional[List[str]] = None,
                 bootstrap_actions: Optional[List[EMRBootstrapAction]] = None,
                 configurations: Optional[List[dict]] = None,
                 tags: Optional[List[dict]] = None,
                 use_glue_catalog: Optional[bool] = True,
                 step_concurrency_level: Optional[int] = 1,
                 description: Optional[str] = None):

        super().__init__(scope, id)

        if profile_components is None:
            return

        self._configuration_name = configuration_name
        self._profile_components = profile_components
        self._description = description
        self._config = {
            'Name': configuration_name,
            'LogUri': f's3://{profile_components.logs_bucket.bucket_name}/elasticmapreduce/{configuration_name}',
            'ReleaseLabel': release_label,
            'Applications': self._get_applications(applications),
            'BootstrapActions': [b.resolve(self) for b in bootstrap_actions] if bootstrap_actions else [],
            'Tags': tags if tags else [],
            'Configurations': self._get_configurations(configurations, use_glue_catalog),
            'JobFlowRole': profile_components.roles.instance_profile_arn,
            'ServiceRole': profile_components.roles.service_role.role_arn,
            'AutoScalingRole': profile_components.roles.autoscaling_role.role_arn,
            'VisibleToAllUsers': True,
            'Instances': {
                'EmrManagedMasterSecurityGroup': profile_components.security_groups.master_group.security_group_id,
                'EmrManagedSlaveSecurityGroup': profile_components.security_groups.workers_group.security_group_id,
                'ServiceAccessSecurityGroup': profile_components.security_groups.service_group.security_group_id,
                'TerminationProtected': False,
                'KeepJobFlowAliveWhenNoSteps': True
            },
            'StepConcurrencyLevel': step_concurrency_level
        }
        if profile_components.security_configuration_name:
            self._config['SecurityConfiguration'] = profile_components.security_configuration_name

        self._ssm_parameter = ssm.CfnParameter(
            self, 'SSMParameter',
            type='String',
            value=json.dumps({
                'EMRProfile': self._profile_components.profile_name,
                'Description': self._description,
                'ClusterConfiguration': self._config
            }),
            name=f'{SSM_PARAMETER_PREFIX}/{namespace}/{configuration_name}')

    def _update_config(self, new_config):
        self._config = new_config
        self._ssm_parameter.value = json.dumps({
            'EMRProfile': self._profile_components.profile_name,
            'Description': self._description,
            'ClusterConfiguration': self._config
        })

    @staticmethod
    def _get_applications(applications: Optional[List[str]]) -> List[dict]:
        return [{'Name': app} for app in (applications if applications else ['Hadoop', 'Hive', 'Spark'])]

    @staticmethod
    def _get_configurations(configurations: Optional[List[dict]], use_glue_catalog: bool) -> List[dict]:
        found_hive_site = False
        found_spark_hive_site = False

        metastore_property = {} if not use_glue_catalog else {
            'hive.metastore.client.factory.class':
                'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
        }

        configurations = [] if configurations is None else configurations
        for config in configurations:
            classification = config.get('Classification', '')
            if classification == 'hive-site':
                found_hive_site = True
                config['Properties'] = dict(config.get('Properties', {}), **metastore_property)
            elif classification == 'spark-hive-site':
                found_spark_hive_site = True
                config['Properties'] = dict(config.get('Properties', {}), **metastore_property)

        if not found_hive_site:
            configurations.append({
                'Classification': 'hive-site',
                'Properties': metastore_property
            })
        if not found_spark_hive_site:
            configurations.append({
                'Classification': 'spark-hive-site',
                'Properties': metastore_property
            })

        return configurations

    @property
    def configuration_name(self) -> str:
        return self._configuration_name

    @property
    def profile_components(self) -> EMRProfile:
        return self._profile_components

    @property
    def description(self) -> str:
        return self._description

    @property
    def config(self) -> dict:
        return self._config

    @staticmethod
    def get_configurations(namespace: str = 'default', next_token: Optional[str] = None) -> Mapping[str, any]:
        params = {
            'Path': f'{SSM_PARAMETER_PREFIX}/{namespace}/'
        }
        if next_token:
            params['NextToken'] = next_token
        result = boto3.client('ssm').get_parameters_by_path(**params)

        configurations = {
            'ClusterConfigurations': [json.loads(p['Value']) for p in result['Parameters']]
        }
        if 'NextToken' in result:
            configurations['NextToken'] = result['NextToken']
        return configurations

    @staticmethod
    def get_configuration(configuration_name: str, namespace: str = 'default') -> Mapping[str, any]:
        try:
            configuration_json = boto3.client('ssm').get_parameter(
                Name=f'{SSM_PARAMETER_PREFIX}/{namespace}/{configuration_name}')['Parameter']['Value']
            return json.loads(configuration_json)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ParameterNotFound':
                raise ClusterConfigurationNotFoundError()

    @staticmethod
    def from_stored_configuration(scope: core.Construct, id: str, configuration_name: str, namespace: str = 'default'):
        stored_config = ClusterConfiguration.get_configuration(configuration_name, namespace)
        cluster_config = ClusterConfiguration(scope, id, configuration_name=configuration_name)
        cluster_config._configuration_name = configuration_name
        cluster_config._profile_components = EMRProfile.from_stored_profile(
            cluster_config, 'EMRProfile', stored_config['EMRProfile'])
        cluster_config._config = stored_config['ClusterConfiguration']
        cluster_config._description = stored_config['Description']
        return cluster_config


class InstanceGroupConfiguration(ClusterConfiguration):

    def __init__(self, scope: core.Construct, id: str, *,
                 configuration_name: str,
                 profile_components: EMRProfile,
                 subnet: ec2.Subnet,
                 namespace: str = 'default',
                 release_label: Optional[str] = 'emr-5.28.0',
                 master_instance_type: Optional[str] = 'm5.2xlarge',
                 master_instance_market: Optional[str] = 'ON_DEMAND',
                 core_instance_type: Optional[str] = 'm5.2xlarge',
                 core_instance_market: Optional[str] = 'ON_DEMAND',
                 core_instance_count: Optional[int] = 2,
                 applications: Optional[List[str]] = None,
                 bootstrap_actions: Optional[List[EMRBootstrapAction]] = None,
                 configurations: Optional[List[dict]] = None,
                 tags: Optional[List[dict]] = None,
                 use_glue_catalog: Optional[bool] = True,
                 step_concurrency_level: Optional[int] = 1):

        super().__init__(scope, id,
                         configuration_name=configuration_name,
                         namespace=namespace,
                         profile_components=profile_components,
                         release_label=release_label,
                         applications=applications,
                         bootstrap_actions=bootstrap_actions,
                         configurations=configurations,
                         tags=tags,
                         use_glue_catalog=use_glue_catalog,
                         step_concurrency_level=step_concurrency_level)

        config = self.config
        config['Instances']['Ec2SubnetId'] = subnet.subnet_id
        config['Instances']['InstanceGroups'] = [
            {
                'Name': 'Master',
                'InstanceRole': 'MASTER',
                'InstanceType': master_instance_type,
                'Market': master_instance_market,
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
                'Market': core_instance_market,
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
        self._update_config(config)
