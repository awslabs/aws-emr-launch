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

from botocore.exceptions import ClientError

from typing import Optional, List
from aws_cdk import (
    aws_ec2 as ec2,
    aws_ssm as ssm,
    core
)

from .emr_profile import EMRProfile


class ClusterConfigurationNotFoundError(Exception):
    pass


class BaseConfiguration(core.Construct):

    def __init__(self, scope: core.Construct, id: str, *,
                 cluster_name: str,
                 profile_components: Optional[EMRProfile] = None,
                 release_label: Optional[str] = 'emr-5.28.0',
                 applications: Optional[List[str]] = None,
                 bootstrap_actions: Optional[List[dict]] = None,
                 configurations: Optional[List[dict]] = None,
                 tags: Optional[List[dict]] = None,
                 use_glue_catalog: Optional[bool] = True,
                 step_concurrency_level: Optional[int] = 1):

        super().__init__(scope, id)

        if profile_components is None:
            return

        self._profile_components = profile_components
        self._config = {
            'Name': cluster_name,
            'LogUri': 's3://{}/elasticmapreduce/{}'.format(
                profile_components.logs_bucket.bucket_name, cluster_name),
            'ReleaseLabel': release_label,
            'Applications': self._get_applications(applications),
            'BootstrapActions': bootstrap_actions if bootstrap_actions else [],
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

        self._ssm_parameter = ssm.StringParameter(
            self, 'SSMParameter',
            string_value=json.dumps({
                'EMRProfile': self._profile_components.profile_name,
                'ClusterConfig': self._config
            }),
            parameter_name='/emr_launch/control_plane/cluster_configs/{}'.format(cluster_name))

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
    def profile_components(self) -> EMRProfile:
        return self._profile_components

    @property
    def config(self) -> dict:
        return self._config

    @staticmethod
    def from_stored_config(scope: core.Construct, id: str, cluster_name: str):
        try:
            profile_json = boto3.client('ssm', region_name=core.Stack.of(scope).region).get_parameter(
                Name='/emr_launch/control_plane/cluster_configs/{}'.format(cluster_name))['Parameter']['Value']
            cluster_config = BaseConfiguration(scope, id, cluster_name=cluster_name)
            stored_config = json.loads(profile_json)
            cluster_config._profile_components = EMRProfile.from_stored_profile(
                cluster_config, 'EMRProfile', stored_config['EMRProfile'])
            cluster_config._config = stored_config['ClusterConfig']
            return cluster_config
        except ClientError as e:
            if e.response['Error']['Code'] == 'ParameterNotFound':
                raise ClusterConfigurationNotFoundError()


class InstanceGroupConfiguration(BaseConfiguration):

    def __init__(self, scope: core.Construct, id: str, *,
                 cluster_name: str,
                 profile_components: EMRProfile,
                 subnet: ec2.Subnet,
                 release_label: Optional[str] = 'emr-5.28.0',
                 master_instance_type: Optional[str] = 'm5.2xlarge',
                 master_instance_market: Optional[str] = 'ON_DEMAND',
                 core_instance_type: Optional[str] = 'm5.2xlarge',
                 core_instance_market: Optional[str] = 'ON_DEMAND',
                 core_instance_count: Optional[int] = 2,
                 applications: Optional[List[str]] = None,
                 bootstrap_actions: Optional[List[dict]] = None,
                 configurations: Optional[List[dict]] = None,
                 tags: Optional[List[dict]] = None,
                 use_glue_catalog: Optional[bool] = True,
                 step_concurrency_level: Optional[int] = 1):

        super().__init__(scope, id, cluster_name=cluster_name, profile_components=profile_components,
                         release_label=release_label, applications=applications,
                         bootstrap_actions=bootstrap_actions, configurations=configurations,
                         tags=tags, use_glue_catalog=use_glue_catalog, step_concurrency_level=step_concurrency_level)

        self.config['Instances']['Ec2SubnetId'] = subnet.subnet_id
        self.config['Instances']['InstanceGroups'] = [
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
