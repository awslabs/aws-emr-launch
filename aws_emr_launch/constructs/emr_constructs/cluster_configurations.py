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

from typing import Optional, List
from aws_cdk import (
    aws_ec2 as ec2,
    core
)

from .profile_components import EMRProfileComponents


class BaseConfiguration(core.Construct):

    def __init__(self, scope: core.Construct, id: str, *,
                 cluster_name: str,
                 profile_components: EMRProfileComponents,
                 release_label: Optional[str] = 'emr-5.27.0',
                 applications: Optional[List[str]] = None,
                 bootstrap_actions: Optional[List[dict]] = None,
                 configurations: Optional[List[dict]] = None,
                 tags: Optional[List[dict]] = None,
                 use_glue_catalog: Optional[bool] = True,
                 auto_terminate: Optional[bool] = False):

        super().__init__(scope, id)

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
            'JobFlowRole': profile_components.roles.instance_role.instance_profile.attr_arn,
            'ServiceRole': profile_components.roles.service_role.role_arn,
            'AutoScalingRole': profile_components.roles.autoscaling_role.role_arn,
            'VisibleToAllUsers': True,
            'Instances': {
                'EmrManagedMasterSecurityGroup': profile_components.security_groups.master_group.security_group_id,
                'EmrManagedSlaveSecurityGroup': profile_components.security_groups.workers_group.security_group_id,
                'ServiceAccessSecurityGroup': profile_components.security_groups.service_group.security_group_id,
                'TerminationProtected': False,
                'KeepJobFlowAliveWhenNoSteps': not auto_terminate
            }
        }
        if profile_components.security_configuration:
            self._config['SecurityConfiguration'] = profile_components.security_configuration.name

    @staticmethod
    def _get_applications(applications: Optional[List[str]]) -> List[dict]:
        return [{'Name': app} for app in (applications if applications else ['Hadoop', 'Hive', 'Spark'])]

    @staticmethod
    def _get_configurations(configurations: Optional[List[dict]], use_glue_catalog: bool) -> List[dict]:
        found_hive_site = False
        found_spark_hive_site = False

        metastore_property = {
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
    def profile_components(self) -> EMRProfileComponents:
        return self._profile_components

    @property
    def config(self) -> dict:
        return self._config


class InstanceGroupConfiguration(BaseConfiguration):

    def __init__(self, scope: core.Construct, id: str, *,
                 cluster_name: str,
                 profile_components: EMRProfileComponents,
                 release_label: Optional[str] = 'emr-5.27.0',
                 subnet: Optional[ec2.Subnet] = None,
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
                 auto_terminate: Optional[bool] = False):

        super().__init__(scope, id, cluster_name=cluster_name, profile_components=profile_components,
                         release_label=release_label, applications=applications,
                         bootstrap_actions=bootstrap_actions, configurations=configurations,
                         tags=tags, use_glue_catalog=use_glue_catalog, auto_terminate=auto_terminate)

        subnet = profile_components.vpc.private_subnets[0] if subnet is None else subnet
        self.config['Instances']['Ec2SubnetId'] = subnet.subnet_id
        self.config['Instances']['InstanceGroups'] = [
            {
                'Name': 'Master - 1',
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
                'Name': 'Core - 2',
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
