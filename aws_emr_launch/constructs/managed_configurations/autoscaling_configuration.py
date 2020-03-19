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


from typing import Dict, List, Optional
from aws_cdk import (
    aws_ec2 as ec2,
    aws_secretsmanager as secretsmanager,
    core
)

from aws_emr_launch.constructs.emr_constructs import emr_code
from aws_emr_launch.constructs.emr_constructs.cluster_configuration import InstanceMarketType
from aws_emr_launch.constructs.managed_configurations.instance_group_configuration import InstanceGroupConfiguration


class AutoScalingClusterConfiguration(InstanceGroupConfiguration):
    def __init__(self, scope: core.Construct, id: str, *,
                 configuration_name: str,
                 subnet: ec2.Subnet,
                 namespace: str = 'default',
                 release_label: Optional[str] = 'emr-5.29.0',
                 master_instance_type: Optional[str] = 'm5.2xlarge',
                 master_instance_market: Optional[InstanceMarketType] = InstanceMarketType.ON_DEMAND,
                 core_instance_type: Optional[str] = 'm5.xlarge',
                 core_instance_market: Optional[InstanceMarketType] = InstanceMarketType.ON_DEMAND,
                 core_instance_count: Optional[int] = 2,
                 task_instance_type: Optional[str] = 'm5.xlarge',
                 task_instance_market: Optional[InstanceMarketType] = InstanceMarketType.ON_DEMAND,
                 initial_task_instance_count: Optional[int] = 2,
                 minimum_task_instance_count: Optional[int] = 2,
                 maximum_task_instance_count: Optional[int] = 5,
                 scale_out_adjustment: Optional[int] = 2,
                 scale_in_adjustment: Optional[int] = 2,
                 applications: Optional[List[str]] = None,
                 bootstrap_actions: Optional[List[emr_code.EMRBootstrapAction]] = None,
                 configurations: Optional[List[dict]] = None,
                 use_glue_catalog: Optional[bool] = True,
                 step_concurrency_level: Optional[int] = 1,
                 description: Optional[str] = None,
                 secret_configurations: Optional[Dict[str, secretsmanager.Secret]] = None):

        super().__init__(scope, id,
                         configuration_name=configuration_name,
                         subnet=subnet,
                         namespace=namespace,
                         release_label=release_label,
                         master_instance_type=master_instance_type,
                         master_instance_market=master_instance_market,
                         core_instance_type=core_instance_type,
                         core_instance_market=core_instance_market,
                         core_instance_count=core_instance_count,
                         applications=applications,
                         bootstrap_actions=bootstrap_actions,
                         configurations=configurations,
                         use_glue_catalog=use_glue_catalog,
                         step_concurrency_level=step_concurrency_level,
                         description=description,
                         secret_configurations=secret_configurations)

        config = self.config

        config['Instances']['InstanceGroups'].append(
            {
                'InstanceCount': initial_task_instance_count,
                'AutoScalingPolicy': {
                    'Constraints': {
                        'MinCapacity': minimum_task_instance_count,
                        'MaxCapacity': maximum_task_instance_count
                    },
                    'Rules': [{
                        'Action': {
                            'SimpleScalingPolicyConfiguration': {
                                'ScalingAdjustment': scale_out_adjustment * -1
                                if scale_out_adjustment < 0
                                else scale_out_adjustment,
                                'CoolDown': 300,
                                'AdjustmentType': 'CHANGE_IN_CAPACITY'
                            }
                        },
                        'Description': 'Scale Out on YARNMemoryAvailablePercentage',
                        'Trigger': {
                            'CloudWatchAlarmDefinition': {
                                'MetricName': 'YARNMemoryAvailablePercentage',
                                'ComparisonOperator': 'LESS_THAN',
                                'Statistic': 'AVERAGE',
                                'Period': 300,
                                'Dimensions': [{
                                    'Value': '${emr.clusterId}',
                                    'Key': 'JobFlowId'
                                }],
                                'EvaluationPeriods': 3,
                                'Unit': 'PERCENT',
                                'Namespace': 'AWS/ElasticMapReduce',
                                'Threshold': 15
                            }
                        },
                        'Name': 'ScaleOut-YARNMemoryAvailablePercentage'
                    }, {
                        'Action': {
                            'SimpleScalingPolicyConfiguration': {
                                'ScalingAdjustment': scale_out_adjustment * -1
                                if scale_out_adjustment < 0
                                else scale_out_adjustment,
                                'CoolDown': 300,
                                'AdjustmentType': 'CHANGE_IN_CAPACITY'
                            }
                        },
                        'Description': 'Scale Out on ContainerPendingRatio',
                        'Trigger': {
                            'CloudWatchAlarmDefinition': {
                                'MetricName': 'ContainerPendingRatio',
                                'ComparisonOperator': 'GREATER_THAN',
                                'Statistic': 'AVERAGE',
                                'Period': 300,
                                'Dimensions': [{
                                    'Value': '${emr.clusterId}',
                                    'Key': 'JobFlowId'
                                }],
                                'EvaluationPeriods': 3,
                                'Unit': 'COUNT',
                                'Namespace': 'AWS/ElasticMapReduce',
                                'Threshold': 0.75
                            }
                        },
                        'Name': 'ScaleOut-ContainerPendingRatio'
                    }, {
                        'Action': {
                            'SimpleScalingPolicyConfiguration': {
                                'ScalingAdjustment': scale_in_adjustment * -1
                                if scale_in_adjustment > 0
                                else scale_in_adjustment,
                                'CoolDown': 300,
                                'AdjustmentType': 'CHANGE_IN_CAPACITY'
                            }
                        },
                        'Description': 'Scale In on YARNMemoryAvailablePercentage',
                        'Trigger': {
                            'CloudWatchAlarmDefinition': {
                                'MetricName': 'YARNMemoryAvailablePercentage',
                                'ComparisonOperator': 'GREATER_THAN',
                                'Statistic': 'AVERAGE',
                                'Period': 300,
                                'Dimensions': [{
                                    'Value': '${emr.clusterId}',
                                    'Key': 'JobFlowId'
                                }],
                                'EvaluationPeriods': 3,
                                'Unit': 'PERCENT',
                                'Namespace': 'AWS/ElasticMapReduce',
                                'Threshold': 75
                            }
                        },
                        'Name': 'ScaleIn-YARNMemoryAvailablePercentage'
                    }]
                },
                'InstanceRole': 'TASK',
                'InstanceType': task_instance_type,
                'Market': task_instance_market.name,
                'Name': 'Task'
            }
        )

        self.override_interfaces['default'] = {
            'ClusterName': 'Name',
            'MasterInstanceType': 'Instances.InstanceGroups.0.InstanceType',
            'MasterInstanceMarket': 'Instances.InstanceGroups.0.Market',
            'CoreInstanceCount': 'Instances.InstanceGroups.1.InstanceCount',
            'CoreInstanceType': 'Instances.InstanceGroups.1.InstanceType',
            'CoreInstanceMarket': 'Instances.InstanceGroups.1.Market',
            'TaskInstanceType': 'Instances.InstanceGroups.2.InstanceType',
            'TaskInstanceMarket': 'Instances.InstanceGroups.2.Market',
            'TaskInitialInstanceCount': 'Instances.InstanceGroups.2.InstanceCount',
            'TaskMinimumInstanceCount': 'Instances.InstanceGroups.2.AutoScalingPolicy.Constraints.MinCapacity',
            'TaskMaximumInstanceCount': 'Instances.InstanceGroups.2.AutoScalingPolicy.Constraints.MaxCapacity'
        }

        self.update_config(config)
