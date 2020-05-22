from typing import Dict, List, Optional

from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import core

from aws_emr_launch.constructs.emr_constructs import emr_code
from aws_emr_launch.constructs.emr_constructs.cluster_configuration import \
    InstanceMarketType
from aws_emr_launch.constructs.managed_configurations.instance_group_configuration import \
    InstanceGroupConfiguration


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

        self.override_interfaces['default'].update({
            'TaskInstanceType': {
                'JsonPath': 'Instances.InstanceGroups.2.InstanceType',
                'Default': task_instance_type
            },
            'TaskInstanceMarket': {
                'JsonPath': 'Instances.InstanceGroups.2.Market',
                'Default': task_instance_market.value
            },
            'TaskInitialInstanceCount': {
                'JsonPath': 'Instances.InstanceGroups.2.InstanceCount',
                'Default': initial_task_instance_count
            },
            'TaskMinimumInstanceCount': {
                'JsonPath': 'Instances.InstanceGroups.2.AutoScalingPolicy.Constraints.MinCapacity',
                'Default': minimum_task_instance_count
            },
            'TaskMaximumInstanceCount': {
                'JsonPath': 'Instances.InstanceGroups.2.AutoScalingPolicy.Constraints.MaxCapacity',
                'Default': maximum_task_instance_count
            }
        })

        self.update_config(config)
