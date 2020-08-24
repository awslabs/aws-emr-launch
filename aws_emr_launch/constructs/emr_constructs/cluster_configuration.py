import base64
import hashlib
import json
import os
from enum import Enum
from typing import Dict, List, Optional

import boto3
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_ssm as ssm
from aws_cdk import core
from botocore.exceptions import ClientError

from aws_emr_launch.constructs.base import BaseConstruct
from aws_emr_launch.constructs.emr_constructs import emr_code

SSM_PARAMETER_PREFIX = '/emr_launch/cluster_configurations'


class ClusterConfigurationNotFoundError(Exception):
    pass


class ReadOnlyClusterConfigurationError(Exception):
    pass


class InstanceMarketType(Enum):
    ON_DEMAND = 'ON_DEMAND'
    SPOT = 'SPOT'


class ClusterConfiguration(BaseConstruct):

    def __init__(self, scope: core.Construct, id: str, *,
                 configuration_name: str,
                 namespace: str = 'default',
                 release_label: Optional[str] = 'emr-5.29.0',
                 applications: Optional[List[str]] = None,
                 bootstrap_actions: Optional[List[emr_code.EMRBootstrapAction]] = None,
                 configurations: Optional[List[dict]] = None,
                 use_glue_catalog: Optional[bool] = True,
                 step_concurrency_level: Optional[int] = 1,
                 description: Optional[str] = None,
                 secret_configurations: Optional[Dict[str, secretsmanager.Secret]] = None):

        super().__init__(scope, id)

        self._override_interfaces = {}

        if configuration_name is None:
            return

        self._configuration_name = configuration_name
        self._namespace = namespace
        self._description = description
        self._bootstrap_actions = bootstrap_actions
        self._secret_configurations = secret_configurations
        self._spark_packages = []
        self._spark_jars = []

        if bootstrap_actions:
            # Create a nested Construct to avoid Construct id collisions
            construct = core.Construct(self, 'BootstrapActions')
            resolved_bootstrap_actions = [b.resolve(construct) for b in bootstrap_actions]
        else:
            resolved_bootstrap_actions = []

        self._config = {
            'AdditionalInfo': None,
            'AmiVersion': None,
            'Applications': self._get_applications(applications),
            'AutoScalingRole': None,
            'BootstrapActions': resolved_bootstrap_actions,
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
            'ManagedScalingPolicy': None,
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
        if bootstrap_actions is not None:
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

        self.override_interfaces['default'] = {
            'ClusterName': {
                'JsonPath': 'Name',
                'Default': configuration_name
            },
            'ReleaseLabel': {
                'JsonPath': 'ReleaseLabel',
                'Default': release_label
            },
            'StepConcurrencyLevel': {
                'JsonPath': 'StepConcurrencyLevel',
                'Default': step_concurrency_level
            }
        }

        self._rehydrated = False

    def to_json(self):
        return {
            'ConfigurationName': self._configuration_name,
            'Description': self._description,
            'Namespace': self._namespace,
            'ClusterConfiguration': self._config,
            'OverrideInterfaces': self._override_interfaces,
            'ConfigurationArtifacts': self._configuration_artifacts,
            'SecretConfigurations':
                {k: v.secret_arn for k, v in self._secret_configurations.items()}
                if self._secret_configurations else None
        }

    def from_json(self, property_values):
        self._configuration_name = property_values['ConfigurationName']
        self._namespace = property_values['Namespace']
        self._config = property_values['ClusterConfiguration']
        self._description = property_values.get('Description', None)
        self._override_interfaces = property_values['OverrideInterfaces']
        self._configuration_artifacts = property_values['ConfigurationArtifacts']

        secret_configurations = property_values.get('SecretConfigurations', None)
        self._secret_configurations = \
            {k: secretsmanager.Secret.from_secret_arn(
                self, f'Secret_{k}', v) for k, v in secret_configurations.items()} \
            if secret_configurations else None

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
        configurations = [] if configurations is None else configurations
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

    def add_spark_package(self, package: str):
        if self._rehydrated:
            raise ReadOnlyClusterConfigurationError()

        self._spark_packages.append(package)
        config = self.config
        config['Configurations'] = self.update_configurations(
            config['Configurations'], 'spark-defaults', {
                'spark.jars.packages': ','.join(self._spark_packages)
            })
        self.update_config(config)
        return self

    def add_spark_jars(self, code: emr_code.EMRCode, jars_in_code: List[str]):
        if self._rehydrated:
            raise ReadOnlyClusterConfigurationError()

        self._configuration_artifacts.append({
            'Bucket': code.deployment_bucket.bucket_name,
            'Path': os.path.join(code.deployment_prefix, '*')
        })

        # We use a nested Construct to avoid Construct id collisions
        # First generate an ID for the Construct from bucket_name and deployment_prefix
        # We use a Hash to avoid potential problems with Tokens in the bucket_name
        hasher = hashlib.md5()
        hasher.update(os.path.join(code.deployment_bucket.bucket_name, code.deployment_prefix).encode('utf-8'))
        token = base64.urlsafe_b64encode(hasher.digest()).decode()
        construct_id = f'EmrCode_SparkJar_{token}'

        # Then attempt to find a previous Construct with this id
        construct = self.node.try_find_child(construct_id)
        # If we didn't find a previous Construct, construct a new one
        construct = core.Construct(self, construct_id) if construct is None else construct

        bucket_path = code.resolve(construct)['S3Path']
        for jar in jars_in_code:
            self._spark_jars.append(os.path.join(bucket_path, jar))
        config = self.config
        config['Configurations'] = self.update_configurations(
            config['Configurations'], 'spark-defaults', {
                'spark.jars': ','.join(self._spark_jars)
            })
        self.update_config(config)
        return self

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
    def override_interfaces(self) -> Dict[str, Dict[str, Dict[str, str]]]:
        return self._override_interfaces

    @property
    def configuration_artifacts(self) -> List[Dict[str, str]]:
        return self._configuration_artifacts

    @property
    def secret_configurations(self) -> Dict[str, secretsmanager.Secret]:
        return self._secret_configurations

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
        cluster_config._rehydrated = True
        return cluster_config
