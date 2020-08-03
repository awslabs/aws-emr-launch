import json
from typing import Dict, List, Optional, Union

import boto3
from aws_cdk import aws_lambda
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_ssm as ssm
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import core
from botocore.exceptions import ClientError
from logzero import logger

from aws_emr_launch import __product__, __version__
from aws_emr_launch.constructs.base import BaseConstruct
from aws_emr_launch.constructs.emr_constructs import (cluster_configuration,
                                                      emr_profile)
from aws_emr_launch.constructs.step_functions import emr_chains, emr_tasks

SSM_PARAMETER_PREFIX = '/emr_launch/emr_launch_functions'


class EMRLaunchFunctionNotFoundError(Exception):
    pass


class EMRLaunchFunction(BaseConstruct):
    def __init__(self, scope: core.Construct, id: str, *,
                 launch_function_name: str,
                 emr_profile: emr_profile.EMRProfile,
                 cluster_configuration: cluster_configuration.ClusterConfiguration,
                 cluster_name: str = None,
                 namespace: str = 'default',
                 default_fail_if_cluster_running: bool = False,
                 success_topic: Optional[sns.Topic] = None,
                 failure_topic: Optional[sns.Topic] = None,
                 override_cluster_configs_lambda: Optional[aws_lambda.Function] = None,
                 allowed_cluster_config_overrides: Optional[Dict[str, Dict[str, str]]] = None,
                 description: Optional[str] = None,
                 cluster_tags: Union[List[core.Tag], Dict[str, str], None] = None,
                 wait_for_cluster_start: bool = True) -> None:
        super().__init__(scope, id)

        if launch_function_name is None:
            return

        self._launch_function_name = launch_function_name
        self._namespace = namespace
        self._emr_profile = emr_profile
        self._cluster_configuration = cluster_configuration
        self._cluster_name = cluster_name
        self._default_fail_if_cluster_running = default_fail_if_cluster_running
        self._success_topic = success_topic
        self._failure_topic = failure_topic
        self._override_cluster_configs_lambda = override_cluster_configs_lambda
        self._allowed_cluster_config_overrides = allowed_cluster_config_overrides
        self._description = description
        self._wait_for_cluster_start = wait_for_cluster_start

        if isinstance(cluster_tags, dict):
            self._cluster_tags = [core.Tag(k, v) for k, v in cluster_tags.items()]
        elif isinstance(cluster_tags, list):
            self._cluster_tags = cluster_tags
        else:
            self._cluster_tags = []

        self._cluster_tags.extend([
            core.Tag('deployment:product:name', __product__),
            core.Tag('deployment:product:version', __version__)
        ])

        if len(cluster_configuration.configuration_artifacts) > 0:
            if emr_profile.mutable_instance_role:
                for configuration_artifact in cluster_configuration.configuration_artifacts:
                    bucket_name = configuration_artifact['Bucket']
                    path = configuration_artifact['Path']
                    bucket = self.node.try_find_child(f'Bucket_{bucket_name}')
                    bucket = s3.Bucket.from_bucket_name(self, f'Bucket_{bucket_name}', bucket_name) \
                        if bucket is None else bucket
                    bucket.grant_read(emr_profile.roles.instance_role, path)
            else:
                logger.warn('--------------------------------------------------------------------------')
                logger.warn('Unable to authorize the artifacts in the ClusterConfiguration')
                logger.warn(f'The EMRProfile {emr_profile.profile_name} has an immutable Instance Role')
                logger.warn('Use of these artifacts will require direct authorization on the EMRProfile')
                logger.warn('--------------------------------------------------------------------------')

        fail = emr_chains.Fail(
            self, 'FailChain',
            message=sfn.TaskInput.from_data_at('$.Error'),
            subject='EMR Launch Function Failure',
            topic=failure_topic,
            error='Failed to Launch Cluster',
            cause='See Execution Event "FailStateEntered" for complete error cause')

        # Create Task for loading the cluster configuration from Parameter Store
        load_cluster_configuration = emr_tasks.LoadClusterConfigurationBuilder.build(
            self, 'LoadClusterConfigurationTask',
            cluster_name=cluster_name,
            cluster_tags=self._cluster_tags,
            profile_namespace=emr_profile.namespace,
            profile_name=emr_profile.profile_name,
            configuration_namespace=cluster_configuration.namespace,
            configuration_name=cluster_configuration.configuration_name)
        load_cluster_configuration.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

        # Create Task for overriding cluster configurations
        override_cluster_configs = emr_tasks.OverrideClusterConfigsBuilder.build(
            self, 'OverrideClusterConfigsTask',
            override_cluster_configs_lambda=override_cluster_configs_lambda,
            allowed_cluster_config_overrides=allowed_cluster_config_overrides)
        # Attach an error catch to the Task
        override_cluster_configs.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

        # Create Task to conditionally fail if a cluster with this name is already
        # running, based on user input
        fail_if_cluster_running = emr_tasks.FailIfClusterRunningBuilder.build(
            self, 'FailIfClusterRunningTask',
            default_fail_if_cluster_running=default_fail_if_cluster_running,)
        # Attach an error catch to the task
        fail_if_cluster_running.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

        # Create a Task for updating the cluster tags at runtime
        update_cluster_tags = emr_tasks.UpdateClusterTagsBuilder.build(
            self, 'UpdateClusterTagsTask')
        # Attach an error catch to the Task
        update_cluster_tags.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

        # Create a Task to create the cluster
        if cluster_configuration.secret_configurations is None and emr_profile.kerberos_attributes_secret is None:
            # Use a the standard Step Functions/EMR integration to create the cluster
            create_cluster = emr_tasks.CreateClusterBuilder.build(
                self, 'CreateClusterTask',
                roles=emr_profile.roles,
                result_path='$.LaunchClusterResult',
                wait_for_cluster_start=wait_for_cluster_start)
        else:
            # Use the RunJobFlow Lambda to create the cluster to avoid exposing the
            # SecretConfigurations and KerberosAttributes values
            create_cluster = emr_tasks.RunJobFlowBuilder.build(
                self, 'CreateClusterTask',
                roles=emr_profile.roles,
                kerberos_attributes_secret=emr_profile.kerberos_attributes_secret,
                secret_configurations=cluster_configuration.secret_configurations,
                result_path='$.LaunchClusterResult',
                wait_for_cluster_start=wait_for_cluster_start)

        # Attach an error catch to the Task
        create_cluster.add_catch(fail, errors=['States.ALL'], result_path='$.Error')

        success = emr_chains.Success(
            self, 'SuccessChain',
            message=sfn.TaskInput.from_data_at('$.LaunchClusterResult'),
            subject='Launch EMR Config Succeeded',
            topic=success_topic,
            output_path='$')

        definition = sfn.Chain \
            .start(load_cluster_configuration) \
            .next(override_cluster_configs) \
            .next(fail_if_cluster_running) \
            .next(update_cluster_tags) \
            .next(create_cluster) \
            .next(success)

        self._state_machine = sfn.StateMachine(
            self, 'StateMachine',
            state_machine_name=f'{namespace}_{launch_function_name}', definition=definition)

        self._ssm_parameter = ssm.CfnParameter(
            self, 'SSMParameter',
            type='String',
            value=json.dumps(self.to_json()),
            tier='Intelligent-Tiering',
            name=f'{SSM_PARAMETER_PREFIX}/{namespace}/{launch_function_name}')

    def to_json(self):
        return {
            'LaunchFunctionName': self._launch_function_name,
            'Namespace': self._namespace,
            'EMRProfile':
                f'{self._emr_profile.namespace}/{self._emr_profile.profile_name}',
            'ClusterConfiguration':
                f'{self._cluster_configuration.namespace}/{self._cluster_configuration.configuration_name}',
            'ClusterName': self._cluster_name,
            'DefaultFailIfClusterRunning': self._default_fail_if_cluster_running,
            'SuccessTopic': self._success_topic.topic_arn
                if self._success_topic is not None
                else None,
            'FailureTopic': self._failure_topic.topic_arn
                if self._failure_topic is not None
                else None,
            'OverrideClusterConfigsLambda':
                self._override_cluster_configs_lambda.function_arn
                if self._override_cluster_configs_lambda is not None
                else None,
            'AllowedClusterConfigOverrides': self._allowed_cluster_config_overrides,
            'StateMachine': self._state_machine.state_machine_arn,
            'Description': self._description,
            'ClusterTags': [{'Key': t.key, 'Value': t.value} for t in self._cluster_tags],
            'WaitForClusterStart': self._wait_for_cluster_start
        }

    def from_json(self, property_values):
        self._launch_function_name = property_values['LaunchFunctionName']
        self._namespace = property_values['Namespace']

        profile_parts = property_values['EMRProfile'].split('/')
        self._emr_profile = emr_profile.EMRProfile.from_stored_profile(
            self, 'EMRProfile', profile_parts[1], profile_parts[0])
        config_parts = property_values['ClusterConfiguration'].split('/')
        self._cluster_configuration = cluster_configuration.ClusterConfiguration.from_stored_configuration(
            self, 'ClusterConfiguration', config_parts[1], config_parts[0])

        self._cluster_name = property_values['ClusterName']
        self._default_fail_if_cluster_running = property_values['DefaultFailIfClusterRunning']

        topic = property_values.get('SuccessTopic', None)
        self._success_topic = sns.Topic.from_topic_arn(self, 'SuccessTopic', topic) \
            if topic is not None \
            else None

        topic = property_values.get('FailureTopic', None)
        self._failure_topic = sns.Topic.from_topic_arn(self, 'FailureTopic', topic) \
            if topic is not None \
            else None

        func = property_values.get('OverrideClusterConfigsLambda', None)
        self._override_cluster_configs_lambda = aws_lambda.Function.from_function_arn(
            self, 'OverrideClusterConfigsLambda', func) \
            if func is not None \
            else None

        self._allowed_cluster_config_overrides = property_values.get('AllowedClusterConfigOverrides', None)
        self._description = property_values.get('Description', None)
        self._cluster_tags = [core.Tag(t['Key'], t['Value']) for t in property_values['ClusterTags']]

        state_machine = property_values['StateMachine']
        self._state_machine = sfn.StateMachine.from_state_machine_arn(self, 'StateMachine', state_machine)

        self._wait_for_cluster_start = property_values.get('WaitForClusterStart', None)
        return self

    @property
    def launch_function_name(self) -> str:
        return self._launch_function_name

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def emr_profile(self) -> emr_profile.EMRProfile:
        return self._emr_profile

    @property
    def cluster_configuration(self) -> cluster_configuration.ClusterConfiguration:
        return self._cluster_configuration

    @property
    def cluster_name(self) -> str:
        return self._cluster_name

    @property
    def default_fail_if_cluster_running(self) -> bool:
        return self._default_fail_if_cluster_running

    @property
    def success_topic(self) -> sns.Topic:
        return self._success_topic

    @property
    def failure_topic(self) -> sns.Topic:
        return self._failure_topic

    @property
    def override_cluster_configs_lambda(self) -> aws_lambda.Function:
        return self._override_cluster_configs_lambda

    @property
    def allowed_cluster_config_overrides(self) -> Optional[Dict[str, Dict[str, str]]]:
        return self._allowed_cluster_config_overrides

    @property
    def state_machine(self) -> sfn.StateMachine:
        return self._state_machine

    @property
    def description(self) -> str:
        return self._description

    @staticmethod
    def get_functions(namespace: str = 'default', next_token: Optional[str] = None,
                      ssm_client=None) -> Dict[str, any]:
        ssm_client = boto3.client('ssm') if ssm_client is None else ssm_client
        params = {
            'Path': f'{SSM_PARAMETER_PREFIX}/{namespace}/'
        }
        if next_token:
            params['NextToken'] = next_token
        result = ssm_client.get_parameters_by_path(**params)

        functions = {
            'EMRLaunchFunctions': [json.loads(p['Value']) for p in result['Parameters']]
        }
        if 'NextToken' in result:
            functions['NextToken'] = result['NextToken']
        return functions

    @staticmethod
    def get_function(launch_function_name: str, namespace: str = 'default',
                     ssm_client=None) -> Dict[str, any]:
        ssm_client = boto3.client('ssm') if ssm_client is None else ssm_client
        try:
            function_json = ssm_client.get_parameter(
                Name=f'{SSM_PARAMETER_PREFIX}/{namespace}/{launch_function_name}')['Parameter']['Value']
            return json.loads(function_json)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ParameterNotFound':
                raise EMRLaunchFunctionNotFoundError()
            else:
                raise e

    @staticmethod
    def from_stored_function(scope: core.Construct, id: str, launch_function_name: str, namespace: str = 'default'):
        stored_function = EMRLaunchFunction.get_function(launch_function_name, namespace)
        launch_function = EMRLaunchFunction(
            scope, id,
            launch_function_name=None,
            emr_profile=None,
            cluster_configuration=None)
        launch_function._launch_function_name = launch_function_name
        launch_function._namespace = namespace
        return launch_function.from_json(stored_function)
