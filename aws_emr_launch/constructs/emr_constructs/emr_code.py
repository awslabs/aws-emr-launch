import enum
import glob
import os
from abc import abstractmethod
from typing import Any, Dict, List, Optional

from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3_deployment
from aws_cdk import core


class StepFailureAction(enum.Enum):
    TERMINATE_JOB_FLOW = 'TERMINATE_JOB_FLOW'
    TERMINATE_CLUSTER = 'TERMINATE_CLUSTER'
    CANCEL_AND_WAIT = 'CANCEL_AND_WAIT'
    CONTINUE = 'CONTINUE'


class Resolvable:
    @abstractmethod
    def resolve(self, scope: core.Construct) -> Dict[str, Any]:
        ...


class EMRCode(Resolvable):
    def __init__(self, *, deployment_props: s3_deployment.BucketDeploymentProps, id: Optional[str] = None):
        self._deployment_props = deployment_props
        self._deployment_bucket = deployment_props.destination_bucket
        self._deployment_prefix = deployment_props.destination_key_prefix
        self._id = id
        self._bucket_deployment = None

    def resolve(self, scope: core.Construct) -> Dict[str, Any]:
        # If the same deployment is used multiple times, retain only the first instantiation
        if self._bucket_deployment is None:
            # Convert BucketDeploymentProps to dict
            deployment_props = vars(self._deployment_props)['_values']
            self._bucket_deployment = s3_deployment.BucketDeployment(
                scope,
                f'{self._id}_BucketDeployment' if self._id else 'BucketDeployment',
                **deployment_props)

        return {'S3Path': self.s3_path}

    @property
    def deployment_bucket(self) -> s3.IBucket:
        return self._deployment_bucket

    @property
    def deployment_prefix(self):
        return self._deployment_prefix

    @property
    def s3_path(self) -> str:
        return os.path.join(f's3://{self._deployment_bucket.bucket_name}', self._deployment_prefix)


class Code:
    @staticmethod
    def from_path(path: str, deployment_bucket: s3.Bucket,
                  deployment_prefix: str, id: Optional[str] = None) -> EMRCode:
        return EMRCode(id=id, deployment_props=s3_deployment.BucketDeploymentProps(
            sources=[s3_deployment.Source.asset(path)],
            destination_bucket=deployment_bucket,
            destination_key_prefix=deployment_prefix))

    @staticmethod
    def from_props(deployment_props: s3_deployment.BucketDeploymentProps, id: Optional[str] = None):
        return EMRCode(id=id, deployment_props=deployment_props)

    @staticmethod
    def files_in_path(path: str, filter: str = '*.*'):
        search_path = os.path.join(path, '')
        files = glob.glob(os.path.join(search_path, f'**/{filter}'), recursive=True)
        return [f.replace(search_path, '') for f in files]


class EMRBootstrapAction(Resolvable):
    def __init__(self, name: str, path: str, args: Optional[List[str]] = None, code: Optional[EMRCode] = None):
        self._name = name
        self._path = path
        self._args = args
        self._code = code

    def resolve(self, scope: core.Construct) -> Dict[str, Any]:
        if self._code is not None:
            self._code.resolve(scope)

        return {
            'Name': self._name,
            'ScriptBootstrapAction': {
                'Path': self._path,
                'Args': self._args if self._args else []
            }
        }

    @property
    def name(self) -> str:
        return self._name

    @property
    def path(self) -> str:
        return self._path

    @property
    def args(self) -> Optional[List[str]]:
        return self._args

    @property
    def code(self) -> Optional[EMRCode]:
        return self._code


class EMRStep(Resolvable):
    def __init__(self, name: str, jar: str, main_class: Optional[str] = None, args: Optional[List[str]] = None,
                 action_on_failure: StepFailureAction = StepFailureAction.CONTINUE,
                 properties: Optional[Dict[str, str]] = None, code: Optional[EMRCode] = None):
        self._name = name
        self._jar = jar
        self._main_class = main_class
        self._args = args
        self._action_on_failure = action_on_failure
        self._properties = properties
        self._code = code

    def resolve(self, scope: core.Construct) -> Dict[str, Any]:
        if self._code is not None:
            self._code.resolve(scope)

        return {
            'Name': self._name,
            'ActionOnFailure': self._action_on_failure.name,
            'HadoopJarStep': {
                'Jar': self._jar,
                'MainClass': self._main_class,
                'Args': self._args if self._args else [],
                'Properties': [{'Key': k, 'Value': v} for k, v in self._properties.items()] if self._properties else []
            }
        }

    @property
    def name(self) -> str:
        return self._name

    @property
    def args(self) -> Optional[List[str]]:
        return self._args
