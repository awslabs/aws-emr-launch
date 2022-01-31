import boto3
import botocore
import pkg_resources

__product__ = "aws-emr-launch"
__version__ = pkg_resources.get_distribution(__product__).version
__package__ = f"{__product__}-{__version__}"


def _get_botocore_config() -> botocore.config.Config:
    return botocore.config.Config(
        retries={"max_attempts": 5},
        connect_timeout=10,
        max_pool_connections=10,
        user_agent_extra=f"{__product__}/{__version__}",
    )


def boto3_client(service_name: str) -> boto3.client:
    return boto3.Session().client(service_name=service_name, use_ssl=True, config=_get_botocore_config())


def boto3_resource(service_name: str) -> boto3.client:
    return boto3.Session().resource(service_name=service_name, use_ssl=True, config=_get_botocore_config())
