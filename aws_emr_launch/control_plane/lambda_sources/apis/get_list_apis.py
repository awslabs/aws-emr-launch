import json
import logging
import os
import traceback
from typing import Any, Dict, Optional, cast

import boto3
import botocore
from botocore.exceptions import ClientError

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


PROFILES_SSM_PARAMETER_PREFIX = "/emr_launch/emr_profiles"
CONFIGURATIONS_SSM_PARAMETER_PREFIX = "/emr_launch/cluster_configurations"
FUNCTIONS_SSM_PARAMETER_PREFIX = "/emr_launch/emr_launch_functions"


class EMRProfileNotFoundError(Exception):
    pass


class ClusterConfigurationNotFoundError(Exception):
    pass


class EMRLaunchFunctionNotFoundError(Exception):
    pass


def _get_botocore_config() -> botocore.config.Config:
    product = os.environ.get("AWS_EMR_LAUNCH_PRODUCT", "")
    version = os.environ.get("AWS_EMR_LAUNCH_VERSION", "")
    return botocore.config.Config(
        retries={"max_attempts": 5},
        connect_timeout=10,
        max_pool_connections=10,
        user_agent_extra=f"{product}/{version}",
    )


def _boto3_client(service_name: str) -> boto3.client:
    return boto3.Session().client(service_name=service_name, use_ssl=True, config=_get_botocore_config())


ssm = _boto3_client("ssm")


def _get_parameter_values(
    ssm_parameter_prefix: str, top_level_return: str, namespace: str = "default", next_token: Optional[str] = None
) -> Dict[str, Any]:
    params = {"Path": f"{ssm_parameter_prefix}/{namespace}/"}
    if next_token:
        params["NextToken"] = next_token
    result = ssm.get_parameters_by_path(**params)

    return_val: Dict[str, Any] = {top_level_return: [json.loads(p["Value"]) for p in result["Parameters"]]}
    if "NextToken" in result:
        return_val["NextToken"] = result["NextToken"]
    return return_val


def _get_parameter_value(ssm_parameter_prefix: str, name: str, namespace: str = "default") -> Dict[str, Any]:
    configuration_json = ssm.get_parameter(Name=f"{ssm_parameter_prefix}/{namespace}/{name}")["Parameter"]["Value"]
    return cast(Dict[str, Any], json.loads(configuration_json))


def _log_exception(e: Exception, event: Dict[str, Any]) -> None:
    trc = traceback.format_exc()
    s = "Error processing event {}: {}\n\n{}".format(str(event), str(e), trc)
    LOGGER.error(s)


def get_profiles_handler(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    LOGGER.info("Lambda metadata: {} (type = {})".format(json.dumps(event), type(event)))
    namespace = event.get("Namespace", "default")
    next_token = event.get("NextToken", None)

    try:
        return _get_parameter_values(PROFILES_SSM_PARAMETER_PREFIX, "EMRProfiles", namespace, next_token)

    except Exception as e:
        _log_exception(e, event)
        raise e


def get_profile_handler(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    LOGGER.info("Lambda metadata: {} (type = {})".format(json.dumps(event), type(event)))
    profile_name = event.get("ProfileName", "")
    namespace = event.get("Namespace", "default")

    try:
        return _get_parameter_value(PROFILES_SSM_PARAMETER_PREFIX, profile_name, namespace)

    except ClientError as e:
        if e.response["Error"]["Code"] == "ParameterNotFound":
            LOGGER.error(f"ProfileNotFound: {namespace}/{profile_name}")
            raise EMRProfileNotFoundError(f"ProfileNotFound: {namespace}/{profile_name}")
        else:
            _log_exception(e, event)
            raise e
    except Exception as e:
        _log_exception(e, event)
        raise e


def get_configurations_handler(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    LOGGER.info("Lambda metadata: {} (type = {})".format(json.dumps(event), type(event)))
    namespace = event.get("Namespace", "default")
    next_token = event.get("NextToken", None)

    try:
        return _get_parameter_values(
            CONFIGURATIONS_SSM_PARAMETER_PREFIX, "ClusterConfigurations", namespace, next_token
        )

    except Exception as e:
        _log_exception(e, event)
        raise e


def get_configuration_handler(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    LOGGER.info("Lambda metadata: {} (type = {})".format(json.dumps(event), type(event)))
    configuration_name = event.get("ConfigurationName", "")
    namespace = event.get("Namespace", "default")

    try:
        return _get_parameter_value(CONFIGURATIONS_SSM_PARAMETER_PREFIX, configuration_name, namespace)

    except ClientError as e:
        if e.response["Error"]["Code"] == "ParameterNotFound":
            LOGGER.error(f"ConfigurationNotFound: {namespace}/{configuration_name}")
            raise ClusterConfigurationNotFoundError(f"ConfigurationNotFound: {namespace}/{configuration_name}")
        else:
            _log_exception(e, event)
            raise e
    except Exception as e:
        _log_exception(e, event)
        raise e


def get_functions_handler(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    LOGGER.info("Lambda metadata: {} (type = {})".format(json.dumps(event), type(event)))
    namespace = event.get("Namespace", "default")
    next_token = event.get("NextToken", None)

    try:
        return _get_parameter_values(FUNCTIONS_SSM_PARAMETER_PREFIX, "EMRLaunchFunctions", namespace, next_token)

    except Exception as e:
        _log_exception(e, event)
        raise e


def get_function_handler(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    LOGGER.info("Lambda metadata: {} (type = {})".format(json.dumps(event), type(event)))
    function_name = event.get("FunctionName", "")
    namespace = event.get("Namespace", "default")

    try:
        return _get_parameter_value(FUNCTIONS_SSM_PARAMETER_PREFIX, function_name, namespace)

    except ClientError as e:
        if e.response["Error"]["Code"] == "ParameterNotFound":
            LOGGER.error(f"FunctionNotFound: {namespace}/{function_name}")
            raise EMRLaunchFunctionNotFoundError(f"FunctionNotFound: {namespace}/{function_name}")
        else:
            _log_exception(e, event)
            raise e
    except Exception as e:
        _log_exception(e, event)
        raise e
