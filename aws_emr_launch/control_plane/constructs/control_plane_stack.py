from typing import Any

import aws_cdk

from aws_emr_launch import __product__, __version__
from aws_emr_launch.control_plane.constructs.lambdas import apis


class ControlPlaneStack(aws_cdk.Stack):
    def __init__(self, app: aws_cdk.App, name: str = "aws-emr-launch-control-plane", **kwargs: Any) -> None:
        super().__init__(app, name, **kwargs)
        self.tags.set_tag("deployment:product:name", __product__)
        self.tags.set_tag("deployment:product:version", __version__)
        self._apis = apis.Apis(self, "Apis")

    @property
    def apis(self) -> apis.Apis:
        return self._apis
