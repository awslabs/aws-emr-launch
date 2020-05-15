from aws_cdk import core

from aws_emr_launch.control_plane.constructs.lambdas import apis


class ControlPlaneStack(core.Stack):
    def __init__(self, app: core.App, name: str = 'aws-emr-launch-control-plane', **kwargs):
        super().__init__(app, name, **kwargs)

        self._apis = apis.Apis(self, 'Apis')

    @property
    def apis(self) -> apis.Apis:
        return self._apis
