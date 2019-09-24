from aws_cdk import (
    aws_ec2 as ec2,
    core
)


class EMRSecurityGroups(core.Construct):

    def __init__(self, scope: core.Construct, id: str, vpc: ec2.Vpc) -> None:
        super().__init__(scope, id)

        self._service_group = ec2.SecurityGroup(self, 'ServiceGroup', allow_all_outbound=False, vpc=vpc)
        self._master_group = ec2.SecurityGroup(self, 'MasterGroup', allow_all_outbound=True, vpc=vpc)
        self._workers_group = ec2.SecurityGroup(self, 'WorkerGroup', allow_all_outbound=True, vpc=vpc)

    @property
    def service_group(self):
        return self._service_group

    @property
    def master_group(self):
        return self._master_group

    @property
    def workers_group(self):
        return self._workers_group
