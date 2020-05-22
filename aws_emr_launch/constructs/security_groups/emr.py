from typing import Optional

from aws_cdk import aws_ec2 as ec2
from aws_cdk import core

from aws_emr_launch.constructs.base import BaseConstruct


class EMRSecurityGroups(BaseConstruct):

    def __init__(self, scope: core.Construct, id: str, *, vpc: Optional[ec2.Vpc] = None) -> None:
        super().__init__(scope, id)

        if vpc:
            self._master_group = ec2.SecurityGroup(self, 'MasterGroup', allow_all_outbound=True, vpc=vpc)
            self._workers_group = ec2.SecurityGroup(self, 'WorkersGroup', allow_all_outbound=True, vpc=vpc)
            self._service_group = ec2.SecurityGroup(self, 'ServiceGroup', allow_all_outbound=False, vpc=vpc)

            # Master SG rules
            self._master_group.add_ingress_rule(self._service_group, ec2.Port.tcp(8443))
            self._set_common_ingress_rules(self._master_group, self.workers_group)

            # Workers SG rules
            self._workers_group.add_ingress_rule(self._service_group, ec2.Port.tcp(8443))
            self._set_common_ingress_rules(self._workers_group, self._master_group)

            # Service SG rules
            self._service_group.add_egress_rule(self._master_group, ec2.Port.tcp(8443))
            self._service_group.add_egress_rule(self._workers_group, ec2.Port.tcp(8443))

    @staticmethod
    def _set_common_ingress_rules(primary: ec2.SecurityGroup, secondary: ec2.SecurityGroup) -> ec2.SecurityGroup:
        primary.add_ingress_rule(primary, ec2.Port.tcp_range(0, 65535))
        primary.add_ingress_rule(primary, ec2.Port.udp_range(0, 65535))
        primary.add_ingress_rule(primary, ec2.Port.icmp_type(-1))
        primary.add_ingress_rule(secondary, ec2.Port.tcp_range(0, 65535))
        primary.add_ingress_rule(secondary, ec2.Port.udp_range(0, 65535))
        primary.add_ingress_rule(secondary, ec2.Port.icmp_type(-1))
        return primary

    @staticmethod
    def from_security_group_ids(scope: core.Construct, id: str, master_group_id: str, workers_group_id: str,
                                service_group_id: str, mutable: Optional[bool] = None):
        security_groups = EMRSecurityGroups(scope, id)
        security_groups._master_group = ec2.SecurityGroup.from_security_group_id(
            security_groups, 'MasterGroup', master_group_id, mutable=mutable)
        security_groups._workers_group = ec2.SecurityGroup.from_security_group_id(
            security_groups, 'WorkersGroup', workers_group_id, mutable=mutable)
        security_groups._service_group = ec2.SecurityGroup.from_security_group_id(
            security_groups, 'ServiceGroup', service_group_id, mutable=False)
        return security_groups

    @property
    def master_group(self) -> ec2.SecurityGroup:
        return self._master_group

    @property
    def workers_group(self) -> ec2.SecurityGroup:
        return self._workers_group

    @property
    def service_group(self) -> ec2.SecurityGroup:
        return self._service_group
