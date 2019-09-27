# Copyright 2019 Amazon.com, Inc. and its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the 'License').
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#   http://aws.amazon.com/asl/
#
# or in the 'license' file accompanying this file. This file is distributed
# on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

from aws_cdk import (
    aws_ec2 as ec2,
    core
)


class EMRSecurityGroups(core.Construct):

    def __init__(self, scope: core.Construct, id: str, vpc: ec2.Vpc) -> None:
        super().__init__(scope, id)

        self._vpc = vpc
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

    def _set_common_ingress_rules(self, primary: ec2.SecurityGroup, secondary: ec2.SecurityGroup):
        primary.add_ingress_rule(primary, ec2.Port.tcp_range(0, 65535))
        primary.add_ingress_rule(primary, ec2.Port.udp_range(0, 65535))
        primary.add_ingress_rule(primary, ec2.Port.icmp_type(-1))
        primary.add_ingress_rule(secondary, ec2.Port.tcp_range(0, 65535))
        primary.add_ingress_rule(secondary, ec2.Port.udp_range(0, 65535))
        primary.add_ingress_rule(secondary, ec2.Port.icmp_type(-1))

    @property
    def vpc(self):
        return self._vpc

    @property
    def master_group(self):
        return self._master_group

    @property
    def workers_group(self):
        return self._workers_group

    @property
    def service_group(self):
        return self._service_group
