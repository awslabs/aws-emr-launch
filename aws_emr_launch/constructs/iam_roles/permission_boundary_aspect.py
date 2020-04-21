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

import jsii

from typing import Union
from aws_cdk import aws_iam, core

from jsii._reference_map import _refs
from jsii._utils import Singleton


@jsii.implements(core.IAspect)
class PermissionBoundaryAspect:
    """
    This aspect finds all aws_iam.Role objects in a node (ie. CDK stack) and sets permission boundary to the given ARN.
    """

    def __init__(self, permission_boundary: Union[aws_iam.ManagedPolicy, str]) -> None:
        """
        :param permission_boundary: Either aws_iam.ManagedPolicy object or managed policy's ARN string
        """
        self.permission_boundary = permission_boundary

    def visit(self, construct_ref: core.IConstruct) -> None:
        """
        construct_ref only contains a string reference to an object. To get the actual object,
         we need to resolve it using JSII mapping.
        :param construct_ref: ObjRef object with string reference to the actual object.
        :return: None
        """
        if isinstance(construct_ref, jsii._kernel.ObjRef) and hasattr(construct_ref, 'ref'):
            kernel = Singleton._instances[jsii._kernel.Kernel]  # The same object is available as: jsii.kernel
            resolve = _refs.resolve(kernel, construct_ref)
        else:
            resolve = construct_ref

        def _walk(obj):
            if isinstance(obj, aws_iam.Role):
                cfn_role = obj.node.find_child('Resource')
                policy_arn = self.permission_boundary \
                    if isinstance(self.permission_boundary, str) \
                    else self.permission_boundary.managed_policy_arn
                cfn_role.add_property_override('PermissionsBoundary', policy_arn)
            else:
                if hasattr(obj, 'permissions_node'):
                    for c in obj.permissions_node.children:
                        _walk(c)
                if hasattr(obj, 'node') and obj.node.children:
                    for c in obj.node.children:
                        _walk(c)

        _walk(resolve)
