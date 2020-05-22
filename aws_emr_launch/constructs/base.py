import os

from aws_cdk import core
from logzero import logger

from aws_emr_launch import __product__, __version__


def _tag_construct(construct: core.Construct):
    suppress_tags = os.environ.get('SUPPRESS_EMR_LAUNCH_DEPLOYMENT_TAGS', '').lower() in \
                    ('1', 't', 'true', 'y', 'yes')

    if not suppress_tags:
        core.Tag.add(construct, 'deployment:product:name', __product__)
        core.Tag.add(construct, 'deployment:product:version', __version__)
    else:
        logger.info('Suppressing "deployment:product" tags for: %s', construct.node.id)


class BaseConstruct(core.Construct):
    def __init__(self, scope: core.Construct, id: str):
        super().__init__(scope, id)
        _tag_construct(self)


class BaseBuilder:
    @staticmethod
    def tag_construct(construct: core.Construct):
        _tag_construct(construct)
