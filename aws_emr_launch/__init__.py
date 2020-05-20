import pkg_resources

__version__ = pkg_resources.get_distribution("aws-emr-launch").version
__package__ = f'aws-emr-launch-{__version__}'
