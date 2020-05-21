import pkg_resources

__product__ = 'aws-emr-launch'
__version__ = pkg_resources.get_distribution(__product__).version
__package__ = f'{__product__}-{__version__}'
