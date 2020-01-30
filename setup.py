import setuptools

with open('VERSION', 'r') as version_file:
    version = version_file.read().strip()

with open('README.md') as fp:
    long_description = fp.read()

boto3_version = '>=1.10.34'
cdk_version = '>=1.22.0'

setuptools.setup(
    name='aws-emr-launch',
    version=version,

    description='EMR Launch modules',
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords=['aws'],

    author='chamcca',
    author_email='chamcca@amazon.com',
    url='https://code.amazon.com/packages/AWSProServe_project_EMRLaunch/trees/mainline',

    package_dir={'aws_emr_launch': 'aws_emr_launch'},
    packages=setuptools.find_packages(),

    install_requires=[
        f'boto3{boto3_version}',
        f'aws-cdk.core{cdk_version}',
        f'aws-cdk-aws.iam{cdk_version}',
        f'aws-cdk-aws.s3{cdk_version}',
        f'aws-cdk-aws.s3.deployment{cdk_version}',
        f'aws-cdk-aws.kms{cdk_version}',
        f'aws-cdk-aws.ec2{cdk_version}',
        f'aws-cdk-aws.emr{cdk_version}',
        f'aws-cdk-aws.sns{cdk_version}',
        f'aws-cdk-aws.sqs{cdk_version}',
        f'aws-cdk-aws.ssm{cdk_version}',
        f'aws-cdk-aws.lambda{cdk_version}',
        f'aws-cdk-aws.lambda-event-sources{cdk_version}',
        f'aws-cdk-aws.stepfunctions{cdk_version}',
        f'aws-cdk-aws.stepfunctions-tasks{cdk_version}',
        f'aws-cdk-aws.events{cdk_version}',
        f'aws-cdk-aws.events-targets{cdk_version}'
    ],

    include_package_data=True,

    python_requires='>=3.6',

    classifiers=[
        'Development Status :: 4 - Beta',

        'Intended Audience :: Developers',

        'License :: OSI Approved :: Apache Software License',

        'Programming Language :: JavaScript',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',

        'Topic :: Software Development :: Code Generators',
        'Topic :: Utilities',

        'Typing :: Typed',
    ],
)
