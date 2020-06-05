import setuptools

with open('VERSION', 'r') as version_file:
    version = version_file.read().strip()

with open('README.md') as fp:
    long_description = fp.read()

boto3_version = '>=1.12.23'
cdk_version = '>=1.29.0'

setuptools.setup(
    name='aws-emr-launch',
    version=version,

    description='AWS EMR Launch modules',
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords=['aws'],

    author='chamcca',
    author_email='chamcca@amazon.com',
    url='https://code.amazon.com/packages/AWSProServe_project_EMRLaunch/trees/mainline',

    package_dir={'aws_emr_launch': 'aws_emr_launch'},
    packages=setuptools.find_packages(exclude=('tests',)),

    install_requires=[
        'logzero~=1.5.0',
    ],

    include_package_data=True,

    python_requires='>=3.6',

    classifiers=[
        'Development Status :: 5 - Production/Stable',

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
