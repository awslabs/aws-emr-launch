import setuptools

with open('VERSION', 'r') as version_file:
    version = version_file.read().strip()

with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="aws-emr-launch",
    version=version,

    description="EMR Launch modules",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="author",

    package_dir={"aws_emr_launch": "aws_emr_launch"},
    packages=setuptools.find_packages(),

    install_requires=[
        "boto3",
        "aws-cdk.core",
        "aws-cdk-aws.iam",
        "aws-cdk-aws.s3",
        "aws-cdk-aws.kms",
        "aws-cdk-aws.ec2",
        "aws-cdk-aws.emr",
        "aws-cdk-aws.sns",
        "aws-cdk-aws.sqs",
        "aws-cdk-aws.ssm",
        "aws-cdk-aws.lambda",
        "aws-cdk-aws.stepfunctions",
        "aws-cdk-aws.stepfunctions-tasks",
        "aws-cdk-aws.events",
        "aws-cdk-aws.events-targets"
    ],

    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "License :: OSI Approved :: Apache Software License",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
