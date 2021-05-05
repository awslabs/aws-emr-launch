import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="infrastructure",
    version="0.0.1",

    description="A CDK Python app for building EMR Pipelines running spark jobs",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="Kevin Soucy - Amazon Web Services",

    package_dir={"": "infrastructure"},
    packages=setuptools.find_packages(where="infrastructure"),

    install_requires=[
        "aws-cdk.core==1.51.0",
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
