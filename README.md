# EMR Launch
A library to simplify the definition and management of Amazon EMR Fleets.

See the __docs/__!

And the __examples/__...

## Usage
This library is acts as a plugin to the [AWS CDK](https://aws.amazon.com/cdk/) providing additional L2 Constructs. 
To avoid circular references with CDK dependencies, this package will not install CDK and Boto3. Instead it expects 
these packages to already be installed. 

It is recommended that a Python3 `venv` be used for all CDK builds and deployments.

To get up and running quickly:

1. Install the [CDK CLI](https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html)
   ```bash
   npm install -g aws-cdk
   ```

2. Use your mechanism of choice to create and activate a Python3 `venv`:
   ```bash
   python3 -m venv .env
   source .env/bin/activate
   ```

3. Install the CDK and Boto3 minimum requirements:
   ```bash
   pip install -r requirements.txt
   ```

4. Install `aws-emr-launch` package (package is currently installed from a `wheel` file):
   ```bash
   pip install aws-emr-launch
   ```


## Development
Follow Step 1 - 3 above to configure an environment and install requirements

### Install development requirements
After activating your `venv`:
```bash
pip install -r requirements-dev.txt
```

#### Installing New Layer Packages
The following will install the Lambda Layer packages 
1. Update the `requirements-lambda-layer.txt` adding the new package(s)
2. Install new package(s): `pip install -r requirements-lambda-layer.txt --target=aws_emr_launch/lambda_sources/layers/emr_config_utils/python/lib/python3.7/site-packages/`
   - This will skip upgrades of previously installed packages

#### Updating Lambda Layer Packages
To Update the Lambda Layer packages it is recommended that you first delete the entire layer contents to eliminate bloat.
1. Remove packages: `rm -fr aws_emr_launch/lambda_sources/layers/emr_config_utils/*`
2. Update the `requirements-lambda-layer.txt`
3. Reinstall packages: `pip install -r requirements-lambda-layer.txt --target=aws_emr_launch/lambda_sources/layers/emr_config_utils/python/lib/python3.7/site-packages/`


### Testing
To run the test suite (from within the `venv`):
```bash
pytest
```

#### After running tests
View test coverage reports by opening `htmlcov/index.html` in your web browser.

#### To write a test
* start a file named test_[the module you want to test].py
* import the module you want to test at the top of the file
* write test case functions that match either `test_*` or `*_test`

For more information refer to [pytest docs](https://docs.pytest.org/en/latest/getting-started.html)
