# EMR Launch

## READ THIS FIRST
__This project is currently in Beta testing with select customers. 
It is considered INTERNAL ONLY and should not be shared with customers outside of a paid ProServe engagement.__

If you're interested in using this library on an engagement, contact __chamcca@__  

## Development
See the __docs/__!

And the __examples/__...


### Install Project Dependencies

Install [aws cdk](https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html)
```sh
npm install -g aws-cdk
```

Install [pipenv](https://github.com/pypa/pipenv)

Then install this project's development dependencies using

```sh
pipenv install --dev
```

Install this project's dependencies using

```sh
pipenv install '-e .'
```

#### Installing New Layer Packages
The following will install 
1. Update the `lambda_layer_requirements.txt` adding the new package(s)
2. Install new package(s): `pipenv run pip install -r lambda_layer_requirements.txt --target=aws_emr_launch/lambda_sources/layers/emr_config_utils/python/lib/python3.7/site-packages/`
   - This will skip upgrades of previously installed packages

#### Updating Lambda Layer Packages
To Update the Lambda Layer packages it is recommended that you first delete the entire layer contents to eliminate bloat.
1. Remove packages: `rm -fr aws_emr_launch/lambda_sources/layers/emr_config_utils/*`
2. Update the `lambda_layer_requirements.txt`
3. Reinstall packages: `pipenv run pip install -r lambda_layer_requirements.txt --target=aws_emr_launch/lambda_sources/layers/emr_config_utils/python/lib/python3.7/site-packages/`

### Testing

To run the test suite (be sure to deactivate the examples virtualenv)
```sh
pipenv run pytest
```

#### After running tests

View test coverage reports by opening `htmlcov/index.html` in your web browser.

#### To write a test
* start a file named test_[the module you want to test].py
* import the module you want to test at the top of the file
* write test case functions that match either `test_*` or `*_test`

For more information refer to [pytest docs](https://docs.pytest.org/en/latest/getting-started.html)
