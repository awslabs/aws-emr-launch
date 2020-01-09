# EMR Launch Modules

## Running the Modules

#### Install Project Dependencies

Install [aws cdk](https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html)
```
npm install -g aws-cdk
```

Install [pipenv](https://github.com/pypa/pipenv)

Then install this project's development dependencies using

```
$ pipenv install --dev
```

Then install this project's dependencies using

```
$ pipenv install '-e .'
```

## Running the examples/
The examples first require deployment of the `control_plane` and some customization of `s3.Buckets` and `vpc.Vpc` for the account/region to be deployed to.

Deploy examples in the following order:
1. `control_plane`
2. `emr_profiles`
3. `cluster_configurations`
4. `emr_launch_functions`
5. `transient_cluster_pipeline`
6. `persistent_cluster_pipeline`
7. `sns_triggered_pipeline`

Create and activate a virtualenv for the examples:
```
$ cd examples/
$ python3 -m venv .env
$ source .env/bin/activate
```

Install the `aws-emr-launch` library and dependencies:
```
$ pip install ..
```

Deploy the `control_plane` (this only needs to be done once or after updates to the `control_plane`):
```
$ cd control_plane
$ cdk deploy
```

Deploy an example:
```
$ cd emr_profiles/
$ cdk deploy
```

## Testing

To run the test suite
```
$ pipenv run pytest
```

### After running tests

View test coverage reports by opening `htmlcov/index.html` in your web browser.

### To write a test
* start a file named test_[the module you want to test].py
* import the module you want to test at the top of the file
* write test case functions that match either `test_*` or `*_test`

For more information refer to [pytest docs](https://docs.pytest.org/en/latest/getting-started.html)
