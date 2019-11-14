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

## Writing Code

#### Writing new template files


#### Running Code

To run a command either

```
$ pipenv run [YOUR COMMAND]
```
or start a shell in your pipenv using

```
$ pipenv shell
```

View all stacks in `app.py` with
```
$ pipenv run cdk ls
```

Deploy a stack with
```
$ pipenv run cdk deploy [NAME-OF-YOUR-STACK]
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
