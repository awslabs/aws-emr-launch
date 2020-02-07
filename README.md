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

Finally install this project's dependencies using

```sh
pipenv install '-e .'
```


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
