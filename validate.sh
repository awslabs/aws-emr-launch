#!/usr/bin/env bash

set -ex

isort --check .
black --check .
mypy --no-incremental .
flake8 .
