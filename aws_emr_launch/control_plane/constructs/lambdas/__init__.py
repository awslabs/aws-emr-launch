import os

LAMBDA_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../lambda_sources/'))


def _lambda_path(path):
    return os.path.join(LAMBDA_DIR, path)
