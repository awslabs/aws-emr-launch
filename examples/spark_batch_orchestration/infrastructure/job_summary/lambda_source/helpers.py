import re


def extract_region_from_arn(arn):
    return re.match(r"arn:[\w\-]+:\w+:([\w\-]+):.*", arn).group(1)


def parse_s3_uri(s3_uri):
    match = re.match(r"s3.?://([\w\-]+)/(.+)$", s3_uri)
    bucket_name = match.group(1)
    object_key = match.group(2)

    return bucket_name, object_key


def make_s3_console_link(bucket_name, object_key):
    return f"https://s3.console.aws.amazon.com/s3/object/{bucket_name}?prefix={object_key}"
