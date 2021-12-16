import argparse
import sys

from pyspark.sql import SparkSession  # noqa


def parse_arguments(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-s3-path", required=True)
    return parser.parse_args(args=args)


def main(argv):
    arguments = parse_arguments(argv)
    spark = SparkSession.builder.appName("demo-spark-app").getOrCreate()
    df = spark.createDataFrame([(x,) for x in range(1000)], ["integers"])
    df.write.save(arguments.output_s3_path, format="csv", mode="append", header=True)


if __name__ == "__main__":
    main(sys.argv[1:])
