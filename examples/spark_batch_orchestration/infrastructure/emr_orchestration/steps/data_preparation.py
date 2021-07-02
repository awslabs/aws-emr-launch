import argparse
import boto3
import functools
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import random
import sys

def union_all(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)


def parse_arguments(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-metadata-table-name", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--input-bucket", required=True)
    parser.add_argument("--region", required=True)
    return parser.parse_args(args=args)

def get_batch_file_metadata(table_name, batch_id, region):
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    response = table.query(
        KeyConditions={
                'BatchId': {
                    'AttributeValueList': [batch_id],
                    'ComparisonOperator': 'EQ'
                }
        }
    )
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.update(response['Items'])
    return data

def load_partition(spark, bucket, partition):
    s3path = "s3://" + bucket + "/" + partition + "/*" 
    df = spark.read.load(s3path)
    return df

def load_and_union_data(spark, batch_metadata, input_bucket):
    distinct_partitions = list(set([x["FilePartition"] for x in batch_metadata]))
    partition_dfs = {}
    for partition in distinct_partitions:
        dfs = [
            load_partition(
                spark,
                bucket= input_bucket,
                partition=partition
            )
            for x in batch_metadata
            if x["FilePartition"] == partition
        ]
        partition_dfs[partition] = union_all(dfs)

    return partition_dfs

def main(args, spark):
    arguments = parse_arguments(args)

    # Load metadata to process
    batch_metadata = get_batch_file_metadata(
        table_name=arguments.batch_metadata_table_name,
        batch_id=arguments.batch_id,
        region = arguments.region
    )

    input_bucket = arguments.input_bucket
    input_data = load_and_union_data(spark, batch_metadata, input_bucket)

    input_dfs = []
    for dataset, df in input_data.items():
        input_dfs.append(df)
    
    # get input dataframe
    input_df = union_all(input_dfs)

    # add extra column to input dataframe
    input_df = input_df.withColumn("current_ts", F.current_timestamp())

    input_df.printSchema()

    input_df.show()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("data-preparation").getOrCreate()
    sc = spark.sparkContext
    main(sys.argv[1:], spark)
    sc.stop()