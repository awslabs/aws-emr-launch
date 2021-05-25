from pyspark.sql import SparkSession
import boto3
import argparse
import sys
import functools
import pyspark.sql.functions as func


def union_all(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)


def parse_arguments(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-metadata-table-name", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--output-bucket", required=True)
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


def load_file_path(spark, bucket, prefix, file_partition, file_slot):
    s3path = "s3://" + bucket + "/" + prefix
    df = spark.read.load(s3path).withColumn("file_partition", func.lit(file_partition)).withColumn("file_slot", func.lit(file_slot))
    return df


def load_and_union_data(spark, batch_metadata):
    distinct_partitions = list(set([x["FilePartition"] for x in batch_metadata]))
    partition_dfs = {}
    for partition in distinct_partitions:
        dfs = [
            load_file_path(
                spark,
                bucket=x["FileBucket"],
                prefix=x["Name"],
                file_partition=x["FilePartition"],
                file_slot=x["FileSlot"]
            )
            for x in batch_metadata
            if x["FilePartition"] == partition
        ]
        partition_dfs[partition] = union_all(dfs)

    return partition_dfs

def write_results(df, table_name, output_bucket, partition_cols=[]):
    df.write.mode('append').partitionBy(*partition_cols).parquet(f"s3://{output_bucket}/{table_name}")


def main(args, spark):
    arguments = parse_arguments(args)
    # Load files to process
    batch_metadata = get_batch_file_metadata(
        table_name=arguments.batch_metadata_table_name,
        batch_id=arguments.batch_id,
        region=arguments.region
    )

    # Load data from s3 and union
    input_data = load_and_union_data(spark, batch_metadata)

    # Save Output to S3
    for dataset, df in input_data.items():
        write_results(
            df,
            table_name=dataset,
            output_bucket=arguments.output_bucket,
            partition_cols=['file_slot']
        )
        break


if __name__ == "__main__":
    spark = SparkSession.builder.appName("data-ingestion").getOrCreate()
    sc = spark.sparkContext
    main(sys.argv[1:], spark)
    sc.stop()
