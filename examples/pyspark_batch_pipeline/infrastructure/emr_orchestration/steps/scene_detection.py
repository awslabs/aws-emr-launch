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
    parser.add_argument("--synchronized-table-name", required=True)
    return parser.parse_args(args=args)


def get_batch_file_metadata(table_name, batch_id):
    dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
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


def load_file_path(spark, bucket, prefix, topic, bag_file):
    s3path = "s3://" + bucket + "/" + prefix
    df = spark.read.load(s3path).withColumn("topic", func.lit(topic)).withColumn("bag_file", func.lit(bag_file))
    return df


def load_and_union_data(spark, batch_metadata):
    distinct_topics = list(set([x["Topic"] for x in batch_metadata]))
    topic_dfs = {}
    for topic in distinct_topics:
        dfs = [
            load_file_path(
                spark,
                bucket=x["FileBucket"],
                prefix=x["Name"],
                topic=x["Topic"],
                bag_file=x["BagFile"]
            )
            for x in batch_metadata
            if x["Topic"] == topic
        ]
        topic_dfs[topic] = union_all(dfs)

    return topic_dfs


#def sample_timestamps_per_topic(df, time_col):


def join_topics(dfs, col_selection_dict):
    filtered_dfs = []
    # Take first row per topic per bag_file per second rounded
    for topic_name, topic_df in dfs.items():
        topic_col_subet = col_selection_dict[topic_name]
        if "bag_file" not in topic_col_subet:
            topic_col_subet.append("bag_file")
        filtered_dfs.append(topic_df.select(*topic_col_subet))


def write_results(df, table_name, output_bucket, partition_cols=[]):
    df.write.mode('append').partitionBy(*partition_cols).parquet(f"s3://{output_bucket}/{table_name}")


def synchronize_timestamps(df):
    return df


def main(args, spark):
    arguments = parse_arguments(args)
    # Load files to process
    batch_metadata = get_batch_file_metadata(
        table_name=arguments.batch_metadata_table_name,
        batch_id=arguments.batch_id
    )

    # Load topic data from s3 and union
    topic_data = load_and_union_data(spark, batch_metadata)

    # TODO Filter cols and join on FileName

    # TODO Synchronize signals
    # synchronized_gps = synchronize_timestamps(topic_data['gps'])

    # TODO Detect example scene

    # Save Synchronized Signals to S3
    for topic, df in topic_data.items():
        write_results(
            df,
            table_name=topic,
            output_bucket=arguments.output_bucket,
            partition_cols=['bag_file']
        )
        break #TODO REMOVE

    # Save Drive Labels to S3

    # Save Drive Labels to ElasticSearch


if __name__ == "__main__":
    spark = SparkSession.builder.appName("scene-detection").getOrCreate()
    sc = spark.sparkContext
    main(sys.argv[1:], spark)
    sc.stop()
