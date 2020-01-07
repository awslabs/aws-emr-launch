from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('TestStepApp').getOrCreate()
spark.sql('SHOW databases')
