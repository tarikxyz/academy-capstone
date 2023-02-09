from pyspark.sql import SparkSession
from pyspark import SparkConf


# Task 1: Extract, transform and load weather data from S3 to Snowflake

# 1. Set up spark and access the S3
config = {
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.1.2",
    "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    }
conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf = conf).getOrCreate()
df = spark.read.json('s3a://dataminded-academy-capstone-resources/raw/open_aq/')

# 2. Extract and transform
#   Flatten all nested columns
#   Some timestamp columns are stored as string, cast them to timestamp



