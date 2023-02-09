from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf
from pyspark.sql.functions import col
from pyspark.sql.types import *
import boto3 
import json

# Task 1: Extract, transform and load weather data from S3 to Snowflake
# 1. Set up spark and access the S3
packages = [
    "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1",
    "net.snowflake:snowflake-jdbc:3.13.3", 
    "org.apache.hadoop:hadoop-aws:3.1.2"
]

config = {
    "spark.jars.packages": "," .join(packages),
    "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
}



conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()
df = spark.read.json(
    's3a://dataminded-academy-capstone-resources/raw/open_aq/')

# 2. Extract and transform
#   Flatten all nested columns
#   Some timestamp columns are stored as string, cast them to timestamp

unnest = df.select(
    "*",
    "coordinates.latitude",
    "coordinates.longitude",
    col("date.local").cast(DateType()),
    col("date.utc").cast(DateType()))

df_tosf = unnest.drop("coordinates", "date")

# 3. Retrieve Snowflake credentials 

client = boto3.client('secretsmanager')
response = client.get_secret_value(
    SecretId = 'snowflake/capstone/login'
)
sfs = json.loads(response['SecretString'])

# 4. Load to Snowflake
# Set options below
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sfOptions = {
  "sfURL" : sfs['URL'],
  "sfUser" : sfs['USER_NAME'],
  "sfPassword" : sfs['PASSWORD'],
  "sfDatabase" : sfs['DATABASE'],
  "sfSchema" : 'TARIK',
  "sfWarehouse" : sfs['WAREHOUSE'],
  "sfRole": sfs["ROLE"]
}

df_tosf.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable","capstone_tarik").mode("overwrite").save()
