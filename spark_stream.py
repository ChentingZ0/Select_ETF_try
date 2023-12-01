import logging

# from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

# create a SparkSession
spark = SparkSession.builder.appName("Kafka Pyspark Streaming Learning")\
                            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
                            .getOrCreate()

# rdd = spark.sparkContext.parallelize(range(1, 100))
# print("THE SUM IS HERE: ", rdd.sum())
# # Stop the SparkSession
# spark.stop()

KAFKA_TOPIC_NAME = "assets"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
    .option("subscribe", KAFKA_TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()
print("kafka dataframe created successfully")

print(df)
