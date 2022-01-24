import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import *
from pyspark.sql.types import *

from preprocessing.preprocess_dataset import clean_dataframe

# run script
# docker exec -it spark-master bash
# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /home/streaming/consumer.py

TOPIC = "cars"

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Streaming processing") \
    .getOrCreate()

quiet_logs(spark)


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19092") \
  .option("subscribe", "usa_cars") \
  .load()

df = clean_dataframe(df)

print("--- 5. Number of car ads per US state in 1 minute every 30 seconds ---")
num_of_ads = df.groupBy(window(df.timestamp, "1 minute", "30 seconds"), "state") \
                .agg(
                  count("*").alias("NumOfAds")
                ) \
                .orderBy(desc("NumOfAds")) \
                .limit(10) \
                .orderBy("window")


HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# num_of_ads \
#     .writeStream \
#     .format("csv") \
#     .option("header", "false") \
#     .option("path", f"{HDFS_NAMENODE}/user/root/data-lake/transformation/ads_per_state_result.csv") \
#     .option("checkpointLocation", f"{HDFS_NAMENODE}/user/root/data-lake/transformation/ads_per_state_result.csv") \
#     .start()

query1 = num_of_ads.select( to_json(struct("*")).alias("value")) \
  .writeStream \
  .outputMode("complete") \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19092") \
  .option("checkpointLocation", f"{HDFS_NAMENODE}/user/root/data-lake/transformation/ads_per_state_result") \
  .option("topic", "ads_per_state") \
  .start()

query2 = num_of_ads \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start(truncate=False)

query1.awaitTermination()
query2.awaitTermination()
