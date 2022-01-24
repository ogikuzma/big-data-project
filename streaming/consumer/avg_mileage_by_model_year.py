import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import *
from pyspark.sql.types import *

from preprocessing.preprocess_dataset import clean_dataframe

# run script
# docker exec -it spark-master bash
# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /home/streaming/consumer.py

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Streaming processing") \
    .getOrCreate()

quiet_logs(spark)


usa_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19092") \
  .option("subscribe", "usa_cars") \
  .load()

# eu_df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19092") \
#   .option("subscribe", "eu_cars") \
#   .load()
# eu_df = clean_join_dataframe(eu_df)
# joined_streams = usa_df.join(eu_df, lit(false), "full")

usa_df = clean_dataframe(usa_df)

print("--- 2. Average mileage for model year ---")
avg_mileage = usa_df.filter("year is not null") \
                .groupBy("year") \
                .agg(
                    round(avg("odometer")).alias("AvgMileage")
                ) \
                .orderBy(desc("year"))

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

query1 = avg_mileage.select( to_json(struct("*")).alias("value")) \
  .writeStream \
  .outputMode("complete") \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19092") \
  .option("checkpointLocation", f"{HDFS_NAMENODE}/user/root/data-lake/transformation/avg_mileage_result") \
  .option("topic", "avg_mileage") \
  .start() 

query2 = avg_mileage \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start(truncate=False)

query1.awaitTermination()
query2.awaitTermination()
