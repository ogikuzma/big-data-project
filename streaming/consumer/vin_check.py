import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import *
from pyspark.sql.types import *

from preprocessing.preprocess_dataset import clean_dataframe

# run script
# docker exec -it spark-master bash
# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /home/streaming/vin_check.py

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def check_if_vin_valid(vin):
  if vin is None or len(vin) != 17:
    return False

  weights = [8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2]

  transliterations = {
    "a" : 1, "b" : 2, "c" : 3, "d" : 4,
    "e" : 5, "f" : 6, "g" : 7, "h" : 8,
    "j" : 1, "k" : 2, "l" : 3, "m" : 4,
    "n" : 5, "p" : 7, "r" : 9, "s" : 2,
    "t" : 3, "u" : 4, "v" : 5, "w" : 6,
    "x" : 7, "y" : 8, "z" : 9
  }

  try:
    sum = 0
    for i in range(len(vin)):
      if( vin[i].isnumeric() is False):
        sum += transliterations[vin[i].lower()] * weights[i]
      else:
        sum += int(vin[i]) * weights[i]

    check_digit = sum % 11
    if(check_digit == 10):
      check_digit = 'X'

    return str(check_digit) == vin[8]

  except:
    return False


# neophodno je kreirati user_defined_function
convertUDF = udf(lambda z: check_if_vin_valid(z), BooleanType())

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

print("--- 1. Check if VIN is valid ---")
df = df.select(col("VIN")).withColumn("is_vin_valid", convertUDF(col("VIN")))

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

query1 = df.select( to_json(struct("*")).alias("value")) \
  .writeStream \
  .outputMode("append") \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19092") \
  .option("checkpointLocation", f"{HDFS_NAMENODE}/user/root/data-lake/transformation/vin_check_result") \
  .option("topic", "vin_check") \
  .start()

query2 = df \
    .writeStream \
    .format("console") \
    .start(truncate=True)

query1.awaitTermination()
query2.awaitTermination()
