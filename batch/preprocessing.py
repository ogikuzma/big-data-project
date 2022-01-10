import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Deleting unneccessary columns") \
    .getOrCreate()

quiet_logs(spark)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

df = spark.read \
  .option("delimiter", ",") \
  .option("header", "true") \
  .csv(HDFS_NAMENODE + "/user/root/data-lake/raw/batch-dataset.csv")


df = df.select(
  "vin", 
  "firstSeen", 
  "lastSeen", 
  "msrp", 
  "askPrice", 
  "mileage", 
  "isNew", 
  "brandName", 
  "modelName", 
  "vf_ModelYear", 
  "vf_Seats",
  "dealerId"
)

df = df.withColumn("firstSeen", col("firstSeen").cast(DateType())) \
    .withColumn("lastSeen", col("lastSeen").cast(DateType())) \
    .withColumn("msrp", col("msrp").cast(FloatType())) \
    .withColumn("askPrice", col("askPrice").cast(FloatType())) \
    .withColumn("mileage", col("mileage").cast(FloatType())) \
    .withColumn("isNew", col("isNew").cast(BooleanType())) \
    .withColumn("vf_ModelYear", col("vf_ModelYear").cast(IntegerType())) \
    .withColumn("vf_Seats", col("vf_Seats").cast(IntegerType()))

df.write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv(HDFS_NAMENODE + "/user/root/data-lake/transformation/batch-preprocessed.csv")



