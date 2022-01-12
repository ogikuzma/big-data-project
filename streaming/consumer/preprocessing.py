from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType() \
  .add("id", StringType()) \
  .add("url", StringType()) \
  .add("region", StringType()) \
  .add("region_url", StringType()) \
  .add("price", StringType()) \
  .add("year", StringType()) \
  .add("manufacturer", StringType()) \
  .add("model", StringType()) \
  .add("condition", StringType()) \
  .add("cylinders", StringType()) \
  .add("fuel", StringType()) \
  .add("odometer", StringType()) \
  .add("title_status", StringType()) \
  .add("transmission", StringType()) \
  .add("VIN", StringType()) \
  .add("drive", StringType()) \
  .add("size", StringType()) \
  .add("type", StringType()) \
  .add("paint_color", StringType()) \
  .add("image_url", StringType()) \
  .add("description", StringType()) \
  .add("county", StringType()) \
  .add("state", StringType()) \
  .add("lat", StringType()) \
  .add("long", StringType()) \
  .add("posting_date", StringType()) \

def clean_dataframe(df):
    df = df.select( \
    from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    df = df.withColumn("id", col("id").cast(LongType())) \
    .withColumn("price", col("price").cast(FloatType())) \
    .withColumn("cylinders", col("cylinders").cast(IntegerType())) \
    .withColumn("odometer", col("odometer").cast(FloatType())) \
    .withColumn("lat", col("lat").cast(FloatType())) \
    .withColumn("long", col("long").cast(FloatType())) \
    .withColumn("posting_date", col("posting_date").cast(DateType())) \
    .drop(col("region")) \
    .drop(col("region_url")) \
    .drop(col("cylinders")) \
    .drop(col("paint_color")) \
    .drop(col("image_url")) \
    .drop(col("description")) \
    .drop(col("county")) \
    .drop(col("state")) \
    .drop(col("lat")) \
    .drop(col("long"))

    return df