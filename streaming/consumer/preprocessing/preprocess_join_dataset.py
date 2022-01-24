from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType() \
  .add("maker", StringType()) \
  .add("model", StringType()) \
  .add("mileage", StringType()) \
  .add("manufacture_year", StringType()) \
  .add("engine_displacement", StringType()) \
  .add("engine_power", StringType()) \
  .add("body_type", StringType()) \
  .add("color_slug", StringType()) \
  .add("stk_year", StringType()) \
  .add("transmission", StringType()) \
  .add("door_count", StringType()) \
  .add("seat_count", StringType()) \
  .add("fuel_type", StringType()) \
  .add("date_created", StringType()) \
  .add("date_last_seen", StringType()) \
  .add("price_eur", StringType()) \

def clean_join_dataframe(df):
  df = df.select(
      col("timestamp"),
      from_json(col("value").cast("string"), schema).alias("data")).select("data.*", "timestamp")

  df = df.withColumn("odometer", col("mileage").cast(IntegerType())) \
  .withColumn("year", col("manufacture_year").cast(IntegerType())) \
  .drop(col("maker")) \
  .drop(col("model")) \
  .drop(col("engine_displacement")) \
  .drop(col("engine_power")) \
  .drop(col("body_type")) \
  .drop(col("color_slug")) \
  .drop(col("stk_year")) \
  .drop(col("transmission")) \
  .drop(col("door_count")) \
  .drop(col("seat_count")) \
  .drop(col("fuel_type")) \
  .drop(col("date_created")) \
  .drop(col("date_last_seen")) \
  .drop(col("price_eur"))

  return df