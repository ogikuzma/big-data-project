import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Processing batch data") \
    .getOrCreate()
    
quiet_logs(spark)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

df = spark.read \
  .option("delimiter", ",") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .csv(HDFS_NAMENODE + "/user/root/data-lake/transformation/batch-preprocessed.csv")


# print("--- 1. Average time to sell a car ---")
# priceDiff = df.dropDuplicates(["vin", "firstSeen"]) \
#     .select(
#         col("firstSeen"), 
#         col("lastSeen"), 
#         datediff(col("lastSeen"), col("firstSeen")).alias("date_difference")
#     ) \
#     .agg(
#         round(avg("date_difference")).alias("AvgSellTime(days)"))

# priceDiff.show()


# print("--- 2. Average price difference between asking price and msrp ---")
# priceDiff = df.filter("msrp > 0 and askPrice > 0") \
#               .selectExpr("(1 - (askPrice/msrp)) * 100 as price_difference") \
#               .agg(
#                   round(avg("price_difference")).alias("AvgPriceDiff(%)"))

# priceDiff.show()

# print("--- 3. Mileage validity ---")
# df = df.dropDuplicates(["vin", "firstSeen"]) \
#        .filter("msrp > 0") \
#        .filter("not (mileage == 0 and isNew == false)")

# df.select("*") \
#   .groupBy("vin") \
#   .agg(count("*").alias("count")) \
#   .orderBy(desc("count")) \
#   .show(truncate=False)

# df.filter("vin == '06d4b59fdef1d3e3ac3c8b10596f73ea4720355857fbd000f9f6921bc61e8e87'") \
#   .orderBy(desc("firstSeen")).show()

# mileageCheck = df.groupBy("vin").count().show(100)

# df.where("vin == '01843d4a7c85f8cb473cef6b5fd6502a7f78b527cc8e119ad8c47cbf2f40adba'").show()

# df.filter("vin == '01843d4a7c85f8cb473cef6b5fd6502a7f78b527cc8e119ad8c47cbf2f40adba'").show()

# df.groupBy("vin").count().show(300, truncate=False)

# df.select(countDistinct("vin")).show()
# print(df.count())

# print("--- 4. Price decline per year for specific model ---")
# window = Window.partitionBy().orderBy("modelName", "vf_ModelYear")

# df = df.filter((col("askPrice") > 0) & (col("askPrice") < 500000)) \
#     .filter("modelName == 'Corolla'") \
#     .groupBy("modelName", "vf_ModelYear")\
#     .agg(
#         min("askPrice").alias("MinPrice"),
#         max("askPrice").alias("MaxPrice"),
#         round(mean("askPrice")).alias("AvgPrice"),
#         count("*").alias("TotalCars")
#     ) \
#     .withColumn("PriceDiff", (col("AvgPrice") - lag("AvgPrice", -1).over(window))) \
#     .orderBy(desc("vf_ModelYear"))

# df_index = df.select("*").withColumn("id", monotonically_increasing_id())
# df_index.show()

# df_index.filter(col("id") < 5) \
#   .agg(round(mean("PriceDiff")).alias("AvgPriceDeclinePerYearInFirstFiveYears")) \
#   .show()

# df_index.filter(col("id") >= 5) \
#   .agg(round(mean("PriceDiff")).alias("AvgPriceDeclinePerYearAfterFirstFiveYears")) \
#   .show()

# df.filter((col("modelName") == 'Q7') & (col("vf_ModelYear") == 2007)).orderBy(desc("askPrice")).show(85, truncate=True)

# print("--- 6. Average mileage per year ---")
# df.dropDuplicates(["vin"]) \
#        .filter("not (mileage == 0 and isNew == false)") \
#        .filter("vf_ModelYear < 2021") \
#        .groupBy("vf_ModelYear") \
#        .agg(
#          round(mean("mileage")).alias("AvgMileage")
#        ) \
#        .orderBy(desc("vf_ModelYear")) \
#        .show()

# print("--- 7. Months sorted by number of ads ---")
# df.withColumn("PostingYear", year(col("firstSeen"))) \
#   .withColumn("PostingMonth", month(col("firstSeen"))) \
#   .groupBy("PostingYear", "PostingMonth") \
#   .agg(
#     count("*").alias("NumberOfAds"),
#     first("firstSeen").alias("date")
#   ) \
#   .filter(
#     ( col("date") >= lit('2018-06-01').cast(DateType()) )  
#     & ( col("date") <= lit('2020-06-30').cast(DateType()) )
#     ) \
#   .orderBy(desc("NumberOfAds")) \
#   .drop("date") \
#   .show(30)

# print("--- 8. Most advertised car brands per year ---")
# df_brands = df.dropDuplicates(["vin"]) \
#   .withColumn("PostingYear", year(col("firstSeen"))) \
#   .groupBy("PostingYear", "brandName") \
#   .agg(
#     count("*").alias("Total"), 
#   )

# df_brands_sorted = df_brands.groupBy("PostingYear") \
#   .agg(
#     max("Total").alias("NumberOfAds")
#   )  

# df_brands \
#   .join(df_brands_sorted) \
#   .where(col("NumberOfAds") == col("Total")) \
#   .drop(df_brands.PostingYear) \
#   .drop(col("Total")) \
#   .orderBy(desc("PostingYear")) \
#   .show(100)
  
# JUNK CODE
# window = Window.partitionBy("PostingYear", "brandName")

# df.dropDuplicates(["vin"]) \
#   .filter("not (mileage == 0 and isNew == false)") \
#   .withColumn("PostingYear", year(col("firstSeen"))) \
#   .withColumn("PostingMonth", month(col("firstSeen"))) \
#   .withColumn("Total", count("*").over(window)) \
#   .withColumn("Max", max("Total").over(window)) \
#   .where(col("Total") == col("Max")) \
#   .show()

df_children = spark.read \
  .option("delimiter", ",") \
  .option("header", "true") \
  .csv(HDFS_NAMENODE + "/user/root/data-lake/raw/illinois-children.csv")

df_children = df_children.withColumn("NumOfChildren", col("NumOfChildren").cast(FloatType()))

df.dropDuplicates(["vin"]) \
  .select("vin", "vf_Seats", "firstSeen") \
  .where("vf_Seats is not null") \
  .withColumn("PostingYear", year(df.firstSeen)) \
  .groupBy("PostingYear") \
  .agg(
    round(avg("vf_Seats"), 2).alias("AvgNumOfSeats")
  ) \
  .join(df_children, col("PostingYear") == df_children["Year"], "leftouter") \
  .filter("NumOfChildren is not null") \
  .drop("Year") \
  .show()





