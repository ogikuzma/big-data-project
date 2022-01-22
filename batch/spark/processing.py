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


# print("--- 1. Average time to sell a car each year---")
# df.dropDuplicates(["vin", "firstSeen"]) \
#     .withColumn("PostingYear", year(col("firstSeen"))) \
#     .withColumn("DateDiff", datediff(col("lastSeen"), col("firstSeen"))) \
#     .groupBy("PostingYear") \
#     .agg(
#         round(avg("DateDiff")).alias("AvgSellTime(days)")
#     ) \
#     .filter(( col("PostingYear") >= 2018 )  & ( col("PostingYear") <= 2020 )) \
#     .show()


# print("--- 2. Average price difference between asking price and msrp ---")
# df.filter("msrp > 0 and askPrice > 0") \
#               .selectExpr("(1 - (askPrice/msrp)) * 100 as price_difference") \
#               .agg(
#                   round(avg("price_difference")).alias("AvgPriceDiff(%)")
#               ) \
#               .show()


# print("--- 3. Mileage validity ---")
# mileage_df = df.dropDuplicates(["vin", "firstSeen"]) \
#        .filter("msrp > 0") \
#        .filter("not (mileage == 0 and isNew == false)")

# mileage_df.select("*") \
#   .groupBy("vin") \
#   .agg(count("*").alias("count")) \
#   .orderBy(desc("count")) \
#   .show(truncate=False)

# mileage_df.filter("vin == '06d4b59fdef1d3e3ac3c8b10596f73ea4720355857fbd000f9f6921bc61e8e87'").show()


# docker cp postgresql-42.3.0.jar spark-master:./postgresql-42.3.0.jar
# /spark/bin/spark-submit --driver-class-path postgresql-42.3.0.jar processing.py

print("--- 4. Price decline per year for specific model ---")
window = Window.partitionBy().orderBy("modelName", "vf_ModelYear")

price_decline_df = df.filter((col("askPrice") > 0) & (col("askPrice") < 500000)) \
    .groupBy("modelName", "brandName", "vf_ModelYear")\
    .agg(
        min("askPrice").alias("MinPrice"),
        max("askPrice").alias("MaxPrice"),
        round(mean("askPrice")).alias("AvgPrice"),
        count("*").alias("TotalCars")
    ) \
    .withColumn("PriceDiff", (col("AvgPrice") - lag("AvgPrice", -1).over(window))) \
    .orderBy(desc("vf_ModelYear"))

df_index = price_decline_df.select("*").withColumn("id", monotonically_increasing_id())

df_index.drop("id").show()

df_index.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://postgresql:5432/big_data") \
  .option("driver", "org.postgresql.Driver") \
  .option("dbtable", "public.price_decline") \
  .option("user", "postgres") \
  .option("password", "1111") \
  .mode("overwrite") \
  .save() 

# df_index.filter(col("id") < 5) \
#   .filter("vf_ModelName == 'Corolla'") \
#   .agg(round(mean("PriceDiff")).alias("AvgPriceDeclinePerYearInFirstFiveYears($)")) \
#   .show()

# df_index.filter(col("id") >= 5) \
#   .filter("vf_ModelName == 'Corolla'") \
#   .agg(round(mean("PriceDiff")).alias("AvgPriceDeclinePerYearAfterFirstFiveYears($)")) \
#   .show()


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
# df_numOfAds = df.withColumn("PostingYear", year(col("firstSeen"))) \
#   .withColumn("PostingMonth", month(col("firstSeen"))) \
#   .groupBy("PostingYear", "PostingMonth") \
#   .agg(
#     count("*").alias("NumberOfAds"),
#   ) \
#   .withColumn("Month", concat(col("PostingYear"), lit("-"), col("PostingMonth"))) \
#   .filter(
#     ( concat(col("Month"), lit("-01")).cast(DateType()) >= lit('2018-06-01').cast(DateType()) )  
#     & ( concat(col("Month"), lit("-01")).cast(DateType()) <= lit('2020-06-30').cast(DateType()) )
#     ) \
#   .orderBy(desc("NumberOfAds"))

# df_numOfAds.show()

# df_numOfAds.write \
#   .format("jdbc") \
#   .option("url", "jdbc:postgresql://postgresql:5432/big_data") \
#   .option("driver", "org.postgresql.Driver") \
#   .option("dbtable", "public.num_of_ads") \
#   .option("user", "postgres") \
#   .option("password", "1111") \
#   .mode("overwrite") \
#   .save() 

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
#   .show()

  
# print("--- 9. Average number of seats joined with number of children born ---")
# df_children = spark.read \
#   .option("delimiter", ",") \
#   .option("header", "true") \
#   .csv(HDFS_NAMENODE + "/user/root/data-lake/raw/illinois-children.csv")

# df_children = df_children.withColumn("NumOfChildren", col("NumOfChildren").cast(FloatType()))

# df.dropDuplicates(["vin"]) \
#   .select("vin", "vf_Seats", "firstSeen") \
#   .where("vf_Seats is not null") \
#   .withColumn("PostingYear", year(df.firstSeen)) \
#   .groupBy("PostingYear") \
#   .agg(
#     round(avg("vf_Seats"), 2).alias("AvgNumOfSeats")
#   ) \
#   .join(df_children, col("PostingYear") == df_children["Year"], "leftouter") \
#   .filter("NumOfChildren is not null") \
#   .drop("Year") \
#   .show()