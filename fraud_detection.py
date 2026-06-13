from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, session_window, to_timestamp,
                                    regexp_replace, when, count, lit, sum as ssum)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = (SparkSession.builder
         .appName("ecommerce-fraud")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# the columns in the kaggle ecommerce file
schema = StructType([
    StructField("event_time",    StringType()),
    StructField("event_type",    StringType()),
    StructField("product_id",    StringType()),
    StructField("category_id",   StringType()),
    StructField("category_code", StringType()),
    StructField("brand",         StringType()),
    StructField("price",         DoubleType()),
    StructField("user_id",       StringType()),
    StructField("user_session",  StringType()),
])

# pick up csv files from the folder, one per micro-batch
stream = (spark.readStream
          .schema(schema)
          .option("header", True)
          .option("maxFilesPerTrigger", 1)
          .csv("stream_input"))

stream = stream.withColumn("event_time", to_timestamp(regexp_replace(col("event_time"), " UTC$", "")))

# bucket each user's activity into visits. a new visit starts after 30 min of no activity.
sessions = (stream
            .withWatermark("event_time", "5 minutes")
            .groupBy(session_window(col("event_time"), "30 minutes"), col("user_id"))
            .agg(
                ssum(when(col("event_type") == "cart", 1).otherwise(0)).alias("cart_adds"),
                ssum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
                count("*").alias("events_in_session"),
            ))

# rule explaination: > 5 things added to the cart and nothing bought
alerts = (sessions
          .filter((col("cart_adds") > 5) & (col("purchases") == 0))
          .select(
              col("user_id"),
              col("session_window.start").alias("session_start"),
              col("cart_adds"),
              col("events_in_session"),
              lit("FRAUD_FLAG").alias("status"),
          ))

query = (alerts.writeStream
         .format("console")
         .outputMode("append")
         .option("truncate", False)
         .start())

query.awaitTermination()
