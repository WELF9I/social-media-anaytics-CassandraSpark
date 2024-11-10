from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count, sum, collect_set, explode, col, to_timestamp
from pyspark.sql.types import *
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SocialMediaSparkProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("SocialMediaAnalytics") \
            .config("spark.cassandra.connection.host", "localhost") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
            .getOrCreate()

        # Define the schema to match Cassandra table structure
        self.schema = StructType([
            StructField("post_id", StringType(), False),  # Changed to non-nullable
            StructField("timestamp", TimestampType(), False),  # Changed to non-nullable
            StructField("likes", IntegerType(), True),
            StructField("shares", IntegerType(), True),
            StructField("comments", IntegerType(), True),
            StructField("hashtags", ArrayType(StringType()), True)
        ])

    def process_data(self):
        # Read data from Cassandra
        df = self.spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="post_stats", keyspace="social_media_analytics") \
            .load()

        # Post aggregates - ensure window_start is the partition key
        post_aggregates = df.withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window("timestamp", "10 minutes"),
                "post_id"
            ) \
            .agg(
                sum("likes").cast("bigint").alias("total_likes"),  # Cast to bigint
                sum("shares").cast("bigint").alias("total_shares"),  # Cast to bigint
                sum("comments").cast("bigint").alias("total_comments")  # Cast to bigint
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("post_id"),
                "total_likes",
                "total_shares",
                "total_comments"
            )

        # Hashtag stats - ensure day_bucket is the partition key
        hashtag_stats = df.withWatermark("timestamp", "10 minutes") \
            .select(
                col("timestamp").cast("date").alias("day_bucket"),  # Cast to date for daily bucket
                explode("hashtags").alias("hashtag")
            ) \
            .groupBy("day_bucket", "hashtag") \
            .agg(count("*").alias("usage_count"))

        # Save results to Cassandra
        post_aggregates.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="post_aggregates", keyspace="social_media_analytics") \
            .save()

        hashtag_stats.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="hashtag_stats", keyspace="social_media_analytics") \
            .save()

    def run(self):
        try:
            logger.info("Starting Spark processing")
            self.process_data()
        except Exception as e:
            logger.error(f"Error during processing: {e}")
        finally:
            self.spark.stop()

if __name__ == "__main__":
    processor = SocialMediaSparkProcessor()
    processor.run()