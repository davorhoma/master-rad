import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    youtube_path = sys.argv[1]
    tiktok_path = sys.argv[2]
    mongo_uri = sys.argv[3]
    mongo_db = sys.argv[4]
    mongo_collection = sys.argv[5]

    spark = SparkSession.builder.appName(
        "viral_score_distribution_by_popularity"
    ).getOrCreate()

    yt_df = spark.read.parquet(youtube_path).select(
        F.col("video_id"),
        F.lit("YouTube").alias("platform"),
        F.col("view_count"),
        F.col("viral_score"),
        F.col("published_at").alias("content_date"),
    )

    tt_df = spark.read.parquet(tiktok_path).select(
        F.col("id").alias("video_id"),
        F.lit("TikTok").alias("platform"),
        F.col("play_count").alias("view_count"),  # Rename play_count to view_count
        F.col("viral_score"),
        F.col("create_time").alias("content_date"),
    )

    df = yt_df.union(tt_df)

    df = df.withColumn("year", F.year(F.to_date("content_date")))
    df_filtered = df.filter(F.col("year").between(2020, 2025))

    df_popularity = df_filtered.withColumn(
        "popularity_group",
        F.when(F.col("view_count") >= 1000000, F.lit("Popular (>= 1M)")).otherwise(
            F.lit("Less popular (< 1M)")
        ),
    )

    final_result = df_popularity.select(
        "year", "platform", "popularity_group", "video_id", "view_count", "viral_score"
    ).filter(F.col("viral_score").isNotNull())

    final_result.orderBy(
        "year", "platform", "popularity_group", F.col("viral_score").desc()
    ).show(50, truncate=False)

    (
        final_result.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()
