import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

if __name__ == "__main__":
    youtube_path = sys.argv[1]
    tiktok_path = sys.argv[2]
    mongo_uri = sys.argv[3]
    mongo_db = sys.argv[4]
    mongo_collection = sys.argv[5]

    spark = SparkSession.builder.appName("top_gaming_videos_by_year").getOrCreate()

    yt_df = spark.read.parquet(youtube_path).select(
        F.col("video_id"),
        F.col("title"),
        F.col("channel_title"),
        F.col("view_count"),
        F.col("likes"),
        F.col("comment_count"),
        F.col("viral_score"),
        F.col("published_at").alias("content_date"),
        F.lit("YouTube").alias("platform"),
    )

    tt_df = spark.read.parquet(tiktok_path).select(
        F.col("id").alias("video_id"),
        F.substring(F.col("desc"), 1, 45).alias("title"),
        F.col("user_id").alias("channel_title"),
        F.col("play_count").alias("view_count"),
        F.col("digg_count").alias("likes"),
        F.col("comment_count"),
        F.col("viral_score"),
        F.col("create_time").alias("content_date"),
        F.lit("TikTok").alias("platform"),
    )

    df = yt_df.union(tt_df)

    df = df.withColumn("year", F.year(F.to_date("content_date")))

    df_filtered = df.filter(F.col("year").between(2020, 2025))

    video_yearly_max = df_filtered.groupBy(
        "year", "platform", "video_id", "title", "channel_title"
    ).agg(
        F.max("view_count").alias("max_views"),
        F.max("likes").alias("max_likes"),
        F.max("comment_count").alias("max_comments"),
        F.max("viral_score").alias("max_viral_score"),
    )

    window_spec = Window.partitionBy("year").orderBy(F.col("max_views").desc())

    ranked_videos = video_yearly_max.withColumn(
        "rank", F.row_number().over(window_spec)
    ).filter(F.col("rank") <= 10)

    final_result = ranked_videos.orderBy("year", F.col("rank").asc())

    final_result.show(50, truncate=False)

    (
        final_result.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()
