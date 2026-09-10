import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    youtube_path = sys.argv[1]
    tiktok_path = sys.argv[2]
    mongo_uri = sys.argv[3]
    mongo_db = sys.argv[4]
    mongo_collection = sys.argv[5]

    spark = SparkSession.builder.appName("daily_trend_views_engagement").getOrCreate()

    yt_df = spark.read.parquet(youtube_path).select(
        F.col("video_id"),
        F.col("view_count"),
        F.col("likes"),
        F.col("comment_count"),
        F.col("published_at").alias("content_date"),
        F.lit("YouTube").alias("platform"),
    )

    tt_df = spark.read.parquet(tiktok_path).select(
        F.col("id").alias("video_id"),
        F.col("play_count").alias("view_count"),
        F.col("digg_count").alias("likes"),
        F.col("comment_count"),
        F.col("create_time").alias("content_date"),
        F.lit("TikTok").alias("platform"),
    )

    df = yt_df.union(tt_df)

    # Pretvaranje u pravi datum (format YYYY-MM-DD) koji će ići na X-osu
    df = df.withColumn("date", F.to_date("content_date"))
    # Ekstrakcija godine isključivo da bi se lako filtriralo na dashboard-u po željenoj godini
    df = df.withColumn("year", F.year("date"))

    # Filtriranje validnih datuma i opsega godina (2020-2025)
    df_filtered = df.filter(
        F.col("date").isNotNull() & F.col("year").between(2020, 2025)
    )

    # Grupisanje po tačnom datumu, godini i platformi
    trend_df = df_filtered.groupBy("date", "year", "platform").agg(
        F.count("video_id").alias("total_videos"),
        F.avg("view_count").alias("avg_views"),
        F.avg("likes").alias("avg_likes"),
        F.avg("comment_count").alias("avg_comments"),
        F.avg(
            (F.col("likes") + F.col("comment_count"))
            / F.when(F.col("view_count") == 0, 1).otherwise(F.col("view_count"))
        ).alias("avg_engagement_rate"),
    )

    # Sortiranje hronološki po datumu
    final_result = trend_df.orderBy("date", "platform")

    final_result.show(50, truncate=False)

    # Upis u MongoDB kolekciju
    (
        final_result.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()
