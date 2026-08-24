import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit


def analyze_average_video_length(
    youtube_path, tiktok_path, mongo_uri, db_name, coll_name
):
    spark = SparkSession.builder.appName("AverageVideoLengthComparison").getOrCreate()

    yt_df = spark.read.parquet(youtube_path).select(
        col("video_id"),
        col("duration_seconds").cast("double").alias("duration"),
        lit("YouTube").alias("platform"),
    )

    tt_df = spark.read.parquet(tiktok_path).select(
        col("id").alias("video_id"),  # TikTok koristi 'id' umesto 'video_id'
        col("duration").cast("double").alias("duration"),
        lit("TikTok").alias("platform"),
    )

    combined_df = yt_df.union(tt_df)

    avg_result = combined_df.groupBy("platform").agg(
        F.avg("duration").alias("average_duration_seconds"),
        F.min("duration").alias("min_duration_seconds"),
        F.max("duration").alias("max_duration_seconds"),
        F.count("video_id").alias("total_videos"),
    )

    # Optional write to console
    # avg_result.show(20, truncate=False)

    (
        avg_result.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", db_name)
        .option("collection", coll_name)
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    analyze_average_video_length(
        sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    )
