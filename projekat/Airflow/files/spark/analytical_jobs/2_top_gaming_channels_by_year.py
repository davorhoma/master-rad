import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, year, to_date, lit, avg
from pyspark.sql.window import Window


def calculate_top_channels(youtube_path, tiktok_path, mongo_uri, db_name, coll_name):
    spark = SparkSession.builder.appName("TopGamingChannelsAnalysis").getOrCreate()

    yt_df = (
        spark.read.parquet(youtube_path)
        .select(
            col("channel_title").alias("channel_name"),
            year(to_date(col("published_at"))).alias("year"),
            col("viral_score"),
            lit("YouTube").alias("platform"),
        )
        .filter((col("year") >= 2020) & (col("year") <= 2024))
    )

    tt_df = (
        spark.read.parquet(tiktok_path)
        .select(
            col("author_name").alias("channel_name"),
            year(to_date(col("create_time"))).alias("year"),
            col("viral_score"),
            lit("TikTok").alias("platform"),
        )
        .filter(col("year") == 2025)
    )

    combined_df = yt_df.union(tt_df)

    channel_yearly_agg = combined_df.groupBy("year", "platform", "channel_name").agg(
        avg("viral_score").alias("avg_viral_score"),
        F.max("viral_score").alias("max_viral_score"),
    )

    window_spec = Window.partitionBy("year").orderBy(F.col("max_viral_score").desc())

    ranked_channels = channel_yearly_agg.withColumn(
        "rank", F.row_number().over(window_spec)
    ).filter(F.col("rank") <= 10)

    final_result = ranked_channels.orderBy("year", F.col("rank").asc())

    # Optional write to console
    final_result.show(50, truncate=False)

    (
        final_result.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", db_name)
        .option("collection", coll_name)
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    calculate_top_channels(
        sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    )
