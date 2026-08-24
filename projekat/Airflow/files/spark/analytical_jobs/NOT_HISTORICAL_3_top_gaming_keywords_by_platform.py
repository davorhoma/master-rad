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
        "top_gaming_keywords_by_platform"
    ).getOrCreate()

    yt_df = spark.read.parquet(youtube_path).select(
        F.col("video_id"),
        F.col("published_at").alias("content_date"),
        F.lit("YouTube").alias("platform"),
        F.lower(
            F.concat_ws(
                " ",
                F.coalesce(F.col("title"), F.lit("")),
                F.when(
                    F.col("tags").isNotNull(), F.concat_ws(" ", F.col("tags"))
                ).otherwise(F.lit("")),
            )
        ).alias("raw_text"),
    )

    tt_df = spark.read.parquet(tiktok_path).select(
        F.col("id").alias("video_id"),
        F.col("create_time").alias("content_date"),
        F.lit("TikTok").alias("platform"),
        F.lower(
            F.concat_ws(
                " ",
                F.coalesce(F.col("desc"), F.lit("")),
                F.when(
                    F.col("challenges").isNotNull(),
                    F.concat_ws(" ", F.col("challenges")),
                ).otherwise(F.lit("")),
            )
        ).alias("raw_text"),
    )

    df = yt_df.union(tt_df)

    df = df.withColumn("year", F.year(F.to_date("content_date")))
    df_filtered = df.filter(F.col("year").between(2020, 2025))

    words_df = df_filtered.select(
        "year",
        "platform",
        "video_id",
        F.explode(
            F.split(
                F.regexp_replace(F.col("raw_text"), "[^a-zA-Z0-9šđčćžŠĐČĆŽ\\s]", ""),
                "\\s+",
            )
        ).alias("keyword"),
    )

    filtered_words = words_df.filter(
        (F.length(F.col("keyword")) > 2)
        & (
            ~F.col("keyword").isin(
                ["the", "and", "for", "you", "with", "this", "that", "http", "https"]
            )
        )
    )

    keyword_counts = filtered_words.groupBy("year", "platform", "keyword").agg(
        F.count("video_id").alias("keyword_count")
    )

    final_result = keyword_counts.orderBy(
        "year", F.col("platform"), F.col("keyword_count").desc()
    )

    # Optional write to console
    final_result.show(50, truncate=False)

    (
        final_result
        .coalesce(1)
        .write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()
