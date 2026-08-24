import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit


def analyze_video_length_impact(
    youtube_path, tiktok_path, mongo_uri, db_name, coll_name
):
    spark = SparkSession.builder.appName("VideoLengthPopularityAnalysis").getOrCreate()

    yt_df = (
        spark.read.parquet(youtube_path)
        .withColumn(
            "length_category",
            F.when(col("duration_seconds") <= 300, "short (< 5 min)")
            .when(
                (col("duration_seconds") > 300) & (col("duration_seconds") <= 1200),
                "medium (5 - 20 min)",
            )
            .otherwise("long (> 20 min)"),
        )
        .select(
            col("video_id"),
            col("length_category"),
            col("viral_score"),
            lit("YouTube").alias("platform"),
        )
    )

    tt_df = (
        spark.read.parquet(tiktok_path)
        .withColumn(
            "length_category",
            F.when(col("duration") <= 30, "short (< 30 s)")
            .when(
                (col("duration") > 30) & (col("duration") <= 180),
                "medium (30 s - 3 min)",
            )
            .otherwise("long (> 3 min)"),
        )
        .select(
            col("id").alias("video_id"),  # TikTok uses 'id' instead of 'video_id'
            col("length_category"),
            col("viral_score"),
            lit("TikTok").alias("platform"),
        )
    )

    combined_df = yt_df.union(tt_df)

    final_result = combined_df.select(
        col("platform"), col("length_category"), col("viral_score")
    )

    # Optional write to console
    # final_result.show(20, truncate=False)

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
    analyze_video_length_impact(
        sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    )
