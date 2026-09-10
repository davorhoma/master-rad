import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit


def analyze_top100_frequent_tags(
    youtube_path, tiktok_path, mongo_uri, db_name, coll_name
):
    spark = SparkSession.builder.appName("Top100FrequentTagsAnalysis").getOrCreate()

    yt_top100 = (
        spark.read.parquet(youtube_path)
        .groupBy("video_id")
        .agg(
            F.max("view_count").alias("view_count"),
            F.first("tag_array").alias("tag_array"),  # Uzimamo tagove tog videa
        )
        .orderBy(col("view_count").desc())
        .limit(100)
    )

    yt_exploded = (
        yt_top100.withColumn("tag", F.explode(col("tag_array")))
        .select(
            lit("YouTube").alias("platform"), F.lower(F.trim(col("tag"))).alias("tag")
        )
        .filter(
            (col("tag").isNotNull()) & (col("tag") != "") & (col("tag") != "[none]")
        )
    )

    yt_tag_counts = yt_exploded.groupBy("platform", "tag").agg(
        F.count("tag").cast("long").alias("tag_frequency")
    )

    tt_top100 = (
        spark.read.parquet(tiktok_path)
        .withColumn("video_id", col("id"))
        .withColumn("view_count", col("digg_count").cast("long"))
        .groupBy("video_id")
        .agg(
            F.max("view_count").alias("view_count"),
            F.first("challenges").alias("challenges"),
        )
        .orderBy(col("view_count").desc())
        .limit(100)
    )

    tt_cleaned = tt_top100.withColumn(
        "cleaned_challenges",
        # Remove [, ], and " from string
        F.translate(col("challenges"), '[]"', ""),
    )

    tt_exploded = (
        tt_cleaned.withColumn("tag_array", F.split(col("cleaned_challenges"), ","))
        .withColumn("tag", F.explode(col("tag_array")))
        .select(
            lit("TikTok").alias("platform"), F.lower(F.trim(col("tag"))).alias("tag")
        )
        .filter((col("tag").isNotNull()) & (col("tag") != ""))
    )

    tt_tag_counts = tt_exploded.groupBy("platform", "tag").agg(
        F.count("tag").cast("long").alias("tag_frequency")
    )

    combined_tags_df = yt_tag_counts.union(tt_tag_counts)
    final_result = combined_tags_df.orderBy(
        col("platform"), col("tag_frequency").desc()
    )

    # Optional write to console
    # final_result.show(30, truncate=False)

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
    analyze_top100_frequent_tags(
        sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    )
