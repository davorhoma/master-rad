import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, expr


def analyze_viral_tag_combinations(
    youtube_path, tiktok_path, mongo_uri, db_name, coll_name
):
    spark = SparkSession.builder.appName("ViralTagCombinationsAnalysis").getOrCreate()

    # YouTube: Group by video_id, sort by max viral_score, limit 100 unique videos
    yt_viral = (
        spark.read.parquet(youtube_path)
        .groupBy("video_id")
        .agg(
            F.max("viral_score").cast("double").alias("viral_score"),
            F.first("tag_array").alias(
                "tag_array"
            ),  # Koristimo već pripremljen niz iz Parquet-a
        )
        .orderBy(col("viral_score").desc())
        .limit(100)
    )

    yt_videos = (
        yt_viral.withColumn("tag_array", F.array_distinct(col("tag_array")))
        .filter((col("tag_array").isNotNull()) & (F.size(col("tag_array")) >= 2))
        .withColumn("tag_array", F.array_sort(col("tag_array")))
        .withColumn(
            "tag_pairs",
            expr("""
                transform(
                    sequence(0, size(tag_array) - 2),
                    i -> transform(
                        slice(tag_array, i + 2, size(tag_array)),
                        x -> concat(tag_array[i], ' + ', x)
                    )
                )
            """),
        )
    )

    yt_exploded = yt_videos.withColumn(
        "tag_combination", F.explode(expr("flatten(tag_pairs)"))
    )

    yt_combination_counts = (
        yt_exploded.select(lit("YouTube").alias("platform"), col("tag_combination"))
        .groupBy("platform", "tag_combination")
        .agg(F.count("tag_combination").cast("long").alias("combination_frequency"))
    )

    # TikTok: Group by video_id, sort by max viral_score, limit 100 unique videos
    tt_viral = (
        spark.read.parquet(tiktok_path)
        .withColumn("video_id", col("id"))
        .withColumn("viral_score", col("viral_score").cast("double"))
        .groupBy("video_id")
        .agg(
            F.max("viral_score").alias("viral_score"),
            F.first("challenges").alias("challenges"),
        )
        .orderBy(col("viral_score").desc())
        .limit(100)
    )

    tt_cleaned_text = tt_viral.withColumn(
        "cleaned_challenges", F.translate(col("challenges"), '[]"', "")
    )

    tt_videos = (
        tt_cleaned_text.withColumn(
            "tag_array", F.array_distinct(F.split(col("cleaned_challenges"), ","))
        )
        .filter((col("tag_array").isNotNull()) & (F.size(col("tag_array")) >= 2))
        .withColumn("tag_array", F.array_sort(col("tag_array")))
        .withColumn(
            "tag_pairs",
            expr("""
                transform(
                    sequence(0, size(tag_array) - 2),
                    i -> transform(
                        slice(tag_array, i + 2, size(tag_array)),
                        x -> concat(tag_array[i], ' + ', x)
                    )
                )
            """),
        )
    )

    tt_exploded = tt_videos.withColumn(
        "tag_combination", F.explode(expr("flatten(tag_pairs)"))
    )

    tt_combination_counts = (
        tt_exploded.select(lit("TikTok").alias("platform"), col("tag_combination"))
        .groupBy("platform", "tag_combination")
        .agg(F.count("tag_combination").cast("long").alias("combination_frequency"))
    )

    # Combine & save to MongoDB
    combined_combinations = yt_combination_counts.union(tt_combination_counts)
    final_result = combined_combinations.orderBy(
        col("platform"), col("combination_frequency").desc()
    )

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
    analyze_viral_tag_combinations(
        sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    )
