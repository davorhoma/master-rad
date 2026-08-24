import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit


def analyze_top100_tags_count(youtube_path, tiktok_path, mongo_uri, db_name, coll_name):
    spark = SparkSession.builder.appName("Top100MostViewedTagsAnalysis").getOrCreate()

    yt_df = (
        spark.read.parquet(youtube_path)
        .select(
            col("video_id"),
            col("view_count").cast("long"),
            col("tag_count").cast("int"),
            lit("YouTube").alias("platform"),
        )
        .orderBy(col("view_count").desc())
        .limit(100)
    )

    tt_df = (
        spark.read.parquet(tiktok_path)
        .select(
            col("id").alias("video_id"),
            col("digg_count").cast("long").alias("view_count"),
            F.size(F.split(col("challenges"), ",")).alias("tag_count"),
            lit("TikTok").alias("platform"),
        )
        .orderBy(col("view_count").desc())
        .limit(100)
    )

    combined_df = yt_df.union(tt_df)

    avg_result = combined_df.groupBy("platform").agg(
        F.avg("tag_count").alias("avg_tag_count"),
        F.min("tag_count").alias("min_tag_count"),
        F.max("tag_count").alias("max_tag_count"),
        F.count("video_id").alias("sample_size"),
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


def inspect_top5_tags(youtube_path, tiktok_path):
    spark = SparkSession.builder.appName("InspectTop5Tags").getOrCreate()

    print("--- TOP 5 YOUTUBE VIDEOS (TAGS) ---")
    yt_df = (
        spark.read.parquet(youtube_path)
        .select(col("video_id"), col("view_count").cast("long"), col("tags"))
        .orderBy(col("view_count").desc())
        .limit(5)
    )
    for row in yt_df.collect():
        print(f"Video ID: {row['video_id']} | Views: {row['view_count']}")
        print(f"Tags: {row['tags']}\n")

    print("--- TOP 5 TIKTOK VIDEOS (CHALLENGES) ---")
    tt_df = (
        spark.read.parquet(tiktok_path)
        .select(
            col("id").alias("video_id"),
            col("digg_count").cast("long").alias("digg_count"),
            col("challenges"),
        )
        .orderBy(col("digg_count").desc())
        .limit(5)
    )
    for row in tt_df.collect():
        print(f"Video ID: {row['video_id']} | Digg Count: {row['digg_count']}")
        print(f"Challenges: {row['challenges']}\n")

    spark.stop()


if __name__ == "__main__":
    analyze_top100_tags_count(
        sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    )

    inspect_top5_tags(sys.argv[1], sys.argv[2])
