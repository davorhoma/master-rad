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

    spark = SparkSession.builder.appName("channel_content_popularity_quadrant").getOrCreate()

    # 1. Učitavanje YouTube i TikTok Parquet fajlova
    youtube_df = spark.read.parquet(youtube_path)
    tiktok_df = spark.read.parquet(tiktok_path)

    # 2. Odabir zajedničkih kolona
    required_columns = [
        "video_id",
        "channel_title",
        "views",
        "viral_score",
        "platform",
        "collected_at",
    ]

    youtube_df = youtube_df.select(*required_columns)
    tiktok_df = tiktok_df.select(*required_columns)

    # 3. Spajanje YouTube i TikTok podataka
    df = youtube_df.unionByName(tiktok_df)

    # 4. Osnovno čišćenje
    df = df.filter(
        F.col("video_id").isNotNull()
        & F.col("channel_title").isNotNull()
        & F.col("views").isNotNull()
        & F.col("viral_score").isNotNull()
        & F.col("platform").isNotNull()
    )

    df = df.filter((F.col("views") >= 0) & (F.col("viral_score") >= 0))

    # 5. Uzimanje najnovijeg zapisa za svaki video
    latest_video_window = Window.partitionBy("platform", "video_id").orderBy(
        F.col("collected_at").desc()
    )

    df = (
        df.withColumn(
            "_row_number",
            F.row_number().over(latest_video_window),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    # 6. Agregacija video zapisa na nivo kanala
    channel_stats = df.groupBy(
        "platform",
        "channel_title",
    ).agg(
        F.count("*").alias("video_count"),
        F.percentile_approx(
            F.col("views"),
            0.5,
            10000,
        ).alias("median_views"),
        F.percentile_approx(
            F.col("viral_score"),
            0.5,
            10000,
        ).alias("median_viral_score"),
    )

    # 7. Percentile rank za popularnost kanala i sadržaja
    channel_popularity_window = Window.partitionBy("platform").orderBy(F.col("median_views"))

    content_popularity_window = Window.partitionBy("platform").orderBy(F.col("median_viral_score"))

    channel_stats = channel_stats.withColumn(
        "channel_popularity",
        F.percent_rank().over(channel_popularity_window) * 100,
    ).withColumn(
        "content_popularity",
        F.percent_rank().over(content_popularity_window) * 100,
    )

    # 8. Zaokruživanje percentile vrednosti
    channel_stats = channel_stats.withColumn(
        "channel_popularity",
        F.round(
            F.col("channel_popularity"),
            2,
        ),
    ).withColumn(
        "content_popularity",
        F.round(
            F.col("content_popularity"),
            2,
        ),
    )

    # 9. Određivanje kvadranta
    final_result = (
        channel_stats.withColumn(
            "channel_is_popular",
            F.col("channel_popularity") >= 50,
        )
        .withColumn(
            "content_is_popular",
            F.col("content_popularity") >= 50,
        )
        .withColumn(
            "quadrant",
            F.when(
                F.col("channel_is_popular") & F.col("content_is_popular"),
                F.lit("Q1 - Popular channel / Popular content"),
            )
            .when(
                F.col("channel_is_popular") & ~F.col("content_is_popular"),
                F.lit("Q2 - Popular channel / Less popular content"),
            )
            .when(
                ~F.col("channel_is_popular") & ~F.col("content_is_popular"),
                F.lit("Q3 - Less popular channel / Less popular content"),
            )
            .otherwise(
                F.lit("Q4 - Less popular channel / Popular content"),
            ),
        )
    )

    # 10. Konačna šema
    final_result = final_result.select(
        "platform",
        "channel_title",
        "video_count",
        "median_views",
        "median_viral_score",
        "channel_popularity",
        "content_popularity",
        "channel_is_popular",
        "content_is_popular",
        "quadrant",
    )

    # 11. Upis u MongoDB
    (
        final_result.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    print("Channel/content popularity quadrant dataset uspešno izračunat i upisan u MongoDB.")

    spark.stop()
