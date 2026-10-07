import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    youtube_path = sys.argv[1]
    tiktok_path = sys.argv[2]
    mongo_uri = sys.argv[3]
    mongo_db = sys.argv[4]
    mongo_collection = sys.argv[5]

    spark = SparkSession.builder.appName("channel_daily_historical_metrics").getOrCreate()

    # 1. Učitavanje YouTube i TikTok Parquet fajlova
    youtube_df = spark.read.parquet(youtube_path)
    tiktok_df = spark.read.parquet(tiktok_path)

    # 2. Odabir potrebnih kolona
    required_columns = [
        "video_id",
        "channel_title",
        "views",
        "viral_score",
        "engagement_rate",
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
        & F.col("engagement_rate").isNotNull()
        & F.col("platform").isNotNull()
        & F.col("collected_at").isNotNull()
    )

    df = df.filter(
        (F.col("views") >= 0) & (F.col("viral_score") >= 0) & (F.col("engagement_rate") >= 0)
    )

    # 5. Normalizacija datuma
    #
    # Ako je collected_at već DateType, cast neće promeniti vrednost.
    # Ako je TimestampType, uzima se samo datum.
    df = df.withColumn("collected_at", F.to_date(F.col("collected_at")))

    # 6. Dnevna agregacija po platformi i kanalu
    #
    # views:
    #   Ukupan broj pregleda svih video zapisa koji su pripadali
    #   kanalu tog dana.
    #
    # viral_score:
    #   Prosečan viral score video zapisa kanala tog dana.
    #
    # engagement_rate:
    #   Prosečan engagement rate video zapisa kanala tog dana.
    #
    # video_count:
    #   Broj video zapisa koji su korišćeni za računanje metrika.
    daily_channel_stats = df.groupBy(
        "platform",
        "channel_title",
        "collected_at",
    ).agg(
        F.sum("views").alias("views"),
        F.avg("viral_score").alias("viral_score"),
        F.avg("engagement_rate").alias("engagement_rate"),
        F.countDistinct("video_id").alias("video_count"),
    )

    # 7. Zaokruživanje rezultata
    final_result = (
        daily_channel_stats.withColumn("views", F.round(F.col("views"), 0).cast("long"))
        .withColumn("viral_score", F.round(F.col("viral_score"), 4))
        .withColumn("engagement_rate", F.round(F.col("engagement_rate"), 6))
    )

    # 8. Konačna šema
    final_result = final_result.select(
        "platform",
        "channel_title",
        "collected_at",
        "views",
        "viral_score",
        "engagement_rate",
        "video_count",
    )

    # 9. Upis u MongoDB
    (
        final_result.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    print("Dnevne istorijske metrike kanala uspešno izračunate i upisane u MongoDB.")

    spark.stop()
