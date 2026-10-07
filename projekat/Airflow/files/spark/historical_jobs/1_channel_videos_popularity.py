import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

if __name__ == "__main__":
    youtube_path = sys.argv[1]
    tiktok_path = sys.argv[2]
    mongo_uri = sys.argv[3]
    mongo_db = sys.argv[4]
    mongo_collection = sys.argv[5]

    spark = SparkSession.builder.appName("channel_videos_popularity").getOrCreate()

    # 1. Učitavanje YouTube i TikTok Parquet fajlova
    youtube_df = spark.read.parquet(youtube_path)
    tiktok_df = spark.read.parquet(tiktok_path)

    # 2. Odabir i ujednačavanje potrebnih kolona
    #
    # YouTube ima title, dok TikTok nema title.
    # Za TikTok koristimo description kao naziv/prikazni tekst.
    youtube_df = youtube_df.select(
        "video_id",
        "title",
        "channel_title",
        "views",
        "likes",
        "comments",
        "viral_score",
        "engagement_rate",
        "platform",
        "collected_at",
    )

    tiktok_df = tiktok_df.select(
        "video_id",
        F.col("description").alias("title"),
        "channel_title",
        "views",
        "likes",
        "comments",
        "viral_score",
        "engagement_rate",
        "platform",
        "collected_at",
    )

    # 3. Spajanje YouTube i TikTok podataka
    df = youtube_df.unionByName(tiktok_df)

    # 4. Osnovno čišćenje
    df = df.filter(
        F.col("video_id").isNotNull()
        & F.col("channel_title").isNotNull()
        & F.col("platform").isNotNull()
        & F.col("collected_at").isNotNull()
        & F.col("views").isNotNull()
        & F.col("viral_score").isNotNull()
    )

    # 5. Normalizacija datuma
    df = df.withColumn("collected_at", F.to_date(F.col("collected_at")))

    # 6. Deduplikacija video klipova
    #
    # Isti video može imati više historical zapisa.
    # Zadržavamo samo NAJNOVIJE prikupljeni zapis za svaki
    # video na određenoj platformi.
    latest_video_window = Window.partitionBy("platform", "video_id").orderBy(
        F.col("collected_at").desc()
    )

    df = (
        df.withColumn("_latest_rank", F.row_number().over(latest_video_window))
        .filter(F.col("_latest_rank") == 1)
        .drop("_latest_rank")
    )

    # 7. Rangiranje videa unutar kanala/platforme
    #
    # Primarna metrika popularnosti je viral_score.
    # views i video_id služe kao tie-breakeri.
    channel_window = Window.partitionBy("platform", "channel_title").orderBy(
        F.col("viral_score").desc(),
        F.col("views").desc(),
        F.col("video_id").asc(),
    )

    df = df.withColumn("popularity_rank", F.row_number().over(channel_window))

    # 8. Zadržavanje maksimalno 15 najpopularnijih videa
    df = df.filter(F.col("popularity_rank") <= 15)

    df = df.withColumn(
        "short_title",
        F.when(
            F.length(F.col("title")) > 100,
            F.concat(F.substring(F.col("title"), 1, 97), F.lit("...")),
        ).otherwise(F.col("title")),
    )

    # 9. Odabir podataka za Superset
    final_result = df.select(
        "platform",
        "channel_title",
        "video_id",
        "title",
        "short_title",
        "collected_at",
        "views",
        "likes",
        "comments",
        "viral_score",
        "engagement_rate",
        "popularity_rank",
    )

    # 10. Upis u MongoDB
    (
        final_result.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    print(
        "Top 15 najpopularnijih video klipova po kanalu " "uspešno izračunato i upisano u MongoDB."
    )

    from pymongo import MongoClient
    
    client = MongoClient(mongo_uri)

    db = client[mongo_db]
    collection = db[mongo_collection]

    collection.create_index(
        [
            ("channel_title", 1),
            ("platform", 1),
            ("popularity_rank", 1),
        ],
        name="channel_platform_rank_idx",
    )

    client.close()
