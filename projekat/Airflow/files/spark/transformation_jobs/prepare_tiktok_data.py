from pyspark.sql import SparkSession, functions as F
import sys

# Konstantna vrednost za čišćenje ekstremnih pregleda
MAX_REALISTIC_PLAYS = 10_000_000_000


if __name__ == "__main__":
    input_file_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("prepare_tiktok_gaming_data").getOrCreate()

    # Čitanje CSV fajla
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(input_file_path)
    )

    TIKTOK_COLUMNS = [
        "id",
        "collected_time",
        "create_time",
        "desc",
        "comment_count",
        "digg_count",
        "play_count",
        "share_count",
        "duration",
        "user_id",
        "challenges",
        "url",
        "keyword",
        "author_name",
        "author_unique_id",
    ]

    df = df.select(*TIKTOK_COLUMNS)

    # Ujednačavanje naziva kolona
    rename_columns = {
        "id": "video_id",
        "collected_time": "collected_at",
        "create_time": "published_at",
        "desc": "description",
        "comment_count": "comments",
        "digg_count": "likes",
        "play_count": "views",
        "share_count": "shares",
        "duration": "duration_seconds",
        "user_id": "channel_id",
        "challenges": "tags",
        "author_name": "channel_title",
        "author_unique_id": "username",
    }

    for old_name, new_name in rename_columns.items():
        if old_name != new_name and old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    # Tipizacija podataka
    # Vremenske kolone su Unix timestamp izražene u sekundama.
    df = (
        df.withColumn(
            "published_at",
            F.to_timestamp(F.from_unixtime(F.col("published_at").cast("long"))),
        )
        .withColumn(
            "collected_at",
            F.to_timestamp(F.from_unixtime(F.col("collected_at").cast("long"))),
        )
        .withColumn("views", F.col("views").cast("long"))
        .withColumn("likes", F.col("likes").cast("long"))
        .withColumn("comments", F.col("comments").cast("long"))
        .withColumn("shares", F.col("shares").cast("long"))
        .withColumn("duration_seconds", F.col("duration_seconds").cast("int"))
        .withColumn("platform", F.lit("TikTok"))
    )

    # Čišćenje ekstremnih vrednosti
    df = df.filter(
        (F.col("views") > 0)
        & (F.col("likes") <= F.col("views"))
        & (F.col("views") <= MAX_REALISTIC_PLAYS)
    )

    # Priprema hashtagova
    # Kolona 'tags' sadrži hashtagove razdvojene zarezima.
    df = df.withColumn(
        "tags",
        F.when(
            F.col("tags").isNull() | (F.trim(F.col("tags")) == ""),
            F.expr("cast(array() as array<string>)"),
        ).otherwise(F.split(F.col("tags"), r",")),
    )

    df = df.withColumn("tag_count", F.size(F.col("tags")))

    # Dodavanje analitičkih metrika
    df = (
        df.withColumn("like_ratio", F.col("likes") / F.col("views"))
        .withColumn("comment_ratio", F.col("comments") / F.col("views"))
        .withColumn("engagement_rate", (F.col("likes") + F.col("comments")) / F.col("views"))
    )

    # Izračunavanje viral_score metrike
    df = df.withColumn(
        "viral_score",
        F.round(
            F.log10(F.col("views")) * 0.50
            + F.col("like_ratio") * 100 * 0.30
            + F.col("comment_ratio") * 100 * 0.20,
            4,
        ),
    )

    # Čuvanje u Parquet formatu
    df.write.mode("overwrite").parquet(output_path)

    print("Priprema TikTok Gaming podataka uspešno završena.")
    spark.stop()
