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
        "video_id",
        "collected_at",
        "published_at",
        "description",
        "comments",
        "likes",
        "views",
        "shares",
        "duration",
        "video_url",
        "search_query",
        "channel_title",
        "tags",
        "viral_score",
        "source_type",
    ]

    df = df.select(*TIKTOK_COLUMNS)

    publish_time = F.trim(F.col("published_at"))
    detected_time = F.trim(F.col("collected_at"))
    df = (
        df.withColumn(
            "published_at",
            F.when(
                publish_time.rlike(r"^\d+(\.\d+)?$"),
                F.to_timestamp(F.from_unixtime(publish_time.cast("double").cast("long"))),
            ).otherwise(F.to_timestamp(publish_time)),
        )
        .withColumn(
            "collected_at",
            F.when(
                detected_time.rlike(r"^\d+(\.\d+)?$"),
                F.to_timestamp(F.from_unixtime(detected_time.cast("double").cast("long"))),
            ).otherwise(F.to_timestamp(detected_time)),
        )
        .withColumn("views", F.col("views").cast("long"))
        .withColumn("likes", F.col("likes").cast("long"))
        .withColumn("comments", F.col("comments").cast("long"))
        .withColumn("shares", F.col("shares").cast("long"))
        .withColumn("platform", F.lit("TikTok"))
    )

    duration_text = F.trim(F.col("duration"))
    duration_parts = F.split(duration_text, ":")
    df = df.withColumn(
        "duration_seconds",
        F.when(
            duration_text.rlike(r"^\d+(\.\d+)?$"),
            duration_text.cast("double").cast("int"),
        )
        .when(
            duration_text.rlike(r"^\d+:\d{2}$"),
            duration_parts.getItem(0).cast("int") * 60
            + duration_parts.getItem(1).cast("int"),
        )
        .otherwise(F.lit(None).cast("int")),
    )
    df = (
        df.withColumn("id", F.col("video_id"))
        .withColumn("collected_time", F.col("collected_at"))
        .withColumn("desc", F.col("description"))
        .withColumn("comment_count", F.col("comments"))
        .withColumn("digg_count", F.col("likes"))
        .withColumn("play_count", F.col("views"))
        .withColumn("share_count", F.col("shares"))
        .withColumn("author_name", F.col("channel_title"))
    )

    # Čišćenje ekstremnih vrednosti
    df = df.filter(
        (F.col("views") > 0)
        & (F.col("likes") <= F.col("views"))
        & (F.col("views") <= MAX_REALISTIC_PLAYS)
    )

    # Priprema hashtagova razdvojenih zarezima.
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
