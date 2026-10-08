from pyspark.sql import SparkSession, functions as F
import sys

# Konstantna vrednost za čišćenje ekstremnih vrednosti
MAX_REALISTIC_VIEWS = 25_000_000_000


if __name__ == "__main__":
    input_file_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("prepare_youtube_gaming_data").getOrCreate()

    # 1. Čitanje CSV fajla
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(input_file_path)
    )

    YOUTUBE_COLUMNS = [
        "video_id",
        "title",
        "published_at",
        "channel_id",
        "channel_title",
        "collected_at",
        "tags",
        "views",
        "likes",
        "comments",
        "description",
        "duration",
        "source_type",
        "region_code",
        "category_id",
        "viral_score",
    ]

    df = df.select(*YOUTUBE_COLUMNS)

    # Normalize joined-source timestamps and numeric fields.
    df = (
        df.withColumn("collected_at", F.to_timestamp(F.col("collected_at")))
        .withColumn("published_at", F.to_timestamp(F.col("published_at")))
        .withColumn("views", F.col("views").cast("long"))
        .withColumn("likes", F.col("likes").cast("long"))
        .withColumn("comments", F.col("comments").cast("long"))
    )

    df = (
        df.withColumn("trending_date", F.to_date(F.col("collected_at")))
        .withColumn("view_count", F.col("views"))
        .withColumn("comment_count", F.col("comments"))
        .withColumn("dislikes", F.lit(None).cast("long"))
        .withColumn("platform", F.lit("YouTube"))
    )

    # 5. Čišćenje ekstremnih vrednosti
    df = df.filter(
        (F.col("views") > 0)
        & (F.col("likes") <= F.col("views"))
        & (F.col("views") <= MAX_REALISTIC_VIEWS)
    )

    # Convert ISO 8601 duration values into seconds.
    df = (
        df.withColumn("duration_h_text", F.regexp_extract("duration", r"PT(\d+)H", 1))
        .withColumn(
            "duration_m_text",
            F.regexp_extract("duration", r"PT(?:\d+H)?(\d+)M", 1),
        )
        .withColumn(
            "duration_s_text",
            F.regexp_extract("duration", r"PT(?:\d+H)?(?:\d+M)?(\d+)S", 1),
        )
        .withColumn(
            "duration_h",
            F.when(F.col("duration_h_text") == "", 0).otherwise(
                F.col("duration_h_text").cast("int")
            ),
        )
        .withColumn(
            "duration_m",
            F.when(F.col("duration_m_text") == "", 0).otherwise(
                F.col("duration_m_text").cast("int")
            ),
        )
        .withColumn(
            "duration_s",
            F.when(F.col("duration_s_text") == "", 0).otherwise(
                F.col("duration_s_text").cast("int")
            ),
        )
        .withColumn(
            "duration_seconds",
            F.col("duration_h") * 3600
            + F.col("duration_m") * 60
            + F.col("duration_s"),
        )
        .drop(
            "duration_h_text",
            "duration_m_text",
            "duration_s_text",
            "duration_h",
            "duration_m",
            "duration_s",
            "duration",
        )
    )

    # 7. Priprema tagova
    # Izvorna kolona tags sadrži tekst; izlazna kolona tags je niz tagova.
    df = df.withColumn(
        "tags",
        F.when(
            F.col("tags").isNull() | (F.col("tags") == "") | (F.col("tags") == "[none]"),
            F.expr("cast(array() as array<string>)"),
        ).otherwise(F.split(F.col("tags"), r"\|")),
    )

    df = df.withColumn("tag_count", F.size(F.col("tags")))

    # 8. Dodavanje analitičkih metrika
    df = (
        df.withColumn("like_ratio", F.col("likes") / F.col("views"))
        .withColumn("comment_ratio", F.col("comments") / F.col("views"))
        .withColumn("engagement_rate", (F.col("likes") + F.col("comments")) / F.col("views"))
    )

    calculated_viral_score = F.round(
        F.log10(F.col("views")) * 0.50
        + F.col("like_ratio") * 100 * 0.30
        + F.col("comment_ratio") * 100 * 0.20,
        4,
    )
    df = df.withColumn(
        "viral_score",
        F.coalesce(F.col("viral_score").cast("double"), calculated_viral_score),
    )

    # 10. Čuvanje u Parquet formatu
    df.write.mode("overwrite").parquet(output_path)

    print("Priprema YouTube Gaming podataka uspešno završena.")
    spark.stop()
