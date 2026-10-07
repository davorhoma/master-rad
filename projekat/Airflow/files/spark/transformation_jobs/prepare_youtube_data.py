from pyspark.sql import SparkSession, functions as F
import sys
import re

# Konstantna vrednost za čišćenje ekstremnih vrednosti
MAX_REALISTIC_VIEWS = 25_000_000_000


def to_snake_case(name: str) -> str:
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


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
        "publishedAt",
        "channelId",
        "channelTitle",
        "trending_date",
        "tags",
        "view_count",
        "likes",
        "dislikes",
        "comment_count",
        "description",
        "duration",
    ]

    df = df.select(*YOUTUBE_COLUMNS)

    # 2. Normalizacija naziva kolona u snake_case
    df = df.select(*[F.col(c).alias(to_snake_case(c)) for c in df.columns])

    # 3. Ujednačavanje naziva kolona
    rename_columns = {
        "view_count": "views",
        "likes": "likes",
        "comment_count": "comments",
    }

    for old_name, new_name in rename_columns.items():
        if old_name != new_name and old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    # 4. Tipizacija podataka i normalizacija vremena
    df = (
        df.withColumn("trending_date", F.to_date(F.col("trending_date"), "yy.dd.MM"))
        .withColumn("published_at", F.to_timestamp(F.col("published_at")))
        .withColumn("views", F.col("views").cast("long"))
        .withColumn("likes", F.col("likes").cast("long"))
        .withColumn("dislikes", F.col("dislikes").cast("long"))
        .withColumn("comments", F.col("comments").cast("long"))
    )

    # trending_date predstavlja vreme prikupljanja podataka
    df = df.withColumnRenamed("trending_date", "collected_at").withColumn(
        "platform", F.lit("YouTube")
    )

    # 5. Čišćenje ekstremnih vrednosti
    df = df.filter(
        (F.col("views") > 0)
        & (F.col("likes") <= F.col("views"))
        & (F.col("views") <= MAX_REALISTIC_VIEWS)
    )

    # 6. Transformacija ISO 8601 trajanja u sekunde
    df = (
        df.withColumn("duration_h", F.regexp_extract("duration", r"PT(\d+)H", 1).cast("int"))
        .withColumn(
            "duration_m",
            F.regexp_extract("duration", r"PT(?:\d+H)?(\d+)M", 1).cast("int"),
        )
        .withColumn(
            "duration_s",
            F.regexp_extract("duration", r"PT(?:\d+H)?(?:\d+M)?(\d+)S", 1).cast("int"),
        )
        .withColumn(
            "duration_seconds",
            F.coalesce(F.col("duration_h"), F.lit(0)) * 3600
            + F.coalesce(F.col("duration_m"), F.lit(0)) * 60
            + F.coalesce(F.col("duration_s"), F.lit(0)),
        )
        .drop("duration_h", "duration_m", "duration_s", "duration")
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

    # 9. Izračunavanje viral_score metrike
    df = df.withColumn(
        "viral_score",
        F.round(
            F.log10(F.col("views")) * 0.50
            + F.col("like_ratio") * 100 * 0.30
            + F.col("comment_ratio") * 100 * 0.20,
            4,
        ),
    )

    # 10. Čuvanje u Parquet formatu
    df.write.mode("overwrite").parquet(output_path)

    print("Priprema YouTube Gaming podataka uspešno završena.")
    spark.stop()
