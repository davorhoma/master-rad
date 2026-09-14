from pyspark.sql import SparkSession, functions as F
import sys

# Konstantna vrednost za čišćenje ekstremnih vrednosti
MAX_REALISTIC_VIEWS = 25_000_000_000


def to_snake_case(name: str) -> str:
    import re

    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


if __name__ == "__main__":
    input_file_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("prepare_youtube_gaming_data").getOrCreate()

    # Čitanje ulaznog CSV fajla na osnovu tvog headera
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(input_file_path)
    )

    # 1. Normalizacija naziva kolona u snake_case
    df = df.select(*[F.col(c).alias(to_snake_case(c)) for c in df.columns])

    # 2. Tipizacija podataka (Cast)
    df = (
        df.withColumn(
            "trending_date", F.to_date("trending_date", "yy.dd.MM")
        )  # Prilagodi format datuma po potrebi
        .withColumn("published_at", F.to_timestamp("published_at"))
        .withColumn("view_count", F.col("view_count").cast("long"))
        .withColumn("likes", F.col("likes").cast("long"))
        .withColumn("dislikes", F.col("dislikes").cast("long"))
        .withColumn("comment_count", F.col("comment_count").cast("long"))
    )

    # 3. Čišćenje podataka (Outliers)
    df = df.filter(F.col("view_count") <= MAX_REALISTIC_VIEWS)

    # 4. Transformacija trajanja (ISO 8601 u sekunde)
    # Koristi se za pitanja vezana za dužinu videa i kategorizaciju (do 5 min, 5-20 min, preko 20 min)
    df = (
        df.withColumn(
            "duration_h", F.regexp_extract("duration", r"PT(\d+)H", 1).cast("int")
        )
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

    # 5. Priprema i obrada tagova (Značajno za pitanja 7, 8 i 9)
    df = df.withColumn("clean_tags", F.coalesce(F.col("tags"), F.lit("")))

    # Ako su tagovi razdvojeni sa "|" (standardno za YouTube API/datasetove)
    df = df.withColumn("tag_array", F.split(F.col("clean_tags"), r"\|")).withColumn(
        "tag_count",
        F.when(
            (F.col("tags") == "[none]") | (F.col("tags").isNull()), F.lit(0)
        ).otherwise(F.size(F.split(F.col("clean_tags"), r"\|"))),
    )

    # 6. Dodavanje analitičkih metrika i 'viral_score'
    df = (
        df.withColumn("safe_views", F.greatest(F.col("view_count"), F.lit(1)))
        .withColumn(
            "like_ratio", F.least(F.col("likes") / F.col("safe_views"), F.lit(1.0))
        )
        .withColumn(
            "comment_ratio",
            F.least(F.col("comment_count") / F.col("safe_views"), F.lit(1.0)),
        )
    )

    # Izračunavanje viral_score metrike prema definiciji
    df = df.withColumn(
        "viral_score",
        F.round(
            F.log10(F.col("safe_views")) * 0.50
            + F.col("like_ratio") * 100 * 0.30
            + F.col("comment_ratio") * 100 * 0.20,
            4,
        ),
    ).drop("safe_views")

    # 7. Čuvanje pripremljenih podataka u Parquet formatu
    df.write.mode("overwrite").parquet(output_path)

    print("Priprema YouTube Gaming podataka uspešno završena.")
    spark.stop()
