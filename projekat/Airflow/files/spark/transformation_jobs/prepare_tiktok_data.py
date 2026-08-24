from pyspark.sql import SparkSession, functions as F
import sys

# Konstantna vrednost za čišćenje ekstremnih pregleda
MAX_REALISTIC_PLAYS = 10_000_000_000

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("prepare_tiktok_gaming_data").getOrCreate()

    # Čitanje TikTok ulaznog CSV fajla
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(input_file_path)
    )

    # 1. Tipizacija podataka (Cast)
    # Na TikTok-u su pregledi pod nazivom play_count, lajkovi kao digg_count, a komentari comment_count
    df = (
        df.withColumn(
            "create_time", F.to_timestamp(F.col("create_time").cast("long"))
        )  # Ako je timestamp u sekundama, ili prilagodi format
        .withColumn(
            "collected_time", F.to_timestamp(F.col("collected_time").cast("long"))
        )
        .withColumn("play_count", F.col("play_count").cast("long"))
        .withColumn(
            "digg_count", F.col("digg_count").cast("long")
        )  # Lajkovi na TikToku
        .withColumn("comment_count", F.col("comment_count").cast("long"))
        .withColumn("share_count", F.col("share_count").cast("long"))
        .withColumn("collect_count", F.col("collect_count").cast("long"))
        .withColumn(
            "duration", F.col("duration").cast("int")
        )  # Na TikToku je duration obično već u sekundama (int)
    )

    # 2. Čišćenje podataka (Outliers po broju pregleda)
    df = df.filter(F.col("play_count") <= MAX_REALISTIC_PLAYS)

    # 3. Priprema i obrada izazova/tagova (Značajno za TikTok pitanja o tagovima)
    # Kolona 'challenges' obično sadrži hashtagove (npr. u JSON formatu ili razdvojene nekim karakterom)
    # Ovde hvatamo kolonu, a broj tagova računamo na osnovu razdvajanja ili dužine
    df = df.withColumn("clean_challenges", F.coalesce(F.col("challenges"), F.lit("")))

    # Pretpostavka: izazovi/tagovi su razdvojeni zarezom ili razmakom (prilagodi separator po potrebi)
    df = df.withColumn(
        "tag_array", F.split(F.col("clean_challenges"), r",")
    ).withColumn(
        "tag_count",
        F.when(
            (F.col("challenges") == "") | (F.col("challenges").isNull()), F.lit(0)
        ).otherwise(F.size(F.split(F.col("clean_challenges"), r","))),
    )

    # 4. Dodavanje analitičkih metrika i 'viral_score' prilagođenog za TikTok
    # Koristimo play_count umesto views, i digg_count umesto likes
    df = (
        df.withColumn("safe_plays", F.greatest(F.col("play_count"), F.lit(1)))
        .withColumn(
            "like_ratio", F.least(F.col("digg_count") / F.col("safe_plays"), F.lit(1.0))
        )
        .withColumn(
            "comment_ratio",
            F.least(F.col("comment_count") / F.col("safe_plays"), F.lit(1.0)),
        )
    )

    # Izračunavanje viral_score metrike po analogiji sa YouTube-om (uz prilagođene nazive kolona)
    df = df.withColumn(
        "viral_score",
        F.round(
            F.log10(F.col("safe_plays")) * 0.50
            + F.col("like_ratio") * 100 * 0.30
            + F.col("comment_ratio") * 100 * 0.20,
            4,
        ),
    ).drop("safe_plays")

    # 5. Čuvanje pripremljenih podataka u Parquet formatu
    df.write.mode("overwrite").parquet(output_path)

    print("Priprema TikTok Gaming podataka uspešno završena.")
    spark.stop()
