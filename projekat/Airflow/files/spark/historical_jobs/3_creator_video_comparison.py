"""
Build top-K similar videos for YouTube and TikTok.

Similarity formula:
    score = (
        0.45 * tag_similarity
        + 0.35 * text_similarity
        + 0.10 * same_theme
        + 0.10 * duration_similarity
    )

Candidate generation:
    MinHashLSH over tags, independently for each platform.

Text:
    YouTube: title + description
    TikTok: description

PostgreSQL:
    Write results and processed source IDs to staging tables.
    Atomically replace existing similarity rows for processed source IDs.
    This removes stale relationships when a batch job is rerun.
"""

import json
import math
import os
import re
import sys
import uuid

from pyspark.ml.feature import (
    CountVectorizer,
    IDF,
    MinHashLSH,
    RegexTokenizer,
)
from pyspark.ml.linalg import VectorUDT
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col, lit, row_number, size, udf, when

from batch_utils import map_to_canonical_theme
from postgres_sink import ensure_postgres_table


import time
from contextlib import contextmanager


@contextmanager
def log_duration(label):
    start = time.perf_counter()
    print(f"[TIMER] START: {label}", flush=True)

    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        print(
            f"[TIMER] END: {label} | " f"Duration: {elapsed:.2f}s ({elapsed / 60:.2f} min)",
            flush=True,
        )


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

DEFAULT_TOP_K = 20
DEFAULT_LSH_DISTANCE_THRESHOLD = 0.5
DEFAULT_LSH_NUM_HASH_TABLES = 5

TAG_WEIGHT = 0.45
TEXT_WEIGHT = 0.35
THEME_WEIGHT = 0.10
DURATION_WEIGHT = 0.10

OUTPUT_COLUMNS = [
    "platform",
    "video_id",
    "similar_video_id",
    "similarity_score",
    "similarity_rank",
    "tag_similarity",
    "text_similarity",
    "same_theme",
    "duration_similarity",
]

EMPTY_RESULTS_SCHEMA = """
    platform string,
    video_id string,
    similar_video_id string,
    similarity_score double,
    similarity_rank int,
    tag_similarity double,
    text_similarity double,
    same_theme boolean,
    duration_similarity double
"""


# ---------------------------------------------------------------------
# General helpers
# ---------------------------------------------------------------------


def quote_identifier(identifier):
    """Validate and quote a PostgreSQL identifier."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Invalid PostgreSQL identifier: {identifier!r}")
    return f'"{identifier}"'


def table_name(schema, table):
    return f"{quote_identifier(schema)}." f"{quote_identifier(table)}"


def parse_tags(value):
    """
    Normalize tags represented as:
      - an array/list,
      - a JSON array string,
      - a comma-, semicolon-, or pipe-separated string.

    Returns a sorted, lowercase, deduplicated list.
    """
    if value is None:
        return []

    if isinstance(value, str):
        value = value.strip()

        if not value:
            return []

        if value.startswith("[") and value.endswith("]"):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    value = parsed
            except (ValueError, TypeError):
                pass

        if isinstance(value, str):
            value = re.split(r"[,;|]", value)

    if not isinstance(value, (list, tuple, set)):
        value = [value]

    result = set()

    for item in value:
        if item is None:
            continue

        token = str(item).strip().lower()
        token = token.lstrip("#").strip()
        token = re.sub(r"\s+", " ", token)

        if token:
            result.add(token)

    return sorted(result)


def normalize_text(value):
    if value is None:
        return ""

    return re.sub(r"\s+", " ", str(value).lower()).strip()


def canonical_theme_for_record(title, description, tags):
    """
    Use the existing canonical theme mapper directly.

    The mapper accepts one raw-text argument, so the input combines
    the available title, description, and tags.
    """
    raw_text = " ".join(
        part
        for part in (
            str(title or ""),
            str(description or ""),
            " ".join(parse_tags(tags)),
        )
        if part.strip()
    )

    return map_to_canonical_theme(raw_text)


def jaccard_similarity(tags_a, tags_b):
    a = set(tags_a or [])
    b = set(tags_b or [])

    if not a and not b:
        return 0.0

    union = a | b

    if not union:
        return 0.0

    return float(len(a & b)) / float(len(union))


def duration_similarity(duration_a, duration_b):
    """
    Return duration similarity in [0, 1].

    Missing, negative, or invalid durations yield 0.
    """
    try:
        a = float(duration_a)
        b = float(duration_b)

        if not math.isfinite(a) or not math.isfinite(b):
            return 0.0

        if a < 0 or b < 0:
            return 0.0

        return float(math.exp(-abs(math.log((a + 1.0) / (b + 1.0)))))

    except (TypeError, ValueError, OverflowError):
        return 0.0


def cosine_similarity(vector_a, vector_b):
    """
    Calculate cosine similarity between Spark ML vectors.
    Missing or zero vectors yield 0.
    """
    if vector_a is None or vector_b is None:
        return 0.0

    try:
        norm_a = float(vector_a.norm(2))
        norm_b = float(vector_b.norm(2))

        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0

        result = float(vector_a.dot(vector_b)) / (norm_a * norm_b)

        return max(0.0, min(1.0, result))

    except Exception:
        return 0.0


def has_nonzero_entries(vector):
    """
    MinHashLSH cannot process vectors with no nonzero entries.
    """
    return vector is not None and vector.numNonzeros() > 0


jaccard_udf = udf(
    jaccard_similarity,
    T.DoubleType(),
)

duration_similarity_udf = udf(
    duration_similarity,
    T.DoubleType(),
)

cosine_similarity_udf = udf(
    cosine_similarity,
    T.DoubleType(),
)

parse_tags_udf = udf(
    parse_tags,
    T.ArrayType(T.StringType()),
)

normalize_text_udf = udf(
    normalize_text,
    T.StringType(),
)

canonical_theme_udf = udf(
    canonical_theme_for_record,
    T.StringType(),
)

has_nonzero_entries_udf = udf(
    has_nonzero_entries,
    T.BooleanType(),
)


# ---------------------------------------------------------------------
# Input preparation
# ---------------------------------------------------------------------


def add_missing_column(df, name, data_type, default=None):
    if name not in df.columns:
        return df.withColumn(
            name,
            lit(default).cast(data_type),
        )

    return df


def prepare_platform_data(spark, parquet_path, platform):
    """
    Read prepared Parquet data and retain the latest record per video.

    Required:
        video_id

    Optional:
        collected_at, tags, title, description, duration_seconds
    """
    df = spark.read.parquet(parquet_path)

    if "video_id" not in df.columns:
        raise ValueError(
            f"{platform}: Parquet input has no required " f"'video_id' column: {parquet_path}"
        )

    df = add_missing_column(df, "collected_at", T.TimestampType())
    df = add_missing_column(df, "tags", T.StringType())
    df = add_missing_column(df, "title", T.StringType())
    df = add_missing_column(df, "description", T.StringType())
    df = add_missing_column(df, "duration_seconds", T.DoubleType())

    df = df.select(
        col("video_id").cast("string").alias("video_id"),
        col("collected_at").cast("timestamp").alias("collected_at"),
        col("tags"),
        col("title").cast("string").alias("title"),
        col("description").cast("string").alias("description"),
        col("duration_seconds").cast("double").alias("duration_seconds"),
    )

    df = df.filter(col("video_id").isNotNull() & (F.trim(col("video_id")) != ""))

    df = df.withColumn("platform", lit(platform))

    df = df.withColumn(
        "tags_array",
        parse_tags_udf(col("tags")),
    )

    # YouTube uses title + description.
    # TikTok uses description only.
    if platform == "YouTube":
        text_expr = F.concat_ws(
            " ",
            F.coalesce(col("title"), lit("")),
            F.coalesce(col("description"), lit("")),
        )
    else:
        text_expr = F.coalesce(
            col("description"),
            lit(""),
        )

    df = df.withColumn("raw_text", text_expr)

    df = df.withColumn(
        "normalized_text",
        normalize_text_udf(col("raw_text")),
    )

    # Theme classification uses the available title, description,
    # and tags through the existing one-argument mapper.
    df = df.withColumn(
        "canonical_theme",
        canonical_theme_udf(
            col("title"),
            col("description"),
            col("tags"),
        ),
    )

    # When the input contains multiple snapshots per video,
    # retain the most recently collected snapshot.
    snapshot_window = Window.partitionBy("platform", "video_id").orderBy(
        col("collected_at").desc_nulls_last()
    )

    df = (
        df.withColumn(
            "_snapshot_rank",
            row_number().over(snapshot_window),
        )
        .filter(col("_snapshot_rank") == 1)
        .drop("_snapshot_rank")
    )

    # Return 0.05 sample for testing
    # df = df.sample(withReplacement=False, fraction=0.05, seed=42)
    return df


# ---------------------------------------------------------------------
# TF-IDF text features
# ---------------------------------------------------------------------


def add_text_vectors(df):
    """
    Build TF-IDF vectors for one platform.

    If every document is empty, text_vector is NULL and text similarity
    will be zero. Nonempty documents with no useful shared vocabulary
    naturally receive a cosine similarity of zero.
    """
    tokenizer = RegexTokenizer(
        inputCol="normalized_text",
        outputCol="text_tokens",
        pattern=r"\W+",
        toLowercase=True,
        minTokenLength=2,
    )

    tokenized = tokenizer.transform(df)

    nonempty = tokenized.filter(size(col("text_tokens")) > 0)

    if nonempty.limit(1).count() == 0:
        return tokenized.withColumn(
            "text_vector",
            lit(None).cast(VectorUDT()),
        )

    vectorizer = CountVectorizer(
        inputCol="text_tokens",
        outputCol="text_tf",
        minDF=1.0,
        binary=False,
    )

    vectorizer_model = vectorizer.fit(nonempty)
    counted = vectorizer_model.transform(tokenized)

    nonempty_counted = counted.filter(size(col("text_tokens")) > 0)

    idf_model = IDF(
        inputCol="text_tf",
        outputCol="text_vector",
    ).fit(nonempty_counted)

    return idf_model.transform(counted)


# ---------------------------------------------------------------------
# Candidate generation and scoring
# ---------------------------------------------------------------------


def empty_results(spark):
    return spark.createDataFrame(
        [],
        schema=EMPTY_RESULTS_SCHEMA,
    )


def calculate_platform_similarities(
    platform_df,
    lsh_distance_threshold,
    num_hash_tables,
    top_k,
):
    """
    Generate candidate pairs using MinHashLSH on tags, then calculate:
      - exact tag Jaccard similarity,
      - TF-IDF cosine similarity,
      - canonical theme match,
      - duration similarity,
      - final weighted score.

    Candidate pairs are emitted in both directions, allowing every
    video to have its own independently ranked top-K list.
    """
    spark = platform_df.sparkSession

    platform_name = platform_df.select("platform").limit(1).collect()
    platform_name = platform_name[0]["platform"] if platform_name else "unknown"
    platform_cols = platform_df.select(
        "platform",
        "video_id",
        "tags_array",
        "title",
        "description",
        "duration_seconds",
        "canonical_theme",
        "normalized_text",
    )
    from pyspark import StorageLevel

    with log_duration(f"[{platform_name}] TF-IDF feature generation"):
        with_text = add_text_vectors(platform_cols).persist(StorageLevel.DISK_ONLY)
        with_text.count()

    # MinHashLSH requires a nonempty set of tag features.
    tag_candidates = with_text.filter(size(col("tags_array")) > 0)

    if tag_candidates.limit(1).count() == 0:
        print(f"[{platform_name}] No videos with tags.")
        with_text.unpersist()
        return empty_results(spark)

    tag_vectorizer = CountVectorizer(
        inputCol="tags_array",
        outputCol="tag_vector",
        binary=True,
        minDF=2.0,
        maxDF=0.02,
        vocabSize=1 << 20,  # veći od podrazumevanih 262144
    )

    with log_duration(f"[{platform_name}] Tag CountVectorizer fit"):
        tag_vectorizer_model = tag_vectorizer.fit(tag_candidates)

    from pyspark.ml.functions import vector_to_array

    tag_vectors = (
        tag_vectorizer_model.transform(tag_candidates)
        .withColumn("_arr", vector_to_array(col("tag_vector")))
        .filter(F.exists(col("_arr"), lambda x: x != 0.0))
        .drop("_arr")
        .cache()
    )

    # Materialize the filtered dataset and report its size.
    valid_vector_count = tag_vectors.count()

    print(f"[{platform_name}] Videos with nonempty tag arrays: " f"{tag_candidates.count()}")
    print(f"[{platform_name}] Videos with nonzero tag vectors: " f"{valid_vector_count}")

    if valid_vector_count == 0:
        print(f"[{platform_name}] No nonzero tag vectors remain. " "Skipping MinHashLSH.")
        tag_vectors.unpersist()
        with_text.unpersist()
        return empty_results(spark)

    # Verify that the actual dataset passed to LSH has no zero vectors.
    zero_vector_count = tag_vectors.filter(~has_nonzero_entries_udf(col("tag_vector"))).count()

    print(f"[{platform_name}] Zero vectors passed to LSH: {zero_vector_count}")

    if zero_vector_count > 0:
        tag_vectors.unpersist()
        raise ValueError(
            f"{platform_name}: Found {zero_vector_count} zero vectors " "after filtering."
        )

    lsh = MinHashLSH(
        inputCol="tag_vector",
        outputCol="tag_hashes",
        numHashTables=num_hash_tables,
    )

    lsh_input = tag_vectors.select("video_id", "tag_vector")

    with log_duration(f"[{platform_name}] MinHashLSH fit"):
        lsh_model = lsh.fit(lsh_input)

    with log_duration(f"[{platform_name}] Candidate generation"):
        joined = lsh_model.approxSimilarityJoin(
            lsh_input, lsh_input, lsh_distance_threshold, distCol="lsh_distance"
        )

        pairs = (
            joined.select(
                col("datasetA.video_id").alias("video_id"),
                col("datasetB.video_id").alias("similar_video_id"),
            )
            .filter(col("video_id") < col("similar_video_id"))
            .withColumn("platform", lit(platform_name))
            .persist(StorageLevel.DISK_ONLY)
        )

        pair_count = pairs.count()

    print(f"[{platform_name}] Candidate pairs: {pair_count}", flush=True)
    pair_count = pairs.count()
    print(f"[{platform_name}] Candidate pairs: {pair_count}")
    tag_vectors.unpersist()

    # Look up full tag, text, theme, and duration data for each pair.
    # `with_text` includes all videos, including those without tags.
    left = with_text.select(
        col("video_id").alias("left_id"),
        col("tags_array").alias("left_tags"),
        col("text_vector").alias("left_text_vector"),
        col("canonical_theme").alias("left_theme"),
        col("duration_seconds").alias("left_duration"),
    )

    right = with_text.select(
        col("video_id").alias("right_id"),
        col("tags_array").alias("right_tags"),
        col("text_vector").alias("right_text_vector"),
        col("canonical_theme").alias("right_theme"),
        col("duration_seconds").alias("right_duration"),
    )

    scored = (
        pairs.join(
            left,
            pairs.video_id == left.left_id,
            "inner",
        )
        .join(
            right,
            pairs.similar_video_id == right.right_id,
            "inner",
        )
        .withColumn(
            "tag_similarity",
            jaccard_udf(
                col("left_tags"),
                col("right_tags"),
            ),
        )
        .withColumn(
            "text_similarity",
            cosine_similarity_udf(
                col("left_text_vector"),
                col("right_text_vector"),
            ),
        )
        .withColumn(
            "same_theme",
            when(
                col("left_theme").isNotNull()
                & col("right_theme").isNotNull()
                & (col("left_theme") == col("right_theme")),
                lit(True),
            ).otherwise(lit(False)),
        )
        .withColumn(
            "duration_similarity",
            duration_similarity_udf(
                col("left_duration"),
                col("right_duration"),
            ),
        )
    )

    scored = scored.withColumn(
        "similarity_score",
        (
            lit(TAG_WEIGHT) * col("tag_similarity")
            + lit(TEXT_WEIGHT) * col("text_similarity")
            + lit(THEME_WEIGHT) * col("same_theme").cast("double")
            + lit(DURATION_WEIGHT) * col("duration_similarity")
        ),
    )

    forward = scored.select(
        "platform",
        col("video_id").alias("video_id"),
        col("similar_video_id").alias("similar_video_id"),
        "similarity_score",
        "tag_similarity",
        "text_similarity",
        "same_theme",
        "duration_similarity",
    )

    reverse = scored.select(
        "platform",
        col("similar_video_id").alias("video_id"),
        col("video_id").alias("similar_video_id"),
        "similarity_score",
        "tag_similarity",
        "text_similarity",
        "same_theme",
        "duration_similarity",
    )

    directed = forward.unionByName(reverse).dropDuplicates(
        ["platform", "video_id", "similar_video_id"]
    )

    ranking_window = Window.partitionBy("platform", "video_id").orderBy(
        col("similarity_score").desc(),
        col("similar_video_id").asc(),
    )

    with log_duration(f"[{platform_name}] Scoring and ranking"):
        ranked = (
            directed.withColumn(
                "similarity_rank",
                row_number().over(ranking_window),
            )
            .filter(col("similarity_rank") <= top_k)
            .select(*OUTPUT_COLUMNS)
            .persist(StorageLevel.DISK_ONLY)
        )

        ranked.count()

    pairs.unpersist()
    with_text.unpersist()
    return ranked


# ---------------------------------------------------------------------
# PostgreSQL persistence
# ---------------------------------------------------------------------


def jdbc_properties():
    """
    PostgreSQL credentials are read from environment variables.
    If they are absent, authentication must be configured in the JDBC URL.
    """
    return {
        "user": os.environ.get(
            "ANALYTICS_POSTGRES_USER",
            "admin",
        ),
        "password": os.environ.get(
            "ANALYTICS_POSTGRES_PASSWORD",
            "admin",
        ),
        "driver": "org.postgresql.Driver",
    }


def write_jdbc_table(df, jdbc_url, schema, table):
    """
    Write a staging DataFrame to a PostgreSQL staging table.

    This overwrites only the uniquely named staging table, not the
    authoritative similarity table.
    """
    props = jdbc_properties()

    writer = (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"{schema}.{table}")
        .option("driver", props["driver"])
        .mode("overwrite")
    )

    if props["user"]:
        writer = writer.option("user", props["user"])

    if props["password"]:
        writer = writer.option("password", props["password"])

    writer.save()


def open_jdbc_connection(spark, jdbc_url):
    """
    Open a JDBC connection using Spark's JVM and PostgreSQL JDBC driver.
    """
    jvm = spark.sparkContext._gateway.jvm
    props = jvm.java.util.Properties()

    jdbc_props = jdbc_properties()

    if not jdbc_props["user"] or not jdbc_props["password"]:
        raise ValueError(
            "PostgreSQL username or password is missing. "
            "Check ANALYTICS_POSTGRES_USER and "
            "ANALYTICS_POSTGRES_PASSWORD."
        )

    props.setProperty("user", jdbc_props["user"])
    props.setProperty("password", jdbc_props["password"])

    return jvm.java.sql.DriverManager.getConnection(
        jdbc_url,
        props,
    )


def replace_processed_results(
    spark,
    results_df,
    processed_sources_df,
    jdbc_url,
    schema,
    target_table,
):
    """
    Atomically replace similarity rows for all processed source videos.

    Steps:
      1. Write results and processed IDs to separate staging tables.
      2. Begin a PostgreSQL transaction.
      3. Delete old rows for processed source IDs.
      4. Insert newly calculated rows.
      5. Commit the transaction.

    Processed IDs are tracked independently of result rows. Therefore,
    old relationships are deleted even when a processed video has no
    valid tag vector or no candidate pairs.

    If deletion or insertion fails, the transaction is rolled back.
    """
    run_id = uuid.uuid4().hex[:12]

    results_stage = f"creator_sim_results_{run_id}"
    sources_stage = f"creator_sim_sources_{run_id}"

    target = table_name(schema, target_table)
    results_table = table_name(schema, results_stage)
    sources_table = table_name(schema, sources_stage)

    try:
        with log_duration("PostgreSQL: write results staging"):
            write_jdbc_table(
                results_df.select(*OUTPUT_COLUMNS),
                jdbc_url,
                schema,
                results_stage,
            )

        with log_duration("PostgreSQL: write processed sources staging"):
            write_jdbc_table(
                processed_sources_df.select("platform", "video_id").dropDuplicates(),
                jdbc_url,
                schema,
                sources_stage,
            )

        with log_duration("PostgreSQL: ensure target table"):
            ensure_postgres_table(
                jdbc_url=jdbc_url,
                schema=schema,
                table=target_table,
                template_table=results_stage,
            )

        connection = open_jdbc_connection(spark, jdbc_url)
        connection.setAutoCommit(False)

        try:
            statement = connection.createStatement()

            delete_sql = f"""
                DELETE FROM {target} AS target
                USING {sources_table} AS sources
                WHERE target.platform = sources.platform
                  AND target.video_id = sources.video_id
            """

            statement.executeUpdate(delete_sql)

            insert_sql = f"""
                INSERT INTO {target} (
                    platform,
                    video_id,
                    similar_video_id,
                    similarity_score,
                    similarity_rank,
                    tag_similarity,
                    text_similarity,
                    same_theme,
                    duration_similarity
                )
                SELECT
                    platform,
                    video_id,
                    similar_video_id,
                    similarity_score,
                    similarity_rank,
                    tag_similarity,
                    text_similarity,
                    same_theme,
                    duration_similarity
                FROM {results_table}
            """

            statement.executeUpdate(insert_sql)

            connection.commit()
            statement.close()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

    finally:
        # Staging tables are temporary workflow artifacts.
        # The main table is changed only by the transaction above.
        cleanup_connection = None

        try:
            cleanup_connection = open_jdbc_connection(
                spark,
                jdbc_url,
            )

            cleanup_statement = cleanup_connection.createStatement()

            cleanup_statement.executeUpdate(f"DROP TABLE IF EXISTS {results_table}")

            cleanup_statement.executeUpdate(f"DROP TABLE IF EXISTS {sources_table}")

            cleanup_statement.close()

        except Exception as cleanup_error:
            print(
                "WARNING: Could not clean up staging tables: " f"{cleanup_error}",
                file=sys.stderr,
            )

        finally:
            if cleanup_connection is not None:
                cleanup_connection.close()


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------


def main():
    if len(sys.argv) < 5:
        raise ValueError(
            "Usage: creator_video_comparison.py "
            "<youtube_parquet_path> "
            "<tiktok_parquet_path> "
            "<postgres_jdbc_url> "
            "<postgres_schema> "
            "[target_table] "
            "[lsh_distance_threshold] "
            "[lsh_num_hash_tables] "
            "[top_k]"
        )

    youtube_path = sys.argv[1]
    tiktok_path = sys.argv[2]
    jdbc_url = sys.argv[3]
    postgres_schema = sys.argv[4]

    target_table = sys.argv[5] if len(sys.argv) > 5 else "creator_video_similarity"

    lsh_distance_threshold = (
        float(sys.argv[6]) if len(sys.argv) > 6 else DEFAULT_LSH_DISTANCE_THRESHOLD
    )

    num_hash_tables = int(sys.argv[7]) if len(sys.argv) > 7 else DEFAULT_LSH_NUM_HASH_TABLES

    top_k = int(sys.argv[8]) if len(sys.argv) > 8 else DEFAULT_TOP_K

    if not 0.0 < lsh_distance_threshold <= 1.0:
        raise ValueError("LSH distance threshold must be in (0, 1].")

    if num_hash_tables < 1:
        raise ValueError("Number of LSH hash tables must be at least 1.")

    if top_k < 1:
        raise ValueError("top_k must be at least 1.")

    spark = SparkSession.builder.appName("creator_video_comparison").getOrCreate()

    processed_sources = None
    results = None

    try:
        with log_duration("Prepare YouTube data"):
            youtube_df = prepare_platform_data(spark, youtube_path, "YouTube")

        with log_duration("Prepare TikTok data"):
            tiktok_df = prepare_platform_data(spark, tiktok_path, "TikTok")

        # Keep every input video ID, including videos without tags.
        # These IDs must be included in the transactional replacement
        # so stale rows are removed when no new candidates are found.
        processed_sources = (
            youtube_df.select("platform", "video_id")
            .unionByName(tiktok_df.select("platform", "video_id"))
            .dropDuplicates()
            .cache()
        )

        print("Calculating YouTube video similarities...")

        youtube_results = calculate_platform_similarities(
            youtube_df,
            lsh_distance_threshold,
            num_hash_tables,
            top_k,
        )

        print("Calculating TikTok video similarities...")

        tiktok_results = calculate_platform_similarities(
            tiktok_df,
            lsh_distance_threshold,
            num_hash_tables,
            top_k,
        )

        results = (
            youtube_results.unionByName(tiktok_results)
            .select(*OUTPUT_COLUMNS)
            .dropDuplicates(["platform", "video_id", "similar_video_id"])
            .cache()
        )

        processed_count = processed_sources.count()
        result_count = results.count()

        print(f"Processed source videos: {processed_count}")
        print(f"Generated similarity rows: {result_count}")

        replace_processed_results(
            spark=spark,
            results_df=results,
            processed_sources_df=processed_sources,
            jdbc_url=jdbc_url,
            schema=postgres_schema,
            target_table=target_table,
        )

        print("Successfully replaced similarity results in " f"{postgres_schema}.{target_table}")

    finally:
        for df in (results, youtube_results, tiktok_results, processed_sources):
            if df is not None:
                df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
