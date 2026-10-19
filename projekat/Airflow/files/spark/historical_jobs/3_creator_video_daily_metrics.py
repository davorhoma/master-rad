import os
import sys
from typing import Optional

import psycopg2

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    DoubleType,
    StringType,
    TimestampType,
)
from pyspark.sql.functions import col

from postgres_sink import upsert_postgres

SCHEMA = "public"
TARGET_TABLE = "creator_video_daily_metrics"
STAGING_TABLE = "creator_video_daily_metrics_staging"

TARGET_COLUMNS = [
    "platform",
    "video_id",
    "collected_at",
    "published_at",
    "title",
    "channel_title",
    "views",
    "likes",
    "comments",
    "engagement_rate",
    "viral_score",
]


def get_existing_column(
    frame: DataFrame,
    name: str,
    data_type,
):
    """Return a casted column, or a typed NULL if it is absent."""
    if name in frame.columns:
        return col(name).cast(data_type)

    return F.lit(None).cast(data_type)


def normalize_platform_data(
    frame: DataFrame,
    platform: str,
) -> DataFrame:
    """
    Normalize the prepared YouTube or TikTok dataset.

    viral_score is deliberately preserved rather than recalculated.
    """

    title = get_existing_column(frame, "title", StringType())
    channel_title = get_existing_column(frame, "channel_title", StringType())

    normalized = frame.select(
        F.lit(platform).alias("platform"),
        get_existing_column(frame, "video_id", StringType()).alias("video_id"),
        F.to_date(get_existing_column(frame, "collected_at", TimestampType())).alias(
            "collected_at"
        ),
        F.to_timestamp(get_existing_column(frame, "published_at", TimestampType())).alias(
            "published_at"
        ),
        title.alias("title"),
        channel_title.alias("channel_title"),
        get_existing_column(frame, "views", LongType()).alias("views"),
        get_existing_column(frame, "likes", LongType()).alias("likes"),
        get_existing_column(frame, "comments", LongType()).alias("comments"),
        get_existing_column(frame, "engagement_rate", DoubleType()).alias("engagement_rate"),
        get_existing_column(frame, "viral_score", DoubleType()).alias("viral_score"),
    )

    # Discard rows without the fields required for the daily key.
    normalized = normalized.filter(
        col("video_id").isNotNull()
        & (F.trim(col("video_id")) != "")
        & col("collected_at").isNotNull()
    )

    # Counts should be non-negative. Invalid rows are excluded.
    normalized = normalized.filter(
        (col("views") >= 0) & (col("likes") >= 0) & (col("comments") >= 0)
    )

    # Keep a valid prepared engagement_rate where available.
    # Otherwise calculate it from the existing counts.
    calculated_engagement = F.when(
        col("views") > 0,
        (col("likes") + col("comments")) / col("views"),
    )

    normalized = normalized.withColumn(
        "engagement_rate",
        F.when(
            col("engagement_rate").isNotNull()
            & (~F.isnan(col("engagement_rate")))
            & (col("engagement_rate") >= 0),
            col("engagement_rate"),
        ).otherwise(calculated_engagement),
    )

    # IMPORTANT:
    # viral_score is not recalculated. Its prepared value is retained.
    # If it is NULL in the input, it remains NULL.

    return normalized


def deduplicate_daily_observations(
    frame: DataFrame,
) -> DataFrame:
    """
    Retain one row per (platform, video_id, collected_at).

    When duplicates exist, prefer the row with more populated
    metrics and metadata. Remaining ties are resolved by the
    metric values to make the choice deterministic where possible.
    """

    completeness_score = (
        F.when(col("published_at").isNotNull(), 1).otherwise(0)
        + F.when(col("title").isNotNull(), 1).otherwise(0)
        + F.when(col("channel_title").isNotNull(), 1).otherwise(0)
        + F.when(col("views").isNotNull(), 1).otherwise(0)
        + F.when(col("likes").isNotNull(), 1).otherwise(0)
        + F.when(col("comments").isNotNull(), 1).otherwise(0)
        + F.when(col("engagement_rate").isNotNull(), 1).otherwise(0)
        + F.when(col("viral_score").isNotNull(), 1).otherwise(0)
    )

    ranked = frame.withColumn("_completeness_score", completeness_score)

    window = Window.partitionBy("platform", "video_id", "collected_at").orderBy(
        col("_completeness_score").desc(),
        col("views").desc_nulls_last(),
        col("likes").desc_nulls_last(),
        col("comments").desc_nulls_last(),
    )

    return (
        ranked.withColumn("_row_number", F.row_number().over(window))
        .filter(col("_row_number") == 1)
        .drop("_completeness_score", "_row_number")
    )


def get_postgres_connection():
    """Open a PostgreSQL connection using environment variables."""

    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def main():
    youtube_path = sys.argv[1]
    tiktok_path = sys.argv[2]
    jdbc_url = sys.argv[3]
    pg_schema = sys.argv[4]
    pg_table = sys.argv[5]

    spark = SparkSession.builder.appName("creator_video_daily_metrics").getOrCreate()

    try:
        youtube_raw = spark.read.parquet(youtube_path)
        tiktok_raw = spark.read.parquet(tiktok_path)

        youtube = normalize_platform_data(youtube_raw, "YouTube")
        tiktok = normalize_platform_data(tiktok_raw, "TikTok")

        combined = youtube.unionByName(tiktok)

        daily_metrics = deduplicate_daily_observations(combined)
        daily_metrics = daily_metrics.persist()

        try:
            daily_metrics.count()

            upsert_postgres(
                daily_metrics,
                jdbc_url,
                pg_schema,
                pg_table,
                key_columns=["platform", "video_id", "collected_at"],
                column_types={
                    "platform": "VARCHAR(32)",
                    "video_id": "VARCHAR(255)",
                    "collected_at": "DATE",
                    "published_at": "TIMESTAMP",
                    "title": "TEXT",
                    "channel_title": "TEXT",
                    "views": "BIGINT",
                    "likes": "BIGINT",
                    "comments": "BIGINT",
                    "engagement_rate": "DOUBLE PRECISION",
                    "viral_score": "DOUBLE PRECISION",
                },
            )
        finally:
            daily_metrics.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
