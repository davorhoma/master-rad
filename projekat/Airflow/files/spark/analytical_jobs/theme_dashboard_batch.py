import logging
import os
import sys
import uuid
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import ArrayType
from pyspark.sql.window import Window

LOGGER = logging.getLogger(__name__)
MEDIAN_ACCURACY = 10000
RANKING_METRIC = "median_viral_score"
OTHER_THEME = "other gaming"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from postgres_sink import write_postgres


def map_to_canonical_theme(raw_text):
    if raw_text is None:
        return OTHER_THEME

    text = str(raw_text).lower().strip()
    specific_games = (
        (("roblox",), "roblox"),
        (("minecraft",), "minecraft"),
        (("cs2", "counter-strike", "counter strike"), "counter-strike 2"),
        (("valorant",), "valorant"),
        (("fortnite",), "fortnite"),
        (("gta", "grand theft auto"), "gta"),
        (("league of legends",), "league of legends"),
        (("dota",), "dota 2"),
        (("apex",), "apex legends"),
        (("overwatch",), "overwatch 2"),
        (("call of duty", "warzone", "cod"), "call of duty"),
        (("pubg",), "pubg"),
        (("free fire",), "free fire"),
        (("brawl stars",), "brawl stars"),
        (("clash royale",), "clash royale"),
        (("clash of clans",), "clash of clans"),
        (("genshin",), "genshin impact"),
        (("fifa", "fc 25", "ea sports fc"), "sports / fifa"),
        (("nba 2k",), "sports / nba 2k"),
        (("rocket league",), "rocket league"),
        (("rainbow six", "r6"), "rainbow six siege"),
        (("elden ring",), "elden ring"),
        (("dark souls",), "dark souls"),
        (("wukong", "black myth"), "black myth wukong"),
        (("cyberpunk",), "cyberpunk 2077"),
        (("red dead", "rdr2"), "red dead redemption 2"),
        (("skyrim", "elder scrolls"), "skyrim"),
        (("resident evil",), "resident evil"),
        (("fnaf", "five nights"), "five nights at freddys"),
        (("phasmophobia",), "phasmophobia"),
        (("lethal company",), "lethal company"),
        (("among us",), "among us"),
        (("helldivers",), "helldivers 2"),
        (("monster hunter",), "monster hunter wilds"),
        (("mario kart",), "mario kart"),
        (("zelda",), "zelda"),
        (("pokemon",), "pokemon"),
    )
    for needles, theme in specific_games:
        if (theme == "league of legends" and text == "lol") or any(
            needle in text for needle in needles
        ):
            return theme

    categories = (
        (("esports", "e-sports", "competitive", "pro gamer"), "esports"),
        (("survival",), "survival"),
        (("horror", "scp"), "horror"),
        (("battle royale",), "battle royale"),
        (("rpg", "roleplay", "role-play"), "rpg"),
        (("strategy", "tactic"), "strategy game"),
        (("simulator", "simulation"), "simulator game"),
        (("indie",), "indie game"),
        (("open world", "sandbox"), "open world / sandbox"),
        (("anime",), "anime games"),
        (("retro",), "retro gaming"),
        (("mobile",), "mobile gaming"),
        (("vr", "virtual reality"), "vr gaming"),
    )
    for needles, theme in categories:
        if any(needle in text for needle in needles):
            return theme

    content_types = (
        (("walkthrough", "playthrough", "no commentary"), "walkthrough / playthrough"),
        (("speedrun",), "speedrun"),
        (("boss fight",), "boss fight"),
        (("tutorial", "tips", "tricks", "guide", "settings"), "tutorial / guides"),
        (
            ("highlight", "montage", "clip", "short", "top plays", "clutch", "trickshot"),
            "highlights & clips",
        ),
        (("funny", "meme", "fail", "rage", "reaction"), "funny moments & memes"),
        (("stream", "twitch", "livestream"), "livestream / vod"),
        (("review", "news"), "gaming news & reviews"),
        (("challenge",), "gaming challenges"),
        (("co-op", "multiplayer"), "multiplayer / co-op"),
    )
    for needles, theme in content_types:
        if any(needle in text for needle in needles):
            return theme

    if any(needle in text for needle in ("setup", "keyboard", "mouse", "hardware", "aim training")):
        return "hardware & setup"
    if "ai gaming" in text or "unreal engine" in text:
        return "tech & ai gaming"
    if any(needle in text for needle in ("fps", "firstperson", "first-person", "shooter")):
        return "fps"
    return OTHER_THEME


def _existing_column(df: DataFrame, names: Iterable[str]):
    return next((name for name in names if name in df.columns), None)


def _safe_cast(df: DataFrame, names: Iterable[str], data_type: str):
    name = _existing_column(df, names)
    if name is None:
        return F.lit(None).cast(data_type)
    return F.expr(f"try_cast(`{name}` as {data_type})")


def _normalized_tags(df: DataFrame):
    name = _existing_column(df, ("tags", "tag_array", "challenges"))
    if name is None:
        return F.array().cast("array<string>")

    field_type = df.schema[name].dataType
    if isinstance(field_type, ArrayType):
        raw_tags = F.col(name)
    else:
        raw_text = F.regexp_replace(F.trim(F.col(name).cast("string")), r"[\[\]\"']", "")
        raw_tags = F.split(raw_text, r"[,|]")

    return raw_tags


def _prepare_tags(df: DataFrame, raw_tags):
    return (
        df.withColumn("_raw_tags", raw_tags)
        .withColumn(
            "tags",
            F.expr(
                "array_distinct(filter(transform(_raw_tags, tag -> lower(trim(tag))), "
                "tag -> tag <> '' and tag not in ('none', 'null', 'nan')))"
            ),
        )
        .drop("_raw_tags")
    )


def normalize_platform_frame(df: DataFrame, platform: str) -> DataFrame:
    video_id = _existing_column(df, ("video_id", "id"))
    collected_at = _existing_column(df, ("collected_at", "collected_time"))
    if video_id is None or collected_at is None:
        raise ValueError(f"{platform} Parquet needs video_id/id and collected_at")

    collected_text = F.trim(F.col(collected_at).cast("string"))
    epoch_seconds = collected_text.rlike(r"^\d{10}(\.\d+)?$")
    epoch_millis = collected_text.rlike(r"^\d{13}$")
    collected_date = (
        F.when(
            epoch_seconds,
            F.to_date(F.from_unixtime(F.expr(f"try_cast(`{collected_at}` as double)"))),
        )
        .when(
            epoch_millis,
            F.to_date(F.from_unixtime(F.expr(f"try_cast(`{collected_at}` as double) / 1000"))),
        )
        .otherwise(F.to_date(F.try_to_timestamp(collected_text)))
    )

    tags = _normalized_tags(df)
    normalized = (
        df.withColumn("video_id", F.trim(F.col(video_id).cast("string")))
        .withColumn("collected_at", collected_date)
        .withColumn("views", _safe_cast(df, ("views", "view_count", "play_count"), "bigint"))
        .withColumn("likes", _safe_cast(df, ("likes", "digg_count"), "bigint"))
        .withColumn("comments", _safe_cast(df, ("comments", "comment_count"), "bigint"))
        .withColumn("viral_score", _safe_cast(df, ("viral_score",), "double"))
        .withColumn(
            "engagement_rate",
            _safe_cast(df, ("engagement_rate",), "double"),
        )
        .withColumn(
            "source_type",
            (
                F.lower(F.trim(F.col("source_type").cast("string")))
                if "source_type" in df.columns
                else F.lit(None).cast("string")
            ),
        )
    )
    normalized = _prepare_tags(normalized, tags)
    normalized = (
        normalized.withColumn("views", F.when(F.col("views") >= 0, F.col("views")))
        .withColumn("likes", F.when(F.col("likes") >= 0, F.col("likes")))
        .withColumn("comments", F.when(F.col("comments") >= 0, F.col("comments")))
    )

    title_column = _existing_column(normalized, ("title", "description", "desc"))
    description_column = _existing_column(normalized, ("description", "desc"))
    tag_text = F.concat_ws(" ", F.col("tags"))
    fallback_text = F.concat_ws(
        " ",
        *[
            F.coalesce(F.col(name).cast("string"), F.lit(""))
            for name in (title_column, description_column)
            if name is not None
        ],
    )
    theme_text = F.when(F.size("tags") > 0, tag_text).otherwise(fallback_text)
    normalized = normalized.withColumn("_theme_text", theme_text)

    score = F.col("viral_score")
    engagement = F.col("engagement_rate")
    normalized = normalized.withColumn(
        "viral_score",
        F.when(
            score.isNotNull()
            & (score >= 0)
            & (~F.isnan(score))
            & (F.abs(score) < F.lit(float("inf"))),
            score,
        ),
    ).withColumn(
        "engagement_rate",
        F.when(
            engagement.isNotNull()
            & (engagement >= 0)
            & (~F.isnan(engagement))
            & (F.abs(engagement) < F.lit(float("inf"))),
            engagement,
        ).otherwise(
            F.when(
                (F.col("views") > 0)
                & F.col("likes").isNotNull()
                & F.col("comments").isNotNull()
                & (F.col("likes") >= 0)
                & (F.col("comments") >= 0),
                (F.col("likes") + F.col("comments")) / F.col("views"),
            )
        ),
    )

    normalized = normalized.filter(
        F.col("video_id").isNotNull()
        & (F.col("video_id") != "")
        & F.col("collected_at").isNotNull()
    ).withColumn("platform", F.lit(platform))

    return normalized.select(
        "platform",
        "video_id",
        "collected_at",
        "tags",
        "_theme_text",
        "views",
        "viral_score",
        "engagement_rate",
        "source_type",
        "likes",
        "comments",
    )


def deduplicate_observations(observations: DataFrame) -> DataFrame:
    scored = observations.withColumn(
        "_completeness",
        sum(
            F.when(F.col(name).isNotNull(), 1).otherwise(0)
            for name in ("views", "viral_score", "engagement_rate", "likes", "comments")
        ),
    ).withColumn("_tag_key", F.concat_ws("\u001f", F.array_sort("tags")))

    duplicate_window = Window.partitionBy("platform", "video_id", "collected_at").orderBy(
        F.col("_completeness").desc(),
        F.col("views").desc_nulls_last(),
        F.col("viral_score").desc_nulls_last(),
        F.col("engagement_rate").desc_nulls_last(),
        F.col("source_type").asc_nulls_last(),
        F.col("_tag_key").asc(),
        F.col("_theme_text").asc(),
    )
    return (
        scored.withColumn("_row_number", F.row_number().over(duplicate_window))
        .filter(F.col("_row_number") == 1)
        .drop("_completeness", "_tag_key", "_row_number")
        .withColumn("theme", F.udf(map_to_canonical_theme, "string")("_theme_text"))
        .filter(F.col("theme") != OTHER_THEME)
    )


def _aggregate_metrics(df: DataFrame, group_columns: list[str]) -> DataFrame:
    return df.groupBy(*group_columns).agg(
        F.countDistinct(F.struct("_identity_platform", "video_id"))
        .cast("long")
        .alias("video_count"),
        F.sum("views").cast("long").alias("sum_views"),
        F.avg("views").alias("avg_views"),
        F.percentile_approx("views", 0.5, MEDIAN_ACCURACY).alias("median_views"),
        F.avg("viral_score").alias("avg_viral_score"),
        F.percentile_approx("viral_score", 0.5, MEDIAN_ACCURACY).alias("median_viral_score"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.percentile_approx("engagement_rate", 0.5, MEDIAN_ACCURACY).alias(
            "median_engagement_rate"
        ),
    )


def build_analytics(observations: DataFrame, minimum_videos: int):
    identity = observations.withColumn("_identity_platform", F.col("platform"))
    all_platforms = identity.withColumn("platform", F.lit("All"))
    rollups = identity.unionByName(all_platforms)

    theme_daily = _aggregate_metrics(rollups, ["platform", "theme", "collected_at"])
    tag_rows = rollups.withColumn("tag", F.explode("tags")).filter(
        (F.col("tag").isNotNull()) & (F.col("tag") != "")
    )
    theme_tag_daily = _aggregate_metrics(tag_rows, ["platform", "theme", "tag", "collected_at"])

    ranking_source = identity.withColumn("platform", F.col("_identity_platform"))
    global_ranking_source = identity.withColumn("platform", F.lit("All"))
    ranking_source = ranking_source.unionByName(global_ranking_source)
    ranking_values = (
        ranking_source.groupBy("platform", "theme")
        .agg(
            F.countDistinct(F.struct("_identity_platform", "video_id"))
            .cast("long")
            .alias("video_count"),
            F.percentile_approx("viral_score", 0.5, MEDIAN_ACCURACY).alias("median_viral_score"),
        )
        .filter((F.col("video_count") >= minimum_videos) & F.col("median_viral_score").isNotNull())
    )

    rank_window = Window.partitionBy("platform").orderBy(
        F.col("median_viral_score").desc_nulls_last(),
        F.col("video_count").desc(),
        F.col("theme").asc(),
    )
    rankings = (
        ranking_values.withColumn("rank", F.row_number().over(rank_window))
        .filter(F.col("rank") <= 10)
        .withColumn("ranking_metric", F.lit(RANKING_METRIC))
        .withColumn("ranking_period", F.lit("all_available_history"))
        .select(
            "platform",
            "theme",
            "ranking_period",
            "ranking_metric",
            "video_count",
            "median_viral_score",
            "rank",
        )
    )
    return theme_daily, theme_tag_daily, rankings


def _stage_table(df: DataFrame, jdbc_url: str, schema: str, stage: str):
    write_postgres(df, jdbc_url, schema, stage, preserve_existing_indexes=False)


def _publish_tables(schema: str, stage_targets: list[tuple[str, str]]):
    import psycopg2
    from psycopg2 import sql

    connection = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    index_columns = (
        ("platform", "theme", "collected_at"),
        ("platform", "theme", "tag", "collected_at"),
        ("platform", "ranking_period", "ranking_metric", "rank"),
    )
    try:
        with connection:
            with connection.cursor() as cursor:
                for (stage, target), columns in zip(stage_targets, index_columns):
                    cursor.execute(
                        sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                            sql.Identifier(schema), sql.Identifier(target)
                        )
                    )
                    cursor.execute(
                        sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                            sql.Identifier(schema),
                            sql.Identifier(stage),
                            sql.Identifier(target),
                        )
                    )
                    index_name = f"{target[:40]}_natural_unique_idx"
                    cursor.execute(
                        sql.SQL("CREATE UNIQUE INDEX {} ON {}.{} ({})").format(
                            sql.Identifier(index_name),
                            sql.Identifier(schema),
                            sql.Identifier(target),
                            sql.SQL(", ").join(map(sql.Identifier, columns)),
                        )
                    )
    finally:
        connection.close()


def run(
    youtube_path: str,
    tiktok_path: str,
    jdbc_url: str,
    schema: str,
    theme_collection: str,
    tag_collection: str,
    ranking_collection: str,
    minimum_videos: int,
):
    if minimum_videos < 1:
        raise ValueError("minimum_videos must be at least 1")

    spark = SparkSession.builder.appName("theme_dashboard_batch").getOrCreate()
    observations = None
    try:
        spark.sparkContext.addPyFile(__file__)
        youtube = normalize_platform_frame(spark.read.parquet(youtube_path), "YouTube")
        tiktok = normalize_platform_frame(spark.read.parquet(tiktok_path), "TikTok")
        observations = deduplicate_observations(youtube.unionByName(tiktok)).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        if observations.count() == 0:
            raise ValueError("No valid, classified video observations were found")
        theme_daily, theme_tag_daily, rankings = build_analytics(observations, minimum_videos)

        for name, frame in (
            ("theme_daily_metrics", theme_daily),
            ("theme_tag_daily_metrics", theme_tag_daily),
            ("theme_rankings", rankings),
        ):
            LOGGER.info("Built analytics dataset %s with columns %s", name, frame.columns)

        run_token = uuid.uuid4().hex
        outputs = (
            (theme_daily, theme_collection),
            (theme_tag_daily, tag_collection),
            (rankings, ranking_collection),
        )
        target_names = [target for _, target in outputs]
        if len(set(target_names)) != len(target_names) or any(
            not name.strip() for name in target_names
        ):
            raise ValueError("Analytics output collection names must be distinct and non-empty")
        stages = []
        try:
            for frame, target in outputs:
                stage = f"{target[:40]}__stage_{run_token[:12]}"
                stages.append((stage, target))
                _stage_table(frame, jdbc_url, schema, stage)
            _publish_tables(schema, stages)
        except Exception:
            import psycopg2
            from psycopg2 import sql

            with psycopg2.connect(
                host=os.environ.get("POSTGRES_HOST", "postgres"),
                port=os.environ.get("POSTGRES_PORT", "5432"),
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
            ) as cleanup_connection:
                with cleanup_connection.cursor() as cursor:
                    for stage, _ in stages:
                        cursor.execute(
                            sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                                sql.Identifier(schema), sql.Identifier(stage)
                            )
                        )
            raise
    finally:
        if observations is not None:
            observations.unpersist()
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 9:
        raise SystemExit(
            "Usage: theme_dashboard_batch.py YOUTUBE_PARQUET TIKTOK_PARQUET "
            "POSTGRES_JDBC_URL POSTGRES_SCHEMA THEME_TABLE TAG_TABLE "
            "RANKING_COLLECTION MINIMUM_VIDEOS"
        )
    run(*sys.argv[1:8], int(sys.argv[8]))
