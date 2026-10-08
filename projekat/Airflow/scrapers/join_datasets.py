import os
import tempfile

import pandas as pd


TIKTOK_COLUMNS = [
    "channel_title",
    "description",
    "tags",
    "views",
    "likes",
    "comments",
    "shares",
    "duration",
    "published_at",
    "collected_at",
    "video_id",
    "video_url",
    "search_query",
    "viral_score",
]

YOUTUBE_COLUMNS = [
    "collected_at",
    "region_code",
    "video_id",
    "title",
    "description",
    "published_at",
    "channel_id",
    "channel_title",
    "tags",
    "category_id",
    "views",
    "likes",
    "comments",
    "duration",
    "definition",
    "caption",
    "privacy_status",
    "embeddable",
    "public_stats_viewable",
    "video_url",
    "viral_score",
]


def _source_path(filename, base_dir):
    if os.path.isabs(filename):
        return filename
    return os.path.join(base_dir, filename)


def _append_source(
    filename,
    source_type,
    columns,
    base_dir,
    output_path,
    chunk_size,
    aliases=None,
):
    path = _source_path(filename, base_dir)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Ulazni CSV nije pronađen: {path}")

    header = pd.read_csv(path, nrows=0).columns.tolist()
    rename_columns = {
        source_column: target_column
        for source_column, target_column in (aliases or {}).items()
        if target_column not in header and source_column in header
    }
    selected_columns = [
        column
        for column in header
        if column in columns or column in rename_columns
    ]
    if not selected_columns and header:
        selected_columns = [header[0]]

    total_rows = 0
    for chunk in pd.read_csv(
        path,
        usecols=selected_columns,
        dtype=str,
        keep_default_na=False,
        chunksize=chunk_size,
    ):
        chunk = chunk.rename(columns=rename_columns)
        chunk = chunk.reindex(columns=columns[:-1], fill_value="")
        chunk["source_type"] = source_type
        chunk.to_csv(output_path, mode="a", header=False, index=False)
        total_rows += len(chunk)
    return total_rows


def _join_sources(sources, columns, output_path, chunk_size):
    output_dir = os.path.dirname(output_path)
    file_descriptor, temp_path = tempfile.mkstemp(
        prefix=f".{os.path.basename(output_path)}.",
        suffix=".tmp",
        dir=output_dir,
    )
    total_rows = 0
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8", newline="") as output:
            pd.DataFrame(columns=columns).to_csv(output, index=False)

        for filename, source_type, base_dir, aliases in sources:
            total_rows += _append_source(
                filename=filename,
                source_type=source_type,
                columns=columns,
                base_dir=base_dir,
                output_path=temp_path,
                chunk_size=chunk_size,
                aliases=aliases,
            )

        os.replace(temp_path, output_path)
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise

    print(f"Sačuvano {total_rows} redova u {output_path}")


def join_scraped_data(
    tiktok_monitoring_file,
    tiktok_search_file,
    youtube_monitoring_file,
    youtube_search_file,
):
    scrapers_dir = os.getenv("SCRAPERS_DIR", "/opt/airflow/scrapers")
    data_dir = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/files/data")
    chunk_size = int(os.getenv("JOIN_CSV_CHUNK_SIZE", "2000"))
    if chunk_size < 1:
        raise ValueError("JOIN_CSV_CHUNK_SIZE mora biti pozitivan ceo broj")

    tiktok_columns = TIKTOK_COLUMNS + ["source_type"]
    tiktok_sources = [
        (
            "tiktok_enriched.csv",
            "historical",
            data_dir,
            {
                "author_name": "channel_title",
                "desc": "description",
                "challenges": "tags",
                "play_count": "views",
                "digg_count": "likes",
                "comment_count": "comments",
                "share_count": "shares",
                "create_time": "published_at",
                "collected_time": "collected_at",
                "id": "video_id",
                "url": "video_url",
                "keyword": "search_query",
                "vq_score": "viral_score",
            },
        ),
        (
            tiktok_monitoring_file,
            "trending",
            scrapers_dir,
            {
                "channel_name": "channel_title",
                "hashtags": "tags",
                "publish_time": "published_at",
                "trending_detected_time": "collected_at",
            },
        ),
        (
            tiktok_search_file,
            "search",
            scrapers_dir,
            {
                "channel_name": "channel_title",
                "hashtags": "tags",
                "publish_time": "published_at",
                "trending_detected_time": "collected_at",
            },
        ),
    ]

    youtube_columns = YOUTUBE_COLUMNS + ["source_type"]
    youtube_sources = [
        (
            "yt-historical-data.csv",
            "historical",
            data_dir,
            {
                "trending_date": "collected_at",
                "publishedAt": "published_at",
                "channelId": "channel_id",
                "channelTitle": "channel_title",
                "categoryId": "category_id",
                "view_count": "views",
                "comment_count": "comments",
            },
        ),
        (
            youtube_monitoring_file,
            "trending",
            scrapers_dir,
            None,
        ),
        (
            youtube_search_file,
            "search",
            scrapers_dir,
            None,
        ),
    ]

    tiktok_output = os.path.join(data_dir, "tiktok_all_joined.csv")
    youtube_output = os.path.join(data_dir, "youtube_all_joined.csv")
    _join_sources(tiktok_sources, tiktok_columns, tiktok_output, chunk_size)
    _join_sources(youtube_sources, youtube_columns, youtube_output, chunk_size)
    return tiktok_output, youtube_output