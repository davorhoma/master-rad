import csv
import math
import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from googleapiclient.discovery import build

load_dotenv()

DEVELOPER_KEY = os.getenv("YOUTUBE_API_KEY_davorhoma")

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

SCRAPERS_DIR = os.getenv("SCRAPERS_DIR", "/opt/airflow/scrapers")
OUTPUT_FILE_NAME = os.getenv(
    "YOUTUBE_MONITORING_OUTPUT_FILE",
    "youtube_gaming_trending_spojeno.csv",
)
OUTPUT_FILE = (
    OUTPUT_FILE_NAME
    if os.path.isabs(OUTPUT_FILE_NAME)
    else os.path.join(SCRAPERS_DIR, OUTPUT_FILE_NAME)
)

REGIONS = [
    "US",
    "BR",
    "CA",
    "DE",
    "FR",
    "GB",
    "IN",
    "JP",
    "KR",
    "MX",
    "RU",
]

MAX_RESULTS = 50

# YouTube Gaming category
GAMING_CATEGORY_ID = "20"


def safe_int(value):
    if value is None:
        return 0

    try:
        return int(value)
    except Exception:
        return 0


def viral_score(video):
    views = max(video["views"], 1)

    likes = video["likes"]
    comments = video["comments"]

    like_ratio = likes / views
    comment_ratio = comments / views

    score = (
        math.log10(views) * 0.50 + like_ratio * 100 * 0.30 + comment_ratio * 100 * 0.20
    )

    return round(score, 4)


def build_youtube():
    return build(
        YOUTUBE_API_SERVICE_NAME,
        YOUTUBE_API_VERSION,
        developerKey=DEVELOPER_KEY,
    )


def fetch_trending_gaming_videos(
    youtube,
    region_code="US",
    max_results=50,
):
    print(f"📡 Fetching gaming trending videos for {region_code}...")

    response = (
        youtube.videos()
        .list(
            part="snippet,statistics,contentDetails,status",
            chart="mostPopular",
            regionCode=region_code,
            maxResults=max_results,
        )
        .execute()
    )

    gaming_videos = []

    for video in response.get("items", []):
        snippet = video.get("snippet", {})

        # Keep only Gaming category
        if snippet.get("categoryId") != GAMING_CATEGORY_ID:
            continue

        stats = video.get("statistics", {})
        content = video.get("contentDetails", {})
        status = video.get("status", {})

        row = {
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "region_code": region_code,
            "video_id": video.get("id"),
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "published_at": snippet.get("publishedAt"),
            "channel_id": snippet.get("channelId"),
            "channel_title": snippet.get("channelTitle"),
            "tags": "|".join(snippet.get("tags", [])),
            "category_id": snippet.get("categoryId"),
            "views": safe_int(stats.get("viewCount")),
            "likes": safe_int(stats.get("likeCount")),
            "comments": safe_int(stats.get("commentCount")),
            "duration": content.get("duration"),
            "definition": content.get("definition"),
            "caption": content.get("caption"),
            "privacy_status": status.get("privacyStatus"),
            "embeddable": status.get("embeddable"),
            "public_stats_viewable": status.get("publicStatsViewable"),
            "video_url": f"https://www.youtube.com/watch?v={video.get('id')}",
        }

        row["viral_score"] = viral_score(row)

        gaming_videos.append(row)

    print(f"🎮 Gaming videos found: {len(gaming_videos)}")

    return gaming_videos


def deduplicate_videos(videos):
    unique = {}

    for video in videos:
        video_id = video["video_id"]

        # Keep highest viral score version
        if (
            video_id not in unique
            or video["viral_score"] > unique[video_id]["viral_score"]
        ):
            unique[video_id] = video

    return list(unique.values())


def save_csv_append(videos, filename=OUTPUT_FILE):
    if not videos:
        print("⚠️ No videos to save.")
        return

    file_exists = os.path.isfile(filename)

    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=videos[0].keys(),
        )

        if not file_exists:
            writer.writeheader()

        writer.writerows(videos)

    print(f"💾 Saved {len(videos)} videos to {filename}")


def main():
    start = time.time()

    try:
        youtube = build_youtube()

        all_videos = []

        for region in REGIONS:
            region_videos = fetch_trending_gaming_videos(
                youtube=youtube,
                region_code=region,
                max_results=MAX_RESULTS,
            )

            all_videos.extend(region_videos)

        print(f"\n📦 Total raw gaming videos: {len(all_videos)}")

        unique_videos = deduplicate_videos(all_videos)

        print(f"🧹 Unique gaming videos: {len(unique_videos)}")

        unique_videos.sort(
            key=lambda x: x["viral_score"],
            reverse=True,
        )

        save_csv_append(unique_videos)

        print(f"\n⏱ Finished in {time.time() - start:.2f}s")

    except Exception as e:
        print(f"❌ ERROR: {e}")


if __name__ == "__main__":
    main()
