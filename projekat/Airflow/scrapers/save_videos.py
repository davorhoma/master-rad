import csv
import os

# CSV_FILE = "tiktok_gaming_videos_fixed.csv"

CSV_FIELDS = [
    "channel_name",
    "description",
    "hashtags",
    "views",
    "likes",
    "comments",
    "shares",
    "duration",
    "publish_time",
    "trending_detected_time",
    "video_id",
    "video_url",
    "viral_score",
]


def save_csv_append(videos, csv_file):
    if not videos:
        return

    file_exists = os.path.exists(csv_file)

    with open(csv_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)

        # write header samo prvi put
        if not file_exists:
            writer.writeheader()

        writer.writerows(videos)
