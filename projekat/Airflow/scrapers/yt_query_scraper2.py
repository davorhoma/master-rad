import csv
import json
import math
import os
import re
import time

from datetime import datetime, timezone
from dotenv import load_dotenv
from googleapiclient.discovery import HttpError, build

load_dotenv()

# =========================================================
# CONFIG
# =========================================================

DEVELOPER_KEY = os.getenv("YOUTUBE_API_KEY_davor_scraper")

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

SCRAPERS_DIR = os.getenv("SCRAPERS_DIR", "/opt/airflow/scrapers")
OUTPUT_FILE_NAME = os.getenv(
    "YOUTUBE_QUERY_OUTPUT_FILE", "youtube_search_gaming_spojeno.csv"
)
OUTPUT_FILE = (
    OUTPUT_FILE_NAME
    if os.path.isabs(OUTPUT_FILE_NAME)
    else os.path.join(SCRAPERS_DIR, OUTPUT_FILE_NAME)
)
DB_FILE_NAME = os.getenv("YOUTUBE_QUERY_SEEN_FILE", "youtube_seen_search_videos.json")
DB_FILE = (
    DB_FILE_NAME
    if os.path.isabs(DB_FILE_NAME)
    else os.path.join(SCRAPERS_DIR, DB_FILE_NAME)
)

MAX_RESULTS_PER_QUERY = 50

SEARCH_QUERIES = [
    "gaming",
    "gameplay",
    "gaming highlights",
    "funny gaming moments",
    "gaming montage",
    "gaming clips",
    "gaming shorts",
    "gaming livestream",
    "gaming stream",
    "esports",
    "competitive gaming",
    "pro gamer",
    "gaming memes",
    "walkthrough",
    "playthrough",
    "speedrun",
    "boss fight",
    "no commentary gameplay",
    "multiplayer gameplay",
    "co-op gameplay",
    "survival game",
    "horror game",
    "open world game",
    "sandbox game",
    "battle royale",
    "FPS gameplay",
    "RPG gameplay",
    "strategy game",
    "simulator game",
    "indie game",
    "mobile gaming",
    "PC gaming",
    "console gaming",
    "retro gaming",
    "new game release",
    "gaming reaction",
    "rage moments",
    "clutch moments",
    "trickshots",
    "gaming edits",
    "game reviewgaming news",
    "gaming tutorial",
    "tips and tricks",
    "best gaming moments",
    "viral gameplay",
    "top plays gaming",
    "gaming compilation",
    "streamer highlights",
    "twitch highlights",
    "minecraft",
    "fortnite",
    "roblox",
    "gta 5",
    "gta online",
    "call of duty",
    "warzone",
    "valorant",
    "counter strike 2",
    "cs2",
    "cs2 gameplay",
    "league of legends",
    "dota 2",
    "apex legends",
    "overwatch 2",
    "rainbow six siege",
    "rocket league",
    "pubg",
    "free fire",
    "brawl stars",
    "clash royale",
    "clash of clans",
    "genshin impact",
    "fifa gameplay",
    "fc 25",
    "nba 2k",
    "elden ring",
    "dark souls",
    "black myth wukong",
    "cyberpunk 2077",
    "red dead redemption 2",
    "skyrim gameplay",
    "resident evil gameplay",
    "five nights at freddys",
    "phasmophobia",
    "lethal company",
    "among us",  # Odavde nastaviti
    "helldivers 2",
    "monster hunter wilds",
    "mario kart",
    "zelda gameplay",
    "pokemon gameplay",
    "anime games",
    "gaming challenges",
    "gaming fails",
    "best settings gaming",
    "aim traininggaming keyboard",
    "gaming setup",
    "AI gaming",
    "unreal engine 5 gameplay",
]

# =========================================================
# HELPERS
# =========================================================


def build_youtube():
    return build(
        YOUTUBE_API_SERVICE_NAME,
        YOUTUBE_API_VERSION,
        developerKey=DEVELOPER_KEY,
    )


def safe_int(value):
    if value is None:
        return 0

    try:
        return int(value)
    except Exception:
        return 0


def clean_text(text):
    if not text:
        return ""

    text = text.replace("\n", " ")
    text = text.replace("\r", " ")
    text = text.replace("\t", " ")

    text = re.sub(r"\s+", " ", text)

    return text.strip()


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


# =========================================================
# SNAPSHOT DATABASE
# =========================================================


def load_seen():
    if not os.path.exists(DB_FILE):
        return {}

    with open(DB_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return {item["video_id"]: item for item in data}

    return data


def save_seen(db):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2)


def get_last_snapshot(video_db, video_id):
    if video_id not in video_db:
        return None

    if not video_db[video_id]["snapshots"]:
        return None

    return video_db[video_id]["snapshots"][-1]


def should_store_snapshot(old, new):
    if not old:
        return True

    def pct_change(old_v, new_v):
        if old_v == 0:
            return 1.0

        return abs(new_v - old_v) / old_v

    views_change = pct_change(old["views"], new["views"])
    likes_change = pct_change(old["likes"], new["likes"])
    viral_change = abs(new["viral_score"] - old["viral_score"])

    return views_change > 0.05 or likes_change > 0.05 or viral_change > 0.2


def update_video(db, video):
    vid = video["video_id"]

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    snapshot = {
        "date": today,
        "views": video["views"],
        "likes": video["likes"],
        "comments": video["comments"],
        "viral_score": video["viral_score"],
    }

    if vid not in db:
        db[vid] = {
            "video_id": vid,
            "snapshots": [snapshot],
        }
        return True

    last = get_last_snapshot(db, vid)

    if should_store_snapshot(last, video):
        db[vid]["snapshots"].append(snapshot)
        return True

    return False


# =========================================================
# FETCH SEARCH VIDEOS
# =========================================================


def search_videos(youtube, query, max_retries=5):
    print(f"🔎 Searching: {query}")

    for attempt in range(1, max_retries + 1):
        try:
            response = (
                youtube.search()
                .list(
                    part="snippet",
                    q=query,
                    type="video",
                    maxResults=MAX_RESULTS_PER_QUERY,
                    order="viewCount",
                    videoCategoryId="20",
                )
                .execute()
            )

            video_ids = []

            for item in response.get("items", []):
                vid = item["id"].get("videoId")
                if vid:
                    video_ids.append(vid)

            return video_ids

        except HttpError as e:
            print(f"❌ Error on attempt {attempt}/{max_retries}: {e}")

            if attempt < max_retries:
                print("⏳ Sleeping 60 seconds before retry...")
                time.sleep(60)
            else:
                print("❌ Max retries reached, skipping query.")
                return []

        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            print("⏳ Sleeping 60 seconds before retry...")
            time.sleep(60)

    return []


def chunk_list(lst, size=50):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def fetch_video_details(youtube, video_ids):
    if not video_ids:
        return []

    videos = []

    for chunk in chunk_list(video_ids, 50):
        response = (
            youtube.videos()
            .list(
                part="snippet,statistics,contentDetails,status",
                id=",".join(chunk),
            )
            .execute()
        )

        for video in response.get("items", []):
            snippet = video.get("snippet", {})
            stats = video.get("statistics", {})
            content = video.get("contentDetails", {})
            status = video.get("status", {})

            row = {
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "video_id": video.get("id"),
                "title": clean_text(snippet.get("title")),
                "description": clean_text(snippet.get("description", ""))[:500],
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

            videos.append(row)

    return videos


# =========================================================
# CSV
# =========================================================


def save_csv_append(videos, filename=OUTPUT_FILE):
    if not videos:
        print("⚠️ No videos to save.")
        return

    file_exists = os.path.isfile(filename)

    with open(
        filename,
        "a",
        newline="",
        encoding="utf-8-sig",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=videos[0].keys(),
        )

        if not file_exists:
            writer.writeheader()

        writer.writerows(videos)

    print(f"💾 Saved {len(videos)} videos")


# =========================================================
# MAIN
# =========================================================


def main():
    start = time.time()

    try:
        youtube = build_youtube()

        db = load_seen()

        print(f"📚 Already tracked videos: {len(db)}")

        all_video_ids = set()

        # SEARCH
        for query in SEARCH_QUERIES[50:]:
            ids = search_videos(youtube, query)
            print(f"🔍 Found videos for '{query}': {len(ids)}")

            all_video_ids.update(ids)

            time.sleep(5)

        print(f"📦 Unique search videos: {len(all_video_ids)}")

        # DETAILS
        videos = fetch_video_details(
            youtube,
            list(all_video_ids),
        )

        print(f"🎮 Detailed videos fetched: {len(videos)}")

        # SORT
        videos.sort(
            key=lambda x: x["viral_score"],
            reverse=True,
        )

        # SNAPSHOTS
        new_entries = []

        for video in videos:
            updated = update_video(db, video)

            if updated:
                new_entries.append(video)

        print(f"🆕 Updated snapshots: {len(new_entries)}")

        # SAVE
        save_seen(db)

        if new_entries:
            save_csv_append(new_entries)
        else:
            print("⚠️ No significant updates.")

        print(f"⏱ Finished in {time.time() - start:.2f}s")

    except Exception as e:
        print(f"❌ ERROR: {e}")


if __name__ == "__main__":
    main()
