# apify_scrapper.py

import csv
import math
import os
import time
from datetime import datetime, timezone

from apify_client import ApifyClient
from scrapers.seen_videos import load_seen, save_seen, update_video
from scrapers.save_videos import save_csv_append

from dotenv import load_dotenv

# Load API key
load_dotenv()

now = datetime.now()
epoch = datetime(2026, 1, 1)
total_days = (now - epoch).days

# There are 66 keys
key_index = ((total_days + 10) % 66) + 1

APIFY_TOKEN = os.getenv(f"APIFY_TOKEN_{key_index}")

if not APIFY_TOKEN:
    raise ValueError(f"Ključ APIFY_TOKEN_{key_index} nije pronađen u environment-u!")

# =========================================================
# CONFIG
# =========================================================

SCRAPERS_DIR = os.getenv("SCRAPERS_DIR", "/opt/airflow/scrapers")
CSV_FILE_NAME = os.getenv("TIKTOK_APIFY_OUTPUT_FILE", "tiktok_apify_gaming_videos.csv")
CSV_FILE = (
    CSV_FILE_NAME
    if os.path.isabs(CSV_FILE_NAME)
    else os.path.join(SCRAPERS_DIR, CSV_FILE_NAME)
)
DB_FILE_NAME = os.getenv("TIKTOK_APIFY_SEEN_FILE", "tt_seen_videos.json")
DB_FILE = (
    DB_FILE_NAME
    if os.path.isabs(DB_FILE_NAME)
    else os.path.join(SCRAPERS_DIR, DB_FILE_NAME)
)
ACTOR_ID = "clockworks/tiktok-scraper"
RESULTS_PER_QUERY = 50

# Local Config
# APIFY_TOKEN = os.getenv(f"YOUTUBE_API_KEY_davor_scraper_proton_me")
# CSV_FILE = CSV_FILE_NAME
# DB_FILE = DB_FILE_NAME

print("APIFY_TOKEN: ", APIFY_TOKEN)
print("CSV_FILE: ", CSV_FILE)
print("DB_FILE: ", DB_FILE)

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
    "among us",
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

# Gaming keywords filter
GAMING_KEYWORDS = [
    "game",
    "gaming",
    "fortnite",
    "minecraft",
    "valorant",
    "cs2",
    "counter strike",
    "cod",
    "call of duty",
    "gta",
    "roblox",
    "league of legends",
    "moba",
    "rpg",
    "fps",
    "stream",
    "twitch",
    "esports",
    "videogame",
    "videogames",
    "gamers",
    "dota",
    "fps",
    "stream",
    "twitch",
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
    "among us",
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
# APIFY CLIENT
# =========================================================

client = ApifyClient(APIFY_TOKEN)


# =========================================================
# HELPERS
# =========================================================


def safe_int(value):
    if value is None:
        return 0
    try:
        return int(float(value))
    except Exception:
        return 0


def parse_timestamp(value):
    """
    TikTok createTime često dolazi kao unix timestamp.
    """

    if not value:
        return None

    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()
    except Exception as e:
        print(f"Error parsing timestamp: {e}")
        return str(value)


def is_gaming(video):
    """
    Gaming filter preko:
    - description
    - hashtags
    """

    text = (
        str(video.get("desc", ""))
        + " "
        + str(video.get("hashtags", ""))
        + " "
        + str(video.get("text", ""))
    ).lower()

    return any(keyword in text for keyword in GAMING_KEYWORDS)


def extract_hashtags(text):
    """
    Izvlači hashtag-ove iz desc ako actor ne vrati hashtags polje.
    """

    if not text:
        return []

    words = text.split()

    hashtags = [w.strip() for w in words if w.startswith("#")]

    return hashtags


# =========================================================
# VIRAL SCORE
# =========================================================


def viral_score(video):
    views = max(video["views"], 1)

    likes = video["likes"]
    comments = video["comments"]
    shares = video["shares"]

    like_ratio = likes / views
    comment_ratio = comments / views
    share_ratio = shares / views

    score = (
        math.log10(views) * 0.35
        + like_ratio * 100 * 0.25
        + comment_ratio * 100 * 0.20
        + share_ratio * 100 * 0.20
    )

    return round(score, 4)


# =========================================================
# APIFY FETCH
# =========================================================


def fetch_tiktok_videos():
    input_data = {
        "searchQueries": SEARCH_QUERIES,
        "resultsPerPage": RESULTS_PER_QUERY,
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"],
        },
    }

    print("📡 Fetching TikTok gaming videos...")
    print(f"🚀 Using actor: {ACTOR_ID}")

    run = client.actor(ACTOR_ID).call(run_input=input_data)

    dataset_id = run.default_dataset_id

    items = list(client.dataset(dataset_id).iterate_items())

    print(f"📥 Raw videos fetched: {len(items)}")

    return items


# =========================================================
# NORMALIZATION
# =========================================================


def extract_duration(video):
    value = (
        video.get("videoMeta", {}).get("duration")
        or video.get("video", {}).get("duration")
        or video.get("duration")
    )

    try:
        value = int(value)
        minutes = value // 60
        seconds = value % 60
        return f"{minutes}:{seconds:02d}"
    except:
        return "-1"


def normalize_video(video):
    description = video.get("desc") or video.get("text") or ""

    hashtags = video.get("hashtags") or extract_hashtags(description)

    normalized = {
        # =====================================
        # CHANNEL
        # =====================================
        "channel_name": (
            video.get("authorMeta", {}).get("name")
            or video.get("authorMeta", {}).get("nickName")
            or video.get("author", {}).get("nickname")
            or video.get("authorName")
            or "unknown"
        ),
        # =====================================
        # CONTENT
        # =====================================
        "description": description,
        "hashtags": ", ".join([str(h) for h in hashtags]),
        # =====================================
        # STATS
        # =====================================
        "views": safe_int(video.get("playCount") or video.get("views")),
        "likes": safe_int(video.get("diggCount") or video.get("likes")),
        "comments": safe_int(video.get("commentCount") or video.get("comments")),
        "shares": safe_int(video.get("shareCount") or video.get("shares")),
        "duration": extract_duration(video),
        # =====================================
        # TIME
        # =====================================
        "publish_time": parse_timestamp(
            video.get("createTime") or video.get("create_time")
        ),
        # TikTok ne daje pravi trending timestamp
        # koristimo vreme scrape-a
        "trending_detected_time": datetime.now(timezone.utc).isoformat(),
        # =====================================
        # META
        # =====================================
        "video_id": video.get("id"),
        "video_url": (video.get("webVideoUrl") or video.get("url")),
    }

    normalized["viral_score"] = viral_score(normalized)

    return normalized


# =========================================================
# FILTER + RANK
# =========================================================


def process_videos(raw_videos):
    gaming = [v for v in raw_videos if is_gaming(v)]

    seen_local = set()
    unique = []

    for v in gaming:
        vid = v.get("id")
        if not vid or vid in seen_local:
            continue
        seen_local.add(vid)
        unique.append(v)

    normalized = [normalize_video(v) for v in unique]
    normalized.sort(key=lambda x: x["viral_score"], reverse=True)

    return normalized


# =========================================================
# SAVE CSV
# =========================================================


def save_csv(videos, filename=CSV_FILE):
    if not videos:
        print("❌ No videos to save.")
        return

    keys = videos[0].keys()

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)

        writer.writeheader()
        writer.writerows(videos)

    print(f"💾 Saved CSV: {filename}")
    print(f"📊 Rows: {len(videos)}")


# =========================================================
# MAIN
# =========================================================


def main():
    start = time.time()

    try:
        # 1. fetch
        raw_videos = fetch_tiktok_videos()

        # 2. process + filter gaming
        processed = process_videos(raw_videos)

        # 3. load already seen videos
        db = load_seen(db_file=DB_FILE)
        print(f"📚 Already seen videos: {len(db)}")

        # 4. keep only NEW videos
        new_entries = []
        for video in processed:
            updated = update_video(db, video)
            if updated:
                new_entries.append(video)

        print(f"🆕 Updated/added snapshots: {len(new_entries)}")

        # 5. save new seen state
        save_seen(db, db_file=DB_FILE)

        # 6. save dataset (APPEND MODE)
        if new_entries:
            save_csv_append(new_entries, csv_file=CSV_FILE)
        else:
            print("⚠️ No new videos today.")

        print(f"⏱ Finished in {time.time() - start:.2f}s")

    except Exception as e:
        print(f"❌ ERROR: {e}")


if __name__ == "__main__":
    main()
