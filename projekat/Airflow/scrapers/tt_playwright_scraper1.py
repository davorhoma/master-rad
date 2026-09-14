import csv
import math
import os
import time
from datetime import datetime, timezone

from playwright.sync_api import sync_playwright

from scrapers.seen_videos import load_seen, save_seen, update_video

# =========================================================
# CONFIG
# =========================================================

RESULTS_SCROLLS = 3

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
    "game review",
    "gaming news",
    "gaming tutorial",
    "tips and tricks",
    "best gaming moments",
    "viral gameplay",
    "top plays gaming",
    "gaming compilation",
    "streamer highlights",
    "twitch highlights",
    "minecraft",
]

SCRAPERS_DIR = os.getenv("SCRAPERS_DIR", "/opt/airflow/scrapers")
OUTPUT_FILE_NAME = os.getenv("TIKTOK_PLAYWRIGHT_OUTPUT_FILE", "scraper_playwright.csv")
OUTPUT_FILE = (
    OUTPUT_FILE_NAME
    if os.path.isabs(OUTPUT_FILE_NAME)
    else os.path.join(SCRAPERS_DIR, OUTPUT_FILE_NAME)
)
DB_FILE_NAME = os.getenv("TIKTOK_PLAYWRIGHT_SEEN_FILE", "playwright_seen_videos.json")
DB_FILE = (
    DB_FILE_NAME
    if os.path.isabs(DB_FILE_NAME)
    else os.path.join(SCRAPERS_DIR, DB_FILE_NAME)
)


# =========================================================
# HELPERS
# =========================================================


def safe_int(value):
    if value is None:
        return 0

    try:
        return int(value)
    except Exception:
        return 0


def parse_timestamp(value):
    if not value:
        return None

    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()

    except Exception:
        return str(value)


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
# SCRAPER
# =========================================================

videos = {}
current_query = None


def format_duration(seconds):
    minutes = seconds // 60
    seconds = seconds % 60
    return f"{minutes}:{seconds:02d}"


def normalize_video(aweme):
    stats = aweme.get("stats", {})
    author = aweme.get("author", {})

    hashtags = []

    for tag in aweme.get("textExtra", []):
        hashtag = tag.get("hashtagName")

        if hashtag:
            hashtags.append(hashtag)

    video = {
        "video_id": aweme.get("id"),
        "channel_name": (author.get("nickname") or author.get("uniqueId") or "unknown"),
        "description": aweme.get("desc", ""),
        "hashtags": ", ".join(hashtags),
        "views": safe_int(stats.get("playCount")),
        "likes": safe_int(stats.get("diggCount")),
        "comments": safe_int(stats.get("commentCount")),
        "shares": safe_int(stats.get("shareCount")),
        "duration": format_duration(safe_int(aweme.get("video", {}).get("duration"))),
        "publish_time": parse_timestamp(aweme.get("createTime")),
        "trending_detected_time": datetime.now(timezone.utc).isoformat(),
        "video_url": (
            f"https://www.tiktok.com/@"
            f"{author.get('uniqueId', 'user')}"
            f"/video/{aweme.get('id')}"
        ),
        "search_query": current_query,
    }

    video["viral_score"] = viral_score(video)

    # print(f"Duration: {video['duration']}")

    return video


def handle_response(response):
    global videos

    url = response.url

    # TikTok search API endpoints
    valid = (
        "api/search/general/full" in url
        or "api/post/item_list" in url
        or "api/recommend/item_list" in url
    )

    if not valid:
        return

    try:
        data = response.json()

    except Exception:
        return

    items = data.get("data", [])

    for item in items:
        aweme = item.get("item") or item.get("aweme_info") or item

        if not isinstance(aweme, dict):
            continue

        video_id = aweme.get("id")

        if not video_id:
            continue

        if video_id in videos:
            continue

        try:
            normalized = normalize_video(aweme)

            videos[video_id] = normalized

            print(f"🎮 {normalized['description'][:60]}")

        except Exception as e:
            print(f"Normalize error: {e}")


# =========================================================
# PLAYWRIGHT MAIN
# =========================================================


def scrape():
    global current_query

    with sync_playwright() as p:
        browser = p.chromium.launch(
            # executable_path="/snap/bin/brave",
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )

        context = browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )

        page = context.new_page()

        page.on("response", handle_response)

        def safe_goto(page, url):
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)

            if "Something went wrong" in page.content():
                print("⚠️ Detected error page → retrying refresh...")
                page.reload()
                page.wait_for_timeout(5000)

        def has_slider_challenge(page):
            try:
                if page.locator("text=Drag the slider").count() > 0:
                    print("has slider challenge")
                    return True
            except:
                return False

        def handle_security_if_needed(page):
            if has_slider_challenge(page):
                print("🚨 Slider challenge detected → trying popup close")
                # page.wait_for_timeout(3000)

                close_popups(page)

                # dodatni refresh fallback
                page.wait_for_timeout(2000)
                # page.reload()
                # page.wait_for_timeout(4000)

        def close_popups(page):
            selectors = [
                'button[aria-label="Close"]',
                'button[aria-label="close"]',
                'div[role="button"] svg',
                ".tiktok-modal__close",
                '[data-e2e="modal-close-inner-button"]',
                '[data-e2e="close"]',
            ]

            for sel in selectors:
                try:
                    page.locator(sel).first.click(timeout=2000)
                    print("❌ Closed popup")
                    return
                except:
                    pass

        page.goto("https://www.tiktok.com")
        page.wait_for_timeout(3000)
        # page.mouse.click(1399, 899)
        page.focus("body")

        for query in SEARCH_QUERIES:
            current_query = query

            print(f"\n🔎 SEARCHING: {query}")

            url = f"https://www.tiktok.com/search?q={query}"

            try:
                safe_goto(page, url)
                handle_security_if_needed(page)
                # print("First close popup")
                # close_popups(page)

                # time.sleep(5)

                # Scroll loading
                # fokus na body (bitno!)
                page.mouse.click(1399, 899)
                # page.focus("body")

                for i in range(RESULTS_SCROLLS):
                    handle_security_if_needed(page)
                    # close_popups(page)
                    print(f"⬇️ Scroll {i + 1}/{RESULTS_SCROLLS}")

                    page.mouse.click(1399, 899)
                    # page.focus("body")
                    page.keyboard.press("PageDown")
                    page.wait_for_timeout(2000)
                    time.sleep(2)

            except Exception as e:
                print(f"❌ Query failed: {e}")

            time.sleep(3)

        browser.close()


# =========================================================
# SAVE CSV
# =========================================================


def save_csv_append(videos):
    if not videos:
        print("❌ No videos found.")
        return

    videos.sort(key=lambda x: x["viral_score"], reverse=True)

    keys = videos[0].keys()

    file_exists = os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)

        if not file_exists:
            writer.writeheader()

        writer.writerows(videos)

    print(f"\n💾 Saved: {OUTPUT_FILE}")
    print(f"📊 Total videos: {len(videos)}")


# =========================================================
# MAIN
# =========================================================


def main():
    start = time.time()

    scrape()

    db = load_seen(db_file=DB_FILE)
    print(f"📚 Already seen videos: {len(db)}")

    new_entries = []
    for video in videos.values():
        updated = update_video(db, video)
        if updated:
            new_entries.append(video)

    print(f"🆕 Updated/added snapshots: {len(new_entries)}")

    save_seen(db, db_file=DB_FILE)

    if new_entries:
        save_csv_append(new_entries)
    else:
        print("⚠️ No new videos today.")

    print(f"\n⏱ Finished in {time.time() - start:.2f}s")


if __name__ == "__main__":
    main()
