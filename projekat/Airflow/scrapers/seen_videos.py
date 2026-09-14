import json
import os
from datetime import datetime, timezone

# DB_FILE = "seen_videos.json"


def load_seen(db_file):
    if not os.path.exists(db_file):
        return {}

    with open(db_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # CONVERT LIST → DICT (MIGRATION FIX)
    if isinstance(data, list):
        return {item["video_id"]: item for item in data}

    return data


def save_seen(db, db_file):
    with open(db_file, "w", encoding="utf-8") as f:
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
        "viral_score": video["viral_score"],
    }

    if vid not in db:
        db[vid] = {"video_id": vid, "snapshots": [snapshot]}
        return True

    last = get_last_snapshot(db, vid)

    if should_store_snapshot(last, video):
        db[vid]["snapshots"].append(snapshot)
        return True

    return False
