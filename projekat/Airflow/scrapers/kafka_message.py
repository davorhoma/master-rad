import ast
import json
import math
from datetime import datetime, timezone

# Zajednička šema za sve Kafka poruke.
MESSAGE_FIELDS = (
    "platform",
    "source_type",
    "video_id",
    "channel_id",
    "channel_title",
    "title",
    "description",
    "tags",
    "views",
    "likes",
    "comments",
    "shares",
    "duration",
    "published_at",
    "collected_at",
    "video_url",
    "search_query",
    "viral_score",
)

# Kanonsko polje -> mogući nazivi u izvornim CSV fajlovima.
FIELD_ALIASES = {
    "video_id": (
        "id",
        "aweme_id",
    ),
    "channel_id": (
        "channelId",
        "author_id",
        "authorId",
        "secUid",
        "sec_uid",
    ),
    "channel_title": (
        "channelTitle",
        "channel_name",
        "author_name",
        "author",
        "username",
    ),
    "title": (
        "video_title",
        "videoTitle",
    ),
    "description": (
        "desc",
        "text",
    ),
    "tags": (
        "hashtags",
        "challenges",
    ),
    "views": (
        "view_count",
        "play_count",
        "playCount",
    ),
    "likes": (
        "like_count",
        "digg_count",
        "diggCount",
    ),
    "comments": (
        "comment_count",
        "commentCount",
    ),
    "shares": (
        "share_count",
        "shareCount",
    ),
    "duration": ("video_duration",),
    "published_at": (
        "publishedAt",
        "publish_time",
        "create_time",
        "createTime",
    ),
    "collected_at": (
        "trending_date",
        "trending_detected_time",
        "collected_time",
        "collectedTime",
    ),
    "video_url": (
        "url",
        "link",
        "webpage_url",
    ),
    "search_query": (
        "keyword",
        "query",
    ),
    "viral_score": ("vq_score",),
}


INTEGER_FIELDS = (
    "views",
    "likes",
    "comments",
    "shares",
    "duration",
)

FLOAT_FIELDS = ("viral_score",)


def _is_missing(value):
    if value is None:
        return True

    if isinstance(value, str):
        return not value.strip()

    return False


def _get_value(record, canonical_field):
    """Pronađi vrednost koristeći kanonski naziv ili neki od aliasa."""
    aliases = FIELD_ALIASES.get(canonical_field, ())
    candidates = (canonical_field, *aliases)

    # Bez obzira na velika i mala slova, prepoznaj poznate nazive.
    normalized_record = {str(key).strip().lower(): value for key, value in record.items()}

    for candidate in candidates:
        value = normalized_record.get(candidate.lower())

        if not _is_missing(value):
            return value

    return None


def _normalize_integer(value):
    if _is_missing(value):
        return None

    try:
        number = float(str(value).strip())

        if not math.isfinite(number) or not number.is_integer():
            return None

        return int(number)
    except (TypeError, ValueError, OverflowError):
        return None


def _normalize_float(value):
    if _is_missing(value):
        return None

    try:
        number = float(str(value).strip())

        if not math.isfinite(number):
            return None

        return number
    except (TypeError, ValueError, OverflowError):
        return None


def _normalize_tags(value):
    """Tagovi se uvek predstavljaju kao lista stringova."""
    if _is_missing(value):
        return []

    if isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        text = str(value).strip()

        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, TypeError):
            try:
                parsed = ast.literal_eval(text)
            except (ValueError, SyntaxError):
                parsed = None

        if isinstance(parsed, (list, tuple, set)):
            items = list(parsed)
        elif "," in text:
            items = text.split(",")
        elif text.startswith("#") and " " in text:
            items = text.split()
        else:
            items = [text]

    return [str(item).strip().lstrip("#") for item in items if not _is_missing(item)]


def _normalize_timestamp(value):
    """
    Vrati ISO 8601 timestamp u UTC kada je moguće.

    Unix timestampovi u sekundama i milisekundama su podržani.
    Ako format nije prepoznat, sačuvaj originalnu vrednost kako
    ne bi došlo do tihog gubitka podataka.
    """
    if _is_missing(value):
        return None

    text = str(value).strip()

    # Unix timestamp u sekundama ili milisekundama.
    if text.isdigit() and len(text) in (10, 13):
        try:
            timestamp = int(text)
            if len(text) == 13:
                timestamp /= 1000

            return (
                datetime.fromtimestamp(
                    timestamp,
                    tz=timezone.utc,
                )
                .isoformat()
                .replace("+00:00", "Z")
            )
        except (ValueError, OverflowError, OSError):
            return text

    try:
        normalized = datetime.fromisoformat(text.replace("Z", "+00:00"))

        # Vrednosti bez vremenske zone tretiramo kao UTC.
        if normalized.tzinfo is None:
            normalized = normalized.replace(tzinfo=timezone.utc)
        else:
            normalized = normalized.astimezone(timezone.utc)

        return normalized.isoformat().replace("+00:00", "Z")
    except ValueError:
        return text


def normalize_message(record, platform, source_type):
    """
    Konvertuj jedan izvorni CSV red u standardizovanu Kafka poruku.

    platform: 'youtube' ili 'tiktok'
    source_type: 'trending' ili 'search'
    """
    if platform not in {"youtube", "tiktok"}:
        raise ValueError(f"Nepoznata platforma: {platform}")

    if source_type not in {"trending", "search"}:
        raise ValueError(f"Nepoznat source_type: {source_type}")

    message = {
        "platform": platform,
        "source_type": source_type,
    }

    for field in MESSAGE_FIELDS:
        if field in {"platform", "source_type"}:
            continue

        value = _get_value(record, field)

        if field == "tags":
            value = _normalize_tags(value)
        elif field in INTEGER_FIELDS:
            value = _normalize_integer(value)
        elif field in FLOAT_FIELDS:
            value = _normalize_float(value)
        elif field in {"published_at", "collected_at"}:
            value = _normalize_timestamp(value)
        elif isinstance(value, str):
            value = value.strip() or None

        message[field] = value

    # Provera da li je poruka dovoljno kompletna za dalju obradu.
    if not message["video_id"]:
        raise ValueError(f"Nedostaje video_id za {platform}/{source_type}")

    return message
