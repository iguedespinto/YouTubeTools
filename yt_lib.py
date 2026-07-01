"""Pure, dependency-free helpers for YouTubeTools.

These functions have no side effects and no Mongo/Flask/Google dependencies, so
they can be imported and unit-tested in isolation (importing app.py connects to
Mongo and pulls in googleapiclient, which is slow and needs a live database).
"""

import re
from datetime import timedelta

# A YouTube channel ID is "UC" + 22 URL-safe base64 chars.
CHANNEL_ID_RE = re.compile(r"^UC[0-9A-Za-z_-]{22}$")

# ISO 8601 durations as returned by videos.list contentDetails.duration.
# YouTube never emits year/month components, so only D/H/M/S are handled.
_ISO_DURATION_RE = re.compile(r"^P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$")

# An opaque playlist/video id: long-ish run of URL-safe chars, no spaces.
OPAQUE_ID_RE = re.compile(r"^[0-9A-Za-z_-]{10,}$")


def parse_iso8601_duration(duration):
    """Convert an ISO 8601 duration (e.g. 'PT1M30S') to whole seconds.

    Returns 0 for missing/unparseable input, or for durations with no time
    component (e.g. 'P0D', which YouTube uses for live/premiere placeholders).
    """
    if not duration or not isinstance(duration, str):
        return 0
    match = _ISO_DURATION_RE.match(duration)
    if not match:
        return 0
    days, hours, minutes, seconds = (int(g) if g else 0 for g in match.groups())
    return ((days * 24 + hours) * 60 + minutes) * 60 + seconds


def extract_channel_ref(channel):
    """Normalize a channel input into a ('id'|'handle'|'username', value) tuple.

    Accepts a raw channel ID (UC...), an @handle, a bare handle, or a YouTube
    URL (/channel/UC..., /@handle, or a legacy /user/NAME or /c/NAME path).
    Returns None for empty input. Bare and /c/ names are treated as handles,
    which the Data API resolves via forHandle.
    """
    ref = (channel or "").strip()
    if not ref:
        return None
    if ref.startswith("http") or "youtube.com" in ref:
        m = re.search(r"/channel/(UC[0-9A-Za-z_-]{22})", ref)
        if m:
            return ("id", m.group(1))
        m = re.search(r"/@([^/?#]+)", ref)
        if m:
            return ("handle", "@" + m.group(1))
        m = re.search(r"/user/([^/?#]+)", ref)
        if m:
            return ("username", m.group(1))
        m = re.search(r"/c/([^/?#]+)", ref)
        if m:
            return ("handle", "@" + m.group(1))
        return None
    if CHANNEL_ID_RE.match(ref):
        return ("id", ref)
    if ref.startswith("@"):
        return ("handle", ref)
    return ("handle", "@" + ref)


def parse_rfc3339(value):
    """Parse an RFC3339/ISO timestamp ('2024-01-02T03:04:05Z') to aware datetime.

    Returns None on missing/invalid input.
    """
    if not value or not isinstance(value, str):
        return None
    try:
        from datetime import datetime
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def published_after_from(now, since_hours):
    """Return the cutoff datetime `since_hours` before `now` (both tz-aware)."""
    return now - timedelta(hours=since_hours)


def filter_candidates(candidates, details, exclusion, min_duration_seconds):
    """Partition (video_id, channel) candidates into add / skip buckets. Pure.

    Args:
        candidates: ordered list of (video_id, channel) tuples.
        details: {video_id: {"durationSeconds": int, ...}} from videos.list.
        exclusion: set of video_ids already in the target or an excluded playlist.
        min_duration_seconds: drop videos shorter than this.

    Returns (to_add, stats):
        to_add: ordered list of (video_id, channel) that should be inserted,
                de-duplicated within the run (first channel wins).
        stats:  {channel: {"skippedPresent", "skippedShort", "toAdd"}}.
    """
    to_add = []
    seen = set()
    stats = {}
    for video_id, channel in candidates:
        bucket = stats.setdefault(
            channel, {"skippedPresent": 0, "skippedShort": 0, "toAdd": 0}
        )
        if not video_id or video_id in exclusion or video_id in seen:
            bucket["skippedPresent"] += 1
            continue
        duration = (details.get(video_id) or {}).get("durationSeconds", 0)
        if duration < min_duration_seconds:
            bucket["skippedShort"] += 1
            continue
        seen.add(video_id)
        bucket["toAdd"] += 1
        to_add.append((video_id, channel))
    return to_add, stats
