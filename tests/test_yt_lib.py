"""Unit tests for the pure helpers in yt_lib.

These import only yt_lib (no Flask/Mongo/googleapiclient), so they run fast and
need no database.
"""

from datetime import datetime, timezone

import pytest

from yt_lib import (
    extract_channel_ref,
    filter_candidates,
    parse_iso8601_duration,
    parse_rfc3339,
    published_after_from,
)


# --- parse_iso8601_duration -------------------------------------------------

@pytest.mark.parametrize(
    "value,expected",
    [
        ("PT1M30S", 90),
        ("PT2H", 7200),
        ("PT2H3M4S", 7384),
        ("PT58S", 58),      # the "under 1 min" case from the NightGuardian run
        ("PT49S", 49),
        ("PT0S", 0),
        ("P1DT1H", 90000),
        ("P0D", 0),         # live/premiere placeholder — no time part
        ("", 0),
        (None, 0),
        ("garbage", 0),
        (123, 0),           # non-string
    ],
)
def test_parse_iso8601_duration(value, expected):
    assert parse_iso8601_duration(value) == expected


def test_one_minute_threshold():
    # The rule "at least 1 minute" is durationSeconds >= 60.
    assert parse_iso8601_duration("PT59S") < 60
    assert parse_iso8601_duration("PT1M") >= 60


# --- extract_channel_ref ----------------------------------------------------

def test_extract_channel_ref_handles():
    assert extract_channel_ref("@DenysDavydov") == ("handle", "@DenysDavydov")
    assert extract_channel_ref("DenysDavydov") == ("handle", "@DenysDavydov")
    assert extract_channel_ref("  @ATPGeo  ") == ("handle", "@ATPGeo")


def test_extract_channel_ref_id():
    cid = "UC" + "a" * 22
    assert extract_channel_ref(cid) == ("id", cid)


def test_extract_channel_ref_urls():
    cid = "UC" + "b" * 22
    assert extract_channel_ref(f"https://www.youtube.com/channel/{cid}") == ("id", cid)
    assert extract_channel_ref("https://youtube.com/@HouseofEl") == ("handle", "@HouseofEl")
    assert extract_channel_ref("https://www.youtube.com/@Professor-Gerdes/videos") == (
        "handle", "@Professor-Gerdes",
    )
    assert extract_channel_ref("https://www.youtube.com/user/SomeLegacyName") == (
        "username", "SomeLegacyName",
    )


def test_extract_channel_ref_empty():
    assert extract_channel_ref("") is None
    assert extract_channel_ref(None) is None


# --- parse_rfc3339 ----------------------------------------------------------

def test_parse_rfc3339():
    dt = parse_rfc3339("2026-07-01T03:21:00Z")
    assert dt == datetime(2026, 7, 1, 3, 21, 0, tzinfo=timezone.utc)


def test_parse_rfc3339_invalid():
    assert parse_rfc3339("") is None
    assert parse_rfc3339(None) is None
    assert parse_rfc3339("not-a-date") is None


def test_published_after_from():
    now = datetime(2026, 7, 1, 19, 21, 0, tzinfo=timezone.utc)
    cutoff = published_after_from(now, 16)
    assert cutoff == datetime(2026, 7, 1, 3, 21, 0, tzinfo=timezone.utc)


# --- filter_candidates ------------------------------------------------------

def _details(**durations):
    return {vid: {"durationSeconds": secs} for vid, secs in durations.items()}


def test_filter_candidates_excludes_present_and_short():
    candidates = [
        ("v_new", "@a"),      # keep
        ("v_present", "@a"),  # already in a playlist
        ("v_short", "@b"),    # under 60s
        ("v_ok", "@b"),       # keep
    ]
    details = _details(v_new=300, v_present=300, v_short=58, v_ok=120)
    exclusion = {"v_present"}

    to_add, stats = filter_candidates(candidates, details, exclusion, 60)

    assert to_add == [("v_new", "@a"), ("v_ok", "@b")]
    assert stats["@a"] == {"skippedPresent": 1, "skippedShort": 0, "toAdd": 1}
    assert stats["@b"] == {"skippedPresent": 0, "skippedShort": 1, "toAdd": 1}


def test_filter_candidates_dedups_within_run():
    candidates = [("dup", "@a"), ("dup", "@b")]
    details = _details(dup=300)
    to_add, stats = filter_candidates(candidates, details, set(), 0)
    # First occurrence wins; second is counted as already-present.
    assert to_add == [("dup", "@a")]
    assert stats["@a"]["toAdd"] == 1
    assert stats["@b"]["skippedPresent"] == 1


def test_filter_candidates_missing_duration_treated_as_short():
    # A video with no metadata (deleted/private) has duration 0 -> filtered out.
    candidates = [("ghost", "@a")]
    to_add, stats = filter_candidates(candidates, {}, set(), 60)
    assert to_add == []
    assert stats["@a"]["skippedShort"] == 1


def test_filter_candidates_zero_threshold_keeps_all_new():
    candidates = [("a", "@x"), ("b", "@x")]
    details = _details(a=5, b=5)
    to_add, _ = filter_candidates(candidates, details, set(), 0)
    assert [v for v, _ in to_add] == ["a", "b"]
