#!/usr/bin/env python3
"""MCP server for YouTubeTools.

A thin FastMCP-based HTTP client that proxies to the Flask app's API endpoints
with Bearer token authentication.

Environment variables:
    YT_BASE_URL: Base URL of the Flask app (e.g., http://localhost:3000)
    YT_API_KEY: API key for Bearer auth (e.g., yt_live_xxx...)
"""

import os
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP

# Configuration
BASE_URL = os.getenv("YT_BASE_URL", "http://localhost:3000")
API_KEY = os.getenv("YT_API_KEY", "")

# Create the MCP server
mcp = FastMCP("youtube-tools")


def _request(
    method: str,
    path: str,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Make an authenticated request to the Flask API."""
    url = f"{BASE_URL.rstrip('/')}{path}"
    headers = {"Authorization": f"Bearer {API_KEY}"}

    with httpx.Client(timeout=60.0) as client:
        if method == "GET":
            response = client.get(url, headers=headers, params=params)
        elif method == "POST":
            response = client.post(url, headers=headers, json=json_body)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        # Try to parse JSON response
        try:
            data = response.json()
        except Exception:
            data = {"error": f"Non-JSON response: {response.text[:500]}"}

        # Add HTTP status for debugging
        if response.status_code >= 400:
            data["_http_status"] = response.status_code

        return data


# ---------------------------------------------------------------------------
# Read Operations (1 quota unit each)
# ---------------------------------------------------------------------------


@mcp.tool()
def list_playlists() -> dict[str, Any]:
    """List all user playlists with metadata.

    Returns a list of playlists, each with:
        id: The YouTube playlist ID.
        title: The playlist name.
        url: Direct link to the playlist (https://www.youtube.com/playlist?list=<id>).
        description: The playlist description.
        itemCount: Number of videos in the playlist.
        thumbnailUrl: URL of the playlist's default thumbnail.
    Quota cost: 1 unit per 50 playlists (cached server-side).
    """
    return _request("GET", "/api/playlists")


@mcp.tool()
def get_playlist_items(
    playlist_id: str,
    page_token: str | None = None,
    details: bool = False,
) -> dict[str, Any]:
    """Fetch videos in a playlist (paginated, 50 per page).

    Args:
        playlist_id: The YouTube playlist ID.
        page_token: Optional token for pagination (from nextPageToken in previous response).
        details: If True, enrich each item with durationSeconds, publishedAt,
                 and channelTitle (costs 1 extra unit per 50 items). Default False.

    Returns items with playlistItemId, videoId, title, and thumbnail (plus the
    detail fields when details=True). Also returns nextPageToken if more pages
    exist, and deadCount/deadItemIds for deleted/private videos.
    Quota cost: 1 unit per page (+1 unit per 50 items when details=True).
    """
    params: dict[str, Any] = {}
    if page_token:
        params["pageToken"] = page_token
    if details:
        params["details"] = "true"
    return _request("GET", f"/playlist/{playlist_id}/items", params=params)


@mcp.tool()
def get_channel_videos(
    channel: str,
    since_hours: float = 24.0,
    min_duration_seconds: int = 0,
    max_results: int = 50,
) -> dict[str, Any]:
    """List a channel's recent uploads, with duration and publish time.

    Resolves a channel by @handle, channel URL, or UC... ID (handle→uploads
    mappings are cached server-side, so repeat scans are quota-free), then
    returns uploads within the recency window.

    Args:
        channel: @handle (e.g. "@DenysDavydov"), channel URL, or UC... channel ID.
        since_hours: Only return videos published within this many hours (default 24).
        min_duration_seconds: Drop videos shorter than this (default 0 = no filter).
        max_results: Maximum number of videos to return (default 50).

    Returns channelId, channelTitle, and videos with videoId, title, url,
    publishedAt, durationSeconds, and channelTitle.
    Quota cost: 1 unit to resolve (cached), 1 unit per page of uploads scanned,
                1 unit per 50 videos for duration lookup.
    """
    return _request(
        "GET",
        "/api/channel/videos",
        params={
            "channel": channel,
            "since_hours": since_hours,
            "min_duration_seconds": min_duration_seconds,
            "max_results": max_results,
        },
    )


@mcp.tool()
def check_playlist_membership(
    video_ids: list[str],
    playlists: list[str],
) -> dict[str, Any]:
    """Check which playlists already contain each of the given videos.

    Args:
        video_ids: YouTube video IDs to look up.
        playlists: Playlist names or IDs to scan.

    Returns membership (videoId -> list of playlist refs containing it),
    resolved (ref -> playlistId), and unresolvedPlaylists.
    Quota cost: 1 unit per 50 items in each scanned playlist.
    """
    return _request(
        "POST",
        "/api/playlist-membership",
        json_body={"video_ids": video_ids, "playlists": playlists},
    )


@mcp.tool()
def random_videos(playlist_name: str, count: int = 5) -> dict[str, Any]:
    """Return a random selection of videos from a playlist.

    The sample is drawn from ALL videos in the playlist (the server paginates
    through every page first), not just the first 50/100.

    Args:
        playlist_name: The playlist's title, as shown on YouTube (case-insensitive).
        count: How many random videos to return (default 5). If the playlist has
               fewer videos than requested, all of them are returned.

    Returns videos with title, url, and thumbnailUrl, plus totalAvailable
    (live videos in the playlist) and returned (how many were sampled).
    Quota cost: 1 unit per 50 videos in the playlist (full scan to sample fairly).
    """
    return _request(
        "GET",
        "/api/random-videos",
        params={"playlist": playlist_name, "count": count},
    )


@mcp.tool()
def get_quota_status() -> dict[str, Any]:
    """Check remaining API budget and savings stats.

    Returns:
        quotaLimit: Daily quota limit in units.
        unitsUsed: Units used today.
        unitsRemaining: Units remaining today.
        writeOperationsRemaining: Number of 50-unit write ops remaining.
        unitsSavedToday: Units saved by optimizations (cache, dedup).
        callsAvertedToday: API calls avoided.
        savingsBreakdown: Per-source breakdown of savings.
    """
    return _request("GET", "/api/quota")


# ---------------------------------------------------------------------------
# Write Operations (50 quota units each)
# ---------------------------------------------------------------------------


@mcp.tool()
def rename_playlist(playlist_id: str, new_name: str) -> dict[str, Any]:
    """Update playlist title.

    Args:
        playlist_id: The YouTube playlist ID.
        new_name: The new title for the playlist.

    Quota cost: 50 units.
    """
    return _request("POST", f"/playlist/{playlist_id}/rename", json_body={"name": new_name})


@mcp.tool()
def delete_playlist(playlist_id: str) -> dict[str, Any]:
    """Delete a single playlist.

    Args:
        playlist_id: The YouTube playlist ID to delete.

    Quota cost: 50 units.
    """
    return _request("POST", f"/delete/{playlist_id}")


@mcp.tool()
def delete_playlists(playlist_ids: list[str]) -> dict[str, Any]:
    """Bulk delete multiple playlists.

    Args:
        playlist_ids: List of YouTube playlist IDs to delete.

    Returns success status, failures list, and whether quota blocked any deletions.
    Quota cost: 50 units per playlist.
    """
    return _request("POST", "/delete-bulk", json_body={"playlist_ids": playlist_ids})


@mcp.tool()
def cleanup_playlist(playlist_id: str, item_ids: list[str] | None = None) -> dict[str, Any]:
    """Remove deleted/private videos from a playlist.

    Args:
        playlist_id: The YouTube playlist ID.
        item_ids: Optional list of specific playlistItemIds to remove.
                  If not provided, scans the entire playlist for dead videos.

    Returns removed count, removedItemIds, and any failures.
    Quota cost: 50 units per video removed (plus 1 unit per 50 videos if scanning).
    """
    body = {}
    if item_ids:
        body["item_ids"] = item_ids
    return _request("POST", f"/playlist/{playlist_id}/cleanup", json_body=body)


@mcp.tool()
def dedupe_playlist(playlist_id: str) -> dict[str, Any]:
    """Remove duplicate videos from a playlist (keeps first occurrence).

    Args:
        playlist_id: The YouTube playlist ID.

    Returns removed count, duplicates found, and remaining video count.
    Quota cost: 1 unit per 50 videos to scan, 50 units per duplicate removed.
    """
    return _request("POST", f"/playlist/{playlist_id}/dedupe")


@mcp.tool()
def import_videos(playlist_id: str, video_ids: list[str]) -> dict[str, Any]:
    """Add videos to a playlist by ID.

    Args:
        playlist_id: The YouTube playlist ID to add videos to.
        video_ids: List of YouTube video IDs to add.

    Returns added count, alreadyPresent count (skipped), and any failures.
    Quota cost: 50 units per video added (videos already present are free).
    """
    return _request("POST", f"/playlist/{playlist_id}/import", json_body={"video_ids": video_ids})


@mcp.tool()
def add_channel_videos_to_playlist(
    playlist: str,
    channels: list[str],
    since_hours: float = 24.0,
    min_duration_seconds: int = 0,
    exclude_playlists: list[str] | None = None,
) -> dict[str, Any]:
    """Add recent uploads from several channels to a playlist, in one call.

    For each channel, finds uploads within the recency window, then adds a video
    ONLY if it is at least min_duration_seconds long AND not already in the
    target playlist or any excluded playlist. Handles resolution, dedup, and
    duration filtering server-side so the whole workflow is one reliable call.

    Args:
        playlist: Target playlist name or ID to add videos to.
        channels: List of @handles, channel URLs, or UC... IDs to pull from.
        since_hours: Recency window in hours (default 24).
        min_duration_seconds: Skip videos shorter than this (default 0 = no filter).
        exclude_playlists: Playlist names/IDs whose videos must not be re-added
                           (e.g. an "already played" list).

    Returns added (total), quotaBlocked, unresolvedExcludePlaylists, and a
    byChannel breakdown with per-channel added / skippedPresent / skippedShort /
    failures / error.
    Quota cost: reads to scan channels + exclusion playlists (1 unit each per
                page/50), plus 50 units per video actually added.
    """
    return _request(
        "POST",
        "/api/add-from-channels",
        json_body={
            "playlist": playlist,
            "channels": channels,
            "since_hours": since_hours,
            "min_duration_seconds": min_duration_seconds,
            "exclude_playlists": exclude_playlists or [],
        },
    )


@mcp.tool()
def transfer_items(
    playlist_id: str,
    destination_id: str,
    items: list[dict[str, str]],
    mode: str = "copy",
) -> dict[str, Any]:
    """Copy or move videos between playlists.

    Args:
        playlist_id: The source playlist ID.
        destination_id: The destination playlist ID.
        items: List of dicts with "videoId" and optionally "playlistItemId" keys.
               playlistItemId is required for move mode to delete from source.
        mode: "copy" (default) or "move". Move also removes from source.

    Returns added count, removed count (for move), alreadyPresent count.
    Quota cost: 50 units per video copied, 50 units per video removed (move mode).
    """
    return _request(
        "POST",
        f"/playlist/{playlist_id}/items/transfer",
        json_body={"destination_id": destination_id, "items": items, "mode": mode},
    )


@mcp.tool()
def merge_playlists(
    target_id: str,
    source_ids: list[str],
    new_name: str | None = None,
) -> dict[str, Any]:
    """Combine multiple playlists into one.

    Copies all videos from source playlists into the target, then deletes the
    source playlists. Duplicates are skipped. Videos already in target are not
    re-added.

    Args:
        target_id: The playlist ID to merge into (will be kept).
        source_ids: List of playlist IDs to merge from (will be deleted after).
        new_name: Optional new name for the merged playlist.

    Returns added count, alreadyPresent count, and any failures.
    Quota cost: 1 unit per 50 source videos to read, 50 units per video added,
                50 units per source playlist deleted, 50 units to rename (if new_name).
    """
    body: dict[str, Any] = {"target_id": target_id, "source_ids": source_ids}
    if new_name:
        body["new_name"] = new_name
    return _request("POST", "/merge-playlists", json_body=body)


if __name__ == "__main__":
    mcp.run()
