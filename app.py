import hashlib
import json
import os
from datetime import datetime, timezone, timedelta

from flask import Flask, redirect, render_template, request, session, url_for
from werkzeug.middleware.proxy_fix import ProxyFix
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from pymongo import MongoClient

SCOPES = ["https://www.googleapis.com/auth/youtube"]
try:
    from dotenv import load_dotenv
except ImportError:  # optional
    load_dotenv = None

if load_dotenv:
    load_dotenv()

IS_PRODUCTION = os.getenv("DYNO") is not None  # Heroku sets DYNO automatically
if not IS_PRODUCTION and not os.getenv("OAUTHLIB_INSECURE_TRANSPORT"):
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
if not os.getenv("OAUTHLIB_RELAX_TOKEN_SCOPE"):
    os.environ["OAUTHLIB_RELAX_TOKEN_SCOPE"] = "1"

CLIENT_SECRETS_FILE = os.getenv("YT_CLIENT_SECRETS", "client_secret.json")
CLIENT_ID = os.getenv("YT_CLIENT_ID") or os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("YT_CLIENT_SECRET") or os.getenv("CLIENT_SECRET")
REDIRECT_URI_OVERRIDE = os.getenv("REDIRECT_URI")  # explicit override if auto-detection fails
TOKEN_FILE = (
    os.getenv("YT_TOKEN_FILE")
    or os.getenv("TOKEN_FILE")
    or "token.json"
)
API_CALL_QUOTA = os.getenv("YT_API_CALLS_QUOTA")
# YouTube Data API v3 default daily quota, measured in *units* (not calls).
# Used as the budget when YT_API_CALLS_QUOTA is not set.
DEFAULT_QUOTA_LIMIT = 10000
API_CALL_COUNT = 0  # running total of quota *units* spent this process

# Per-operation quota-unit costs.
# Reads cost 1 unit; writes (insert/update/delete) cost 50 units each.
# https://developers.google.com/youtube/v3/determine_quota_cost
QUOTA_UNIT_COSTS = {"list": 1, "insert": 50, "update": 50, "delete": 50}
DEFAULT_UNIT_COST = 1
WRITE_UNIT_COST = 50

# How long to cache the user's playlist list server-side before refetching.
PLAYLIST_CACHE_TTL_SECONDS = int(os.getenv("YT_PLAYLIST_CACHE_TTL", "300"))

# Playlists to hide from the UI. "PLPs83dPIe4l4" is a YouTube "ghost" playlist:
# it appears in playlists.list(mine=True) but cannot be opened or deleted
# ("The playlist does not exist"), so we filter it out. Add more (comma/space
# separated) via the YT_IGNORE_PLAYLISTS env var.
IGNORED_PLAYLIST_IDS = {"PLPs83dPIe4l4"} | {
    pid.strip()
    for chunk in os.getenv("YT_IGNORE_PLAYLISTS", "").split(",")
    for pid in chunk.split()
    if pid.strip()
}

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
mongo_db = mongo_client.get_default_database()
api_logs_collection = mongo_db["api_logs"]
playlist_cache_collection = mongo_db["playlist_cache"]
savings_log_collection = mongo_db["savings_log"]

# Index the timestamp so the daily quota/savings tallies are index-only scans.
try:
    api_logs_collection.create_index("timestamp")
    savings_log_collection.create_index("timestamp")
except Exception:
    pass

# On startup, delete logged records from previous days
today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
deleted = api_logs_collection.delete_many({"timestamp": {"$lt": today_start}})
if deleted.deleted_count > 0:
    print(f"[Startup] Deleted {deleted.deleted_count} API log record(s) from previous days.")
savings_log_collection.delete_many({"timestamp": {"$lt": today_start}})

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev_secret_key_change_me")
if IS_PRODUCTION:
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    app.config["PREFERRED_URL_SCHEME"] = "https"


@app.after_request
def inject_api_stats(response):
    """Inject quotaUnitsToday into every JSON response so the frontend can update stats."""
    if response.content_type and "application/json" in response.content_type:
        try:
            data = response.get_json()
            if isinstance(data, dict):
                data["quotaUnitsToday"] = get_api_calls_today()
                saved = get_savings_today()
                data["quotaUnitsSavedToday"] = saved["units"]
                data["callsAvertedToday"] = saved["calls"]
                data["savingsBreakdown"] = saved["by_source"]
                response.set_data(json.dumps(data))
        except Exception:
            pass
    return response


def credentials_to_dict(credentials):
    data = {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": credentials.scopes,
    }
    if credentials.expiry:
        data["expiry"] = credentials.expiry.isoformat()
    return data


def record_api_call(endpoint="unknown", call_type="unknown", count=1):
    """Log an API call weighted by its true quota-unit cost (read=1, write=50)."""
    global API_CALL_COUNT
    units = QUOTA_UNIT_COSTS.get(call_type, DEFAULT_UNIT_COST) * count
    API_CALL_COUNT += units
    api_logs_collection.insert_one({
        "timestamp": datetime.now(timezone.utc),
        "endpoint": endpoint,
        "type": call_type,
        "units": units,
    })


def get_api_calls_today():
    """Return total quota *units* (not raw call count) spent today."""
    today_start_utc = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    cursor = api_logs_collection.aggregate([
        {"$match": {"timestamp": {"$gte": today_start_utc}}},
        {"$group": {"_id": None, "units": {"$sum": {"$ifNull": ["$units", 1]}}}},
    ])
    doc = next(iter(cursor), None)
    return int(doc["units"]) if doc else 0


def get_quota_limit():
    try:
        return int(API_CALL_QUOTA) if API_CALL_QUOTA else DEFAULT_QUOTA_LIMIT
    except (TypeError, ValueError):
        return DEFAULT_QUOTA_LIMIT


def get_quota_remaining():
    return max(get_quota_limit() - get_api_calls_today(), 0)


def remaining_write_budget():
    """How many 50-unit write operations we can still afford today."""
    return get_quota_remaining() // WRITE_UNIT_COST


def record_savings(source, calls, units):
    """Record API calls/units avoided by an optimization (cache, dedup, sort)."""
    calls = int(calls or 0)
    units = int(units or 0)
    if calls <= 0 and units <= 0:
        return
    try:
        savings_log_collection.insert_one({
            "timestamp": datetime.now(timezone.utc),
            "source": source,
            "calls": calls,
            "units": units,
        })
    except Exception:
        pass


def get_savings_today():
    """Total calls/units avoided today (vs. a naive app), with a per-source breakdown."""
    today_start_utc = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    by_source = {}
    total_calls = 0
    total_units = 0
    try:
        cursor = savings_log_collection.aggregate([
            {"$match": {"timestamp": {"$gte": today_start_utc}}},
            {"$group": {"_id": "$source",
                        "calls": {"$sum": "$calls"},
                        "units": {"$sum": "$units"}}},
        ])
        for doc in cursor:
            src = doc.get("_id") or "other"
            c = int(doc.get("calls") or 0)
            u = int(doc.get("units") or 0)
            by_source[src] = {"calls": c, "units": u}
            total_calls += c
            total_units += u
    except Exception:
        pass
    return {"calls": total_calls, "units": total_units, "by_source": by_source}


def get_youtube_client(credentials_dict):
    """Build a YouTube API client from stored credentials, refreshing if needed.

    Uses the static discovery document bundled with google-api-python-client
    (static_discovery=True, the default in v2+), so build() makes no network
    request. Note: discovery fetches never count against the YouTube Data API
    quota regardless — only actual youtube/v3 method calls do.
    """
    normalized = normalize_saved_credentials(credentials_dict) or credentials_dict
    creds = Credentials(**normalized)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        save_credentials(creds)
    return build("youtube", "v3", credentials=creds, static_discovery=True), creds


def get_flow(state=None):
    if REDIRECT_URI_OVERRIDE:
        redirect_uri = REDIRECT_URI_OVERRIDE
    else:
        redirect_uri = url_for("oauth2callback", _external=True)
        if IS_PRODUCTION and redirect_uri.startswith("http://"):
            redirect_uri = redirect_uri.replace("http://", "https://", 1)
    print(f"[OAuth] redirect_uri = {redirect_uri}", flush=True)
    if CLIENT_ID and CLIENT_SECRET:
        client_config = {
            "web": {
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [redirect_uri],
            }
        }
        return Flow.from_client_config(
            client_config,
            scopes=SCOPES,
            state=state,
            redirect_uri=redirect_uri,
        )
    return Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE,
        scopes=SCOPES,
        state=state,
        redirect_uri=redirect_uri,
    )


def get_client_secrets_error():
    if CLIENT_ID and CLIENT_SECRET:
        return None
    if os.path.exists(CLIENT_SECRETS_FILE):
        return None
    return (
        "Missing OAuth client secrets file. "
        "Download it from Google Cloud Console and save it as "
        f"'{CLIENT_SECRETS_FILE}'."
    )


def normalize_saved_credentials(data):
    if not isinstance(data, dict):
        return None
    if data.get("token"):
        expiry = data.get("expiry")
        if isinstance(expiry, str):
            try:
                parsed = datetime.fromisoformat(expiry)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
                data["expiry"] = parsed
            except ValueError:
                data.pop("expiry", None)
        return data
    if data.get("access_token"):
        scopes = data.get("scope") or data.get("scopes") or SCOPES
        if isinstance(scopes, str):
            scopes = [s for s in scopes.split(" ") if s]
        expiry = None
        expiry_date = data.get("expiry_date")
        if isinstance(expiry_date, (int, float)):
            expiry = datetime.fromtimestamp(expiry_date / 1000, tz=timezone.utc)
            expiry = expiry.astimezone(timezone.utc).replace(tzinfo=None)
        return {
            "token": data.get("access_token"),
            "refresh_token": data.get("refresh_token"),
            "token_uri": data.get("token_uri") or "https://oauth2.googleapis.com/token",
            "client_id": data.get("client_id") or CLIENT_ID,
            "client_secret": data.get("client_secret") or CLIENT_SECRET,
            "scopes": scopes,
            "expiry": expiry,
        }
    return None


def load_saved_credentials():
    if not os.path.exists(TOKEN_FILE):
        return None
    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as token_file:
            data = json.load(token_file)
        return normalize_saved_credentials(data)
    except (OSError, json.JSONDecodeError):
        return None


def save_credentials(credentials):
    try:
        token_dir = os.path.dirname(TOKEN_FILE)
        if token_dir:
            os.makedirs(token_dir, exist_ok=True)
        existing = load_saved_credentials() or {}
        data = credentials_to_dict(credentials)
        if not data.get("refresh_token") and existing.get("refresh_token"):
            data["refresh_token"] = existing.get("refresh_token")
        if not data.get("token_uri") and existing.get("token_uri"):
            data["token_uri"] = existing.get("token_uri")
        if not data.get("client_id") and existing.get("client_id"):
            data["client_id"] = existing.get("client_id")
        if not data.get("client_secret") and existing.get("client_secret"):
            data["client_secret"] = existing.get("client_secret")
        with open(TOKEN_FILE, "w", encoding="utf-8") as token_file:
            json.dump(data, token_file)
    except OSError:
        return


def cache_key_for(credentials_dict):
    creds = credentials_dict or {}
    token = creds.get("refresh_token") or creds.get("token") or "default"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def get_cached_playlists(credentials_dict):
    """Return the cached playlist list if fresh, else None."""
    try:
        doc = playlist_cache_collection.find_one({"_id": cache_key_for(credentials_dict)})
    except Exception:
        return None
    if not doc:
        return None
    fetched_at = doc.get("fetched_at")
    if not isinstance(fetched_at, datetime):
        return None
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - fetched_at).total_seconds()
    if age > PLAYLIST_CACHE_TTL_SECONDS:
        return None
    return doc.get("items")


def set_cached_playlists(credentials_dict, items):
    key = cache_key_for(credentials_dict)
    try:
        playlist_cache_collection.replace_one(
            {"_id": key},
            {"_id": key, "fetched_at": datetime.now(timezone.utc), "items": items},
            upsert=True,
        )
    except Exception:
        pass


def invalidate_playlist_cache(credentials_dict):
    try:
        playlist_cache_collection.delete_one({"_id": cache_key_for(credentials_dict)})
    except Exception:
        pass


def fetch_playlist_video_ids(youtube, playlist_id):
    """Return the set of videoIds already in a playlist (1 unit per 50 items).

    Used to skip inserts of videos already present, avoiding wasted 50-unit writes.
    """
    video_ids = set()
    request_page = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=playlist_id,
        maxResults=50,
    )
    while request_page is not None:
        response = request_page.execute()
        record_api_call(endpoint="playlistItems.list", call_type="list")
        for item in response.get("items", []):
            video_id = (item.get("contentDetails") or {}).get("videoId")
            if video_id:
                video_ids.add(video_id)
        request_page = youtube.playlistItems().list_next(request_page, response)
    return video_ids


def get_playlists(credentials_dict):
    youtube, _ = get_youtube_client(credentials_dict)
    playlists = []
    request_page = youtube.playlists().list(
        part="snippet,contentDetails",
        mine=True,
        maxResults=50,
    )
    while request_page is not None:
        response = request_page.execute()
        record_api_call(endpoint="playlists.list", call_type="list")
        playlists.extend(response.get("items", []))
        request_page = youtube.playlists().list_next(request_page, response)
    return playlists


def sort_playlists(playlists, sort_by, sort_order):
    reverse = sort_order == "desc"
    if sort_by == "count":
        key_func = lambda item: item.get("contentDetails", {}).get("itemCount", 0)
    else:
        key_func = lambda item: (item.get("snippet", {}).get("title") or "").lower()
    return sorted(playlists, key=key_func, reverse=reverse)


@app.route("/")
def index():
    credentials = session.get("credentials")
    if not credentials:
        saved_credentials = load_saved_credentials()
        if saved_credentials:
            session["credentials"] = saved_credentials
            credentials = saved_credentials
        else:
            return render_template("index.html")
    try:
        playlists = get_cached_playlists(credentials)
        if playlists is None:
            playlists = get_playlists(credentials)
            set_cached_playlists(credentials, playlists)
        else:
            # Served from cache — record the playlists.list calls we avoided.
            pages = max(1, (len(playlists) + 49) // 50)
            record_savings("cache", pages, pages)
        if IGNORED_PLAYLIST_IDS:
            playlists = [p for p in playlists if p.get("id") not in IGNORED_PLAYLIST_IDS]
        sort_by = request.args.get("sort", "title")
        sort_order = request.args.get("order", "asc")
        if sort_by not in {"title", "count"}:
            sort_by = "title"
        if sort_order not in {"asc", "desc"}:
            sort_order = "asc"
        playlists = sort_playlists(playlists, sort_by, sort_order)
    except Exception as exc:
        return render_template("error.html", error_message=str(exc))
    total_playlists = len(playlists)
    total_videos = sum(
        (item.get("contentDetails", {}).get("itemCount", 0) or 0)
        for item in playlists
    )
    api_calls_today = get_api_calls_today()
    quota = get_quota_limit()
    api_calls_remaining = max(quota - api_calls_today, 0)
    saved = get_savings_today()
    return render_template(
        "playlists.html",
        playlists=playlists,
        sort_by=sort_by,
        sort_order=sort_order,
        total_playlists=total_playlists,
        total_videos=total_videos,
        api_calls_used=api_calls_today,
        api_calls_remaining=api_calls_remaining,
        quota_limit=quota,
        units_saved=saved["units"],
        calls_averted=saved["calls"],
        savings_breakdown=saved["by_source"],
        read_unit_cost=DEFAULT_UNIT_COST,
        write_unit_cost=WRITE_UNIT_COST,
    )


@app.route("/delete/<playlist_id>", methods=["POST"])
def delete_playlist(playlist_id):
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return redirect(url_for("index"))
    youtube, _ = get_youtube_client(credentials)
    try:
        youtube.playlists().delete(id=playlist_id).execute()
        record_api_call(endpoint="playlists.delete", call_type="delete")
        invalidate_playlist_cache(credentials)
    except Exception as exc:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return {"error": str(exc)}, 400
        return render_template("error.html", error_message=str(exc))
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return {"success": True}
    sort_by = request.args.get("sort", "title")
    sort_order = request.args.get("order", "asc")
    return redirect(url_for("index", sort=sort_by, order=sort_order))


@app.route("/delete-bulk", methods=["POST"])
def delete_bulk():
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    payload = request.get_json(silent=True) or {}
    playlist_ids = payload.get("playlist_ids") or []
    if not isinstance(playlist_ids, list) or not playlist_ids:
        return {"error": "No playlists provided"}, 400
    youtube, _ = get_youtube_client(credentials)
    budget = remaining_write_budget()
    failures = []
    skipped = 0
    for playlist_id in playlist_ids:
        if budget <= 0:
            skipped += 1
            continue
        try:
            youtube.playlists().delete(id=playlist_id).execute()
            record_api_call(endpoint="playlists.delete", call_type="delete")
            budget -= 1
        except Exception as exc:
            failures.append({"id": playlist_id, "error": str(exc)})
    invalidate_playlist_cache(credentials)
    return {
        "success": len(failures) == 0 and skipped == 0,
        "failures": failures,
        "skipped": skipped,
        "quotaBlocked": skipped > 0,
    }


@app.route("/playlist/<playlist_id>/items")
def playlist_items(playlist_id):
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    youtube, _ = get_youtube_client(credentials)
    page_token = request.args.get("pageToken") or None
    api_request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=50,
        pageToken=page_token,
    )
    try:
        response = api_request.execute()
    except Exception as exc:
        return {
            "items": [],
            "nextPageToken": None,
            "deadItemIds": [],
            "deadCount": 0,
            "error": "This playlist could not be read (it may have been deleted): " + str(exc),
        }
    record_api_call(endpoint="playlistItems.list", call_type="list")
    simplified = []
    dead_item_ids = []
    for item in response.get("items", []):
        snippet = item.get("snippet") or {}
        details = item.get("contentDetails") or {}
        title = snippet.get("title") or ""
        if title in ("Deleted video", "Private video"):
            # Report dead videos but DO NOT delete them here — deleting on a read
            # path silently costs 50 units each. Cleanup is an explicit POST action.
            dead_item_ids.append(item.get("id"))
            continue
        simplified.append(
            {
                "playlistItemId": item.get("id"),
                "videoId": details.get("videoId"),
                "title": title,
                "thumbnail": ((snippet.get("thumbnails") or {}).get("default") or {}).get("url"),
            }
        )
    return {
        "items": simplified,
        "nextPageToken": response.get("nextPageToken"),
        "deadItemIds": dead_item_ids,
        "deadCount": len(dead_item_ids),
    }


@app.route("/playlist/<playlist_id>/cleanup", methods=["POST"])
def cleanup_playlist(playlist_id):
    """Explicitly remove deleted/private videos. Each removal costs 50 units."""
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    payload = request.get_json(silent=True) or {}
    item_ids = payload.get("item_ids")
    youtube, _ = get_youtube_client(credentials)
    # If the caller didn't supply specific IDs, scan the whole playlist for dead ones.
    if not isinstance(item_ids, list) or not item_ids:
        item_ids = []
        try:
            request_page = youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=50,
            )
            while request_page is not None:
                resp = request_page.execute()
                record_api_call(endpoint="playlistItems.list", call_type="list")
                for it in resp.get("items", []):
                    title = (it.get("snippet") or {}).get("title") or ""
                    if title in ("Deleted video", "Private video"):
                        item_ids.append(it.get("id"))
                request_page = youtube.playlistItems().list_next(request_page, resp)
        except Exception as exc:
            return {
                "success": False,
                "removed": 0,
                "removedItemIds": [],
                "skipped": 0,
                "quotaBlocked": False,
                "failures": [],
                "error": "This playlist could not be read (it may have been deleted): " + str(exc),
            }
    budget = remaining_write_budget()
    failures = []
    removed_item_ids = []
    skipped = 0
    for item_id in item_ids:
        if not item_id:
            continue
        if budget <= 0:
            skipped += 1
            continue
        try:
            youtube.playlistItems().delete(id=item_id).execute()
            record_api_call(endpoint="playlistItems.delete", call_type="delete")
            removed_item_ids.append(item_id)
            budget -= 1
        except Exception as exc:
            failures.append({"id": item_id, "error": str(exc)})
    if removed_item_ids:
        invalidate_playlist_cache(credentials)
    return {
        "success": len(failures) == 0 and skipped == 0,
        "removed": len(removed_item_ids),
        "removedItemIds": removed_item_ids,
        "skipped": skipped,
        "quotaBlocked": skipped > 0,
        "failures": failures,
    }


@app.route("/playlist/<playlist_id>/dedupe", methods=["POST"])
def dedupe_playlist(playlist_id):
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    youtube, _ = get_youtube_client(credentials)
    items = []
    try:
        request_page = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
        )
        while request_page is not None:
            response = request_page.execute()
            record_api_call(endpoint="playlistItems.list", call_type="list")
            items.extend(response.get("items", []))
            request_page = youtube.playlistItems().list_next(request_page, response)
    except Exception as exc:
        return {
            "success": False,
            "error": "This playlist could not be read (it may have been deleted): " + str(exc),
        }
    seen_video_ids = set()
    duplicate_item_ids = []
    for item in items:
        details = item.get("contentDetails") or {}
        video_id = details.get("videoId")
        playlist_item_id = item.get("id")
        if not video_id or not playlist_item_id:
            continue
        if video_id in seen_video_ids:
            duplicate_item_ids.append(playlist_item_id)
        else:
            seen_video_ids.add(video_id)
    failures = []
    removed_item_ids = []
    budget = remaining_write_budget()
    skipped = 0
    for item_id in duplicate_item_ids:
        if budget <= 0:
            skipped += 1
            continue
        try:
            youtube.playlistItems().delete(id=item_id).execute()
            record_api_call(endpoint="playlistItems.delete", call_type="delete")
            removed_item_ids.append(item_id)
            budget -= 1
        except Exception as exc:
            failures.append({"id": item_id, "error": str(exc)})
    removed_count = len(removed_item_ids)
    remaining_count = len(items) - removed_count
    if removed_count:
        invalidate_playlist_cache(credentials)
    return {
        "success": len(failures) == 0 and skipped == 0,
        "removed": removed_count,
        "removedItemIds": removed_item_ids,
        "duplicates": len(duplicate_item_ids),
        "remaining": remaining_count,
        "skipped": skipped,
        "quotaBlocked": skipped > 0,
        "failures": failures,
    }


@app.route("/playlist/<playlist_id>/items/delete-bulk", methods=["POST"])
def delete_playlist_items_bulk(playlist_id):
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    payload = request.get_json(silent=True) or {}
    item_ids = payload.get("playlist_item_ids") or []
    if not isinstance(item_ids, list) or not item_ids:
        return {"error": "No items provided"}, 400
    youtube, _ = get_youtube_client(credentials)
    budget = remaining_write_budget()
    failures = []
    skipped = 0
    for item_id in item_ids:
        if budget <= 0:
            skipped += 1
            continue
        try:
            youtube.playlistItems().delete(id=item_id).execute()
            record_api_call(endpoint="playlistItems.delete", call_type="delete")
            budget -= 1
        except Exception as exc:
            failures.append({"id": item_id, "error": str(exc)})
    invalidate_playlist_cache(credentials)
    return {
        "success": len(failures) == 0 and skipped == 0,
        "failures": failures,
        "skipped": skipped,
        "quotaBlocked": skipped > 0,
    }


@app.route("/playlist/<playlist_id>/items/transfer", methods=["POST"])
def transfer_playlist_items(playlist_id):
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    payload = request.get_json(silent=True) or {}
    destination_id = payload.get("destination_id")
    items = payload.get("items") or []
    mode = payload.get("mode") or "copy"
    if mode not in {"copy", "move"}:
        return {"error": "Invalid transfer mode"}, 400
    if not destination_id:
        return {"error": "Destination playlist is required"}, 400
    if not isinstance(items, list) or not items:
        return {"error": "No items provided"}, 400
    youtube, _ = get_youtube_client(credentials)
    # Skip inserting videos already present in the destination (saves 50 units each).
    try:
        existing_video_ids = fetch_playlist_video_ids(youtube, destination_id)
    except Exception as exc:
        return {
            "success": False,
            "added": 0,
            "removed": 0,
            "failures": [{"error": "Destination playlist could not be read "
                                   "(it may have been deleted): " + str(exc)}],
        }
    budget = remaining_write_budget()
    failures = []
    added = 0
    skipped = 0
    already_present = 0
    delete_after = []
    for item in items:
        if not isinstance(item, dict):
            continue
        video_id = item.get("videoId")
        playlist_item_id = item.get("playlistItemId")
        if not video_id:
            failures.append({"videoId": video_id, "error": "Missing videoId"})
            continue
        if video_id in existing_video_ids:
            # Already in destination; skip the 50-unit insert. Still remove from source on move.
            already_present += 1
            if mode == "move" and playlist_item_id:
                delete_after.append(playlist_item_id)
            continue
        if budget <= 0:
            skipped += 1
            continue
        try:
            youtube.playlistItems().insert(
                part="snippet",
                body={
                    "snippet": {
                        "playlistId": destination_id,
                        "resourceId": {
                            "kind": "youtube#video",
                            "videoId": video_id,
                        },
                    }
                },
            ).execute()
            record_api_call(endpoint="playlistItems.insert", call_type="insert")
            added += 1
            budget -= 1
            existing_video_ids.add(video_id)
            if mode == "move" and playlist_item_id:
                delete_after.append(playlist_item_id)
        except Exception as exc:
            failures.append({"videoId": video_id, "error": str(exc)})
    removed_item_ids = []
    if mode == "move" and delete_after:
        for item_id in delete_after:
            if budget <= 0:
                skipped += 1
                continue
            try:
                youtube.playlistItems().delete(id=item_id).execute()
                record_api_call(endpoint="playlistItems.delete", call_type="delete")
                removed_item_ids.append(item_id)
                budget -= 1
            except Exception as exc:
                failures.append({"id": item_id, "error": str(exc)})
    if already_present:
        record_savings("dedup", already_present, already_present * WRITE_UNIT_COST)
    invalidate_playlist_cache(credentials)
    return {
        "success": len(failures) == 0 and skipped == 0,
        "added": added,
        "removed": len(removed_item_ids),
        "removedItemIds": removed_item_ids,
        "alreadyPresent": already_present,
        "skipped": skipped,
        "quotaBlocked": skipped > 0,
        "failures": failures,
    }


@app.route("/merge-playlists", methods=["POST"])
def merge_playlists():
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    payload = request.get_json(silent=True) or {}
    target_id = payload.get("target_id")
    source_ids = payload.get("source_ids") or []
    new_name = payload.get("new_name")
    if new_name is not None:
        new_name = str(new_name).strip()
    if not target_id or not isinstance(source_ids, list) or len(source_ids) < 1:
        return {"error": "Invalid playlist selection"}, 400
    if new_name is not None and not new_name:
        return {"error": "New name is required"}, 400
    youtube, _ = get_youtube_client(credentials)
    # Skip inserting videos already present in the target (saves 50 units each).
    try:
        existing_video_ids = fetch_playlist_video_ids(youtube, target_id)
    except Exception as exc:
        return {
            "success": False,
            "added": 0,
            "failures": [{"playlist_id": target_id,
                          "error": "Target playlist could not be read: " + str(exc)}],
        }
    budget = remaining_write_budget()

    failures = []
    added = 0
    skipped = 0
    already_present = 0
    unavailable = 0  # ghost playlists / deleted videos — nothing the user can fix
    source_fully_merged = {}
    for playlist_id in source_ids:
        ok = True
        try:
            request_page = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
            )
            while request_page is not None:
                response = request_page.execute()
                record_api_call(endpoint="playlistItems.list", call_type="list")
                for item in response.get("items", []):
                    video_id = (item.get("contentDetails") or {}).get("videoId")
                    if not video_id:
                        continue
                    if video_id in existing_video_ids:
                        already_present += 1
                        continue
                    if budget <= 0:
                        skipped += 1
                        ok = False
                        continue
                    try:
                        youtube.playlistItems().insert(
                            part="snippet",
                            body={
                                "snippet": {
                                    "playlistId": target_id,
                                    "resourceId": {
                                        "kind": "youtube#video",
                                        "videoId": video_id,
                                    },
                                }
                            },
                        ).execute()
                        record_api_call(endpoint="playlistItems.insert", call_type="insert")
                        added += 1
                        budget -= 1
                        existing_video_ids.add(video_id)
                    except Exception as exc:
                        # A 404 means the video is deleted/private/unavailable and
                        # can't be added — nothing the user can fix, so skip it
                        # quietly rather than reporting a failure.
                        status = getattr(getattr(exc, "resp", None), "status", None)
                        if status == 404:
                            unavailable += 1
                        else:
                            failures.append({"playlist_id": playlist_id, "video_id": video_id, "error": str(exc)})
                            ok = False
                request_page = youtube.playlistItems().list_next(request_page, response)
        except Exception as exc:
            # Source playlist could not be read. A 404 means it's a ghost / already
            # gone — nothing the user can fix, so skip it quietly. Other errors are
            # reported. Never delete a source we couldn't fully read.
            status = getattr(getattr(exc, "resp", None), "status", None)
            if status == 404:
                unavailable += 1
            else:
                failures.append({"playlist_id": playlist_id,
                                 "error": "Could not read playlist: " + str(exc)})
            ok = False
        source_fully_merged[playlist_id] = ok

    # Only delete a source whose videos were ALL copied — never lose videos that
    # didn't make it across (e.g. when the daily quota budget ran out).
    for playlist_id in source_ids:
        if not source_fully_merged.get(playlist_id):
            continue
        if budget <= 0:
            skipped += 1
            continue
        try:
            youtube.playlists().delete(id=playlist_id).execute()
            record_api_call(endpoint="playlists.delete", call_type="delete")
            budget -= 1
        except Exception as exc:
            # A 404 means the source playlist is already gone — e.g. it was
            # deleted by a prior merge but still lingered in playlists.list
            # (YouTube is eventually consistent), or it's a ghost. The videos
            # were already copied and the source no longer exists, so the merge
            # goal is met — treat it as success rather than a failure.
            status = getattr(getattr(exc, "resp", None), "status", None)
            if status != 404:
                failures.append({"playlist_id": playlist_id, "error": str(exc)})

    if new_name and budget > 0:
        try:
            youtube.playlists().update(
                part="snippet",
                body={"id": target_id, "snippet": {"title": new_name}},
            ).execute()
            record_api_call(endpoint="playlists.update", call_type="update")
            budget -= 1
        except Exception as exc:
            failures.append({"playlist_id": target_id, "error": str(exc)})

    if already_present:
        record_savings("dedup", already_present, already_present * WRITE_UNIT_COST)
    invalidate_playlist_cache(credentials)
    return {
        "success": len(failures) == 0 and skipped == 0,
        "failures": failures,
        "added": added,
        "alreadyPresent": already_present,
        "unavailable": unavailable,
        "skipped": skipped,
        "quotaBlocked": skipped > 0,
    }


@app.route("/playlist/<playlist_id>/rename", methods=["POST"])
def rename_playlist(playlist_id):
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    payload = request.get_json(silent=True) or {}
    new_name = payload.get("name")
    if new_name is not None:
        new_name = str(new_name).strip()
    if not new_name:
        return {"error": "New name is required"}, 400
    youtube, _ = get_youtube_client(credentials)
    try:
        # Preserve description and other snippet fields (update with part="snippet"
        # overwrites the whole snippet). Reuse the cached playlist snippet if we
        # have it, falling back to a 1-unit list read only when the cache is cold.
        current_snippet = None
        cached = get_cached_playlists(credentials)
        if cached:
            for playlist in cached:
                if playlist.get("id") == playlist_id:
                    current_snippet = dict(playlist.get("snippet") or {})
                    break
        if current_snippet is None:
            existing = youtube.playlists().list(
                part="snippet",
                id=playlist_id,
            ).execute()
            record_api_call(endpoint="playlists.list", call_type="list")
            items = existing.get("items", [])
            if not items:
                return {"error": "Playlist not found"}, 404
            current_snippet = items[0].get("snippet", {})
        current_snippet["title"] = new_name
        youtube.playlists().update(
            part="snippet",
            body={"id": playlist_id, "snippet": current_snippet},
        ).execute()
        record_api_call(endpoint="playlists.update", call_type="update")
        invalidate_playlist_cache(credentials)
    except Exception as exc:
        return {"error": str(exc)}, 400
    return {"success": True, "name": new_name}


@app.route("/playlist/<playlist_id>/import", methods=["POST"])
def import_videos(playlist_id):
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    payload = request.get_json(silent=True) or {}
    video_ids = payload.get("video_ids") or []
    if not isinstance(video_ids, list) or not video_ids:
        return {"error": "No video IDs provided"}, 400
    youtube, _ = get_youtube_client(credentials)
    # Skip videos already present in the destination (saves 50 units each).
    try:
        existing_video_ids = fetch_playlist_video_ids(youtube, playlist_id)
    except Exception as exc:
        return {
            "success": False,
            "added": 0,
            "error": "Destination playlist could not be read "
                     "(it may have been deleted): " + str(exc),
        }
    budget = remaining_write_budget()
    failures = []
    added = 0
    already_present = 0
    skipped = 0
    for video_id in video_ids:
        if not video_id or not isinstance(video_id, str):
            continue
        if video_id in existing_video_ids:
            already_present += 1
            continue
        if budget <= 0:
            skipped += 1
            continue
        try:
            youtube.playlistItems().insert(
                part="snippet",
                body={
                    "snippet": {
                        "playlistId": playlist_id,
                        "resourceId": {
                            "kind": "youtube#video",
                            "videoId": video_id,
                        },
                    }
                },
            ).execute()
            record_api_call(endpoint="playlistItems.insert", call_type="insert")
            added += 1
            budget -= 1
            existing_video_ids.add(video_id)
        except Exception as exc:
            failures.append({"videoId": video_id, "error": str(exc)})
    if already_present:
        record_savings("dedup", already_present, already_present * WRITE_UNIT_COST)
    if added:
        invalidate_playlist_cache(credentials)
    return {
        "success": len(failures) == 0 and skipped == 0,
        "added": added,
        "alreadyPresent": already_present,
        "skipped": skipped,
        "quotaBlocked": skipped > 0,
        "failures": failures,
    }


@app.route("/record-savings", methods=["POST"])
def record_savings_endpoint():
    """Record client-side savings (e.g. a client-side sort that avoided a refetch)."""
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    payload = request.get_json(silent=True) or {}
    if payload.get("source") != "sort":  # only the sort optimization is client-reported
        return {"error": "Unknown savings source"}, 400
    try:
        calls = int(payload.get("calls") or 0)
        units = int(payload.get("units") or 0)
    except (TypeError, ValueError, OverflowError):  # OverflowError: JSON Infinity
        return {"error": "Invalid amounts"}, 400
    # Clamp so a misbehaving client can't inflate the tally.
    calls = max(0, min(calls, 10000))
    units = max(0, min(units, 1000000))
    record_savings("sort", calls, units)
    return {"ok": True}


@app.route("/login")
def login():
    error_message = get_client_secrets_error()
    if error_message:
        return render_template("error.html", error_message=error_message)
    flow = get_flow()
    authorization_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    print(f"[OAuth] authorization_url = {authorization_url}", flush=True)
    session["state"] = state
    return redirect(authorization_url)


@app.route("/oauth2callback")
def oauth2callback():
    error_message = get_client_secrets_error()
    if error_message:
        return render_template("error.html", error_message=error_message)
    flow = get_flow(state=session.get("state"))
    authorization_response = request.url
    if IS_PRODUCTION:
        authorization_response = authorization_response.replace("http://", "https://", 1)
    flow.fetch_token(authorization_response=authorization_response)
    credentials = flow.credentials
    session["credentials"] = credentials_to_dict(credentials)
    save_credentials(credentials)
    return redirect(url_for("index"))


@app.route("/logout")
def logout():
    session.clear()
    if os.path.exists(TOKEN_FILE):
        try:
            os.remove(TOKEN_FILE)
        except OSError:
            pass
    return redirect(url_for("index"))


@app.route("/debug-oauth")
def debug_oauth():
    redirect_uri = url_for("oauth2callback", _external=True)
    if IS_PRODUCTION and redirect_uri.startswith("http://"):
        redirect_uri = redirect_uri.replace("http://", "https://", 1)
    return {
        "client_id": CLIENT_ID,
        "redirect_uri_auto": redirect_uri,
        "redirect_uri_override": REDIRECT_URI_OVERRIDE,
        "redirect_uri_used": REDIRECT_URI_OVERRIDE or redirect_uri,
        "is_production": IS_PRODUCTION,
        "request_scheme": request.scheme,
        "request_host": request.host,
        "hint": "Copy 'redirect_uri_used' and paste it EXACTLY into Google Cloud Console > Credentials > OAuth 2.0 Client ID > Authorized redirect URIs",
    }


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=3000, debug=True)
