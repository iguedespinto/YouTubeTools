import hashlib
import json
import os
import random
import re
from datetime import datetime, timezone, timedelta

from flask import Flask, g, redirect, render_template, request, session, url_for
from werkzeug.middleware.proxy_fix import ProxyFix
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
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
# Single-account OAuth token store. The credentials live under this _id in
# MongoDB so they survive Heroku dyno restarts (the dyno filesystem is
# ephemeral and silently loses a token file on every restart/deploy).
# Re-authenticating just overwrites this one document.
TOKEN_DOC_ID = os.getenv("YT_TOKEN_DOC_ID", "default")
# Refresh the access token this many seconds before it actually expires, so a
# long-running, system-to-system call never fires with a token about to lapse.
TOKEN_REFRESH_BUFFER = timedelta(seconds=120)
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
# Durable OAuth token store (see TOKEN_DOC_ID). Survives dyno restarts.
oauth_tokens_collection = mongo_db["oauth_tokens"]
# API keys for MCP Bearer auth (hashed, never stored in plaintext).
api_keys_collection = mongo_db["api_keys"]

# Index the timestamp so the daily quota/savings tallies are index-only scans.
try:
    api_logs_collection.create_index("timestamp")
    savings_log_collection.create_index("timestamp")
    api_keys_collection.create_index("key_hash", unique=True)
except Exception:
    pass

# Endpoints that accept Bearer token authentication (for MCP clients).
# Tuples of (HTTP method, Flask route pattern).
BEARER_AUTH_ENDPOINTS = frozenset([
    # Read
    ("GET", "/api/playlists"),
    ("GET", "/api/quota"),
    ("GET", "/api/random-videos"),
    ("GET", "/playlist/<playlist_id>/items"),
    # Write
    ("POST", "/delete/<playlist_id>"),
    ("POST", "/delete-bulk"),
    ("POST", "/playlist/<playlist_id>/rename"),
    ("POST", "/playlist/<playlist_id>/cleanup"),
    ("POST", "/playlist/<playlist_id>/dedupe"),
    ("POST", "/playlist/<playlist_id>/import"),
    ("POST", "/playlist/<playlist_id>/items/delete-bulk"),
    ("POST", "/playlist/<playlist_id>/items/transfer"),
    ("POST", "/merge-playlists"),
])

# On startup, delete logged records from previous days
today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
deleted = api_logs_collection.delete_many({"timestamp": {"$lt": today_start}})
if deleted.deleted_count > 0:
    print(f"[Startup] Deleted {deleted.deleted_count} API log record(s) from previous days.")
savings_log_collection.delete_many({"timestamp": {"$lt": today_start}})

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev_secret_key_change_me")
# Keep the owner's browser login durable so requiring a session (instead of the
# old anonymous auto-login) doesn't force a re-login on every browser restart.
app.permanent_session_lifetime = timedelta(days=30)
if IS_PRODUCTION:
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    app.config["PREFERRED_URL_SCHEME"] = "https"


@app.before_request
def check_bearer_auth():
    """Authenticate Bearer tokens for allowed API endpoints.

    If the request has a valid Bearer token and hits an allowed endpoint,
    store the resolved credentials in g.bearer_credentials so route handlers
    can use them instead of session credentials.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return  # Not a Bearer request; continue to normal session auth
    # Bearer token present — check if this endpoint accepts it
    if not is_bearer_auth_allowed(request.method, request.path):
        return {"error": "Bearer auth not allowed for this endpoint"}, 403
    credentials = get_bearer_user(request)
    if not credentials:
        return {"error": "Invalid or expired API key"}, 401
    g.bearer_credentials = credentials
    # Mark API key as used (async-safe, fire-and-forget)
    if hasattr(g, "api_key_doc") and g.api_key_doc:
        mark_api_key_used(str(g.api_key_doc["_id"]))


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


# ---------------------------------------------------------------------------
# API Key Management (Bearer auth for MCP clients)
# ---------------------------------------------------------------------------

def hash_api_key(plaintext):
    """Return SHA-256 hex digest of the plaintext API key."""
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


def create_api_key(name, key_hash, prefix):
    """Store a new API key (hashed). Returns the inserted document ID."""
    doc = {
        "name": name,
        "key_hash": key_hash,
        "prefix": prefix,
        "created_at": datetime.now(timezone.utc),
        "last_used_at": None,
        "revoked": False,
    }
    result = api_keys_collection.insert_one(doc)
    return str(result.inserted_id)


def get_api_key_by_hash(key_hash):
    """Lookup an API key by its hash. Returns the doc or None."""
    return api_keys_collection.find_one({"key_hash": key_hash, "revoked": False})


def mark_api_key_used(key_id):
    """Update last_used_at timestamp for the given key."""
    from bson import ObjectId
    try:
        api_keys_collection.update_one(
            {"_id": ObjectId(key_id)},
            {"$set": {"last_used_at": datetime.now(timezone.utc)}},
        )
    except Exception:
        pass


def revoke_api_key(key_id):
    """Mark an API key as revoked."""
    from bson import ObjectId
    api_keys_collection.update_one(
        {"_id": ObjectId(key_id)},
        {"$set": {"revoked": True, "revoked_at": datetime.now(timezone.utc)}},
    )


def list_api_keys():
    """Return all API keys (metadata only, never the hash)."""
    keys = []
    for doc in api_keys_collection.find({"revoked": False}):
        keys.append({
            "id": str(doc["_id"]),
            "name": doc.get("name"),
            "prefix": doc.get("prefix"),
            "created_at": doc.get("created_at"),
            "last_used_at": doc.get("last_used_at"),
        })
    return keys


def is_bearer_auth_allowed(method, path):
    """Check if the given method+path accepts Bearer token auth."""
    # Check exact matches first
    if (method, path) in BEARER_AUTH_ENDPOINTS:
        return True
    # Check pattern matches (convert path params like /playlist/ABC/items to /playlist/<playlist_id>/items)
    for allowed_method, allowed_pattern in BEARER_AUTH_ENDPOINTS:
        if method != allowed_method:
            continue
        # Convert pattern to regex: <param> → [^/]+
        regex_pattern = re.sub(r"<[^>]+>", r"[^/]+", allowed_pattern)
        if re.fullmatch(regex_pattern, path):
            return True
    return False


def get_bearer_user(req):
    """Extract and validate Bearer token from Authorization header.

    Returns the credentials dict if valid, None otherwise.
    Sets g.api_key_doc if auth succeeds (for marking usage).
    """
    auth_header = req.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header[7:].strip()
    if not token:
        return None
    key_hash = hash_api_key(token)
    key_doc = get_api_key_by_hash(key_hash)
    if not key_doc:
        return None
    # Bearer auth uses the single stored OAuth token (single-account mode).
    credentials = load_saved_credentials()
    if not credentials:
        return None
    g.api_key_doc = key_doc
    return credentials


def get_youtube_client(credentials_dict):
    """Build a YouTube API client from stored credentials, refreshing if needed.

    Uses the static discovery document bundled with google-api-python-client
    (static_discovery=True, the default in v2+), so build() makes no network
    request. Note: discovery fetches never count against the YouTube Data API
    quota regardless — only actual youtube/v3 method calls do.
    """
    normalized = normalize_saved_credentials(credentials_dict) or credentials_dict
    creds = Credentials(**normalized)
    if _needs_refresh(creds) and creds.refresh_token:
        try:
            creds.refresh(Request())
        except RefreshError:
            # The refresh token is dead — revoked, or expired because the OAuth
            # consent screen is still in "Testing" (those expire after 7 days).
            # Drop the stored token so the UI prompts a fresh Connect.
            clear_saved_credentials()
            raise
        save_credentials(creds)
    return build("youtube", "v3", credentials=creds, static_discovery=True), creds


def _needs_refresh(creds):
    """True if the token is missing/expired or within the pre-expiry buffer."""
    if not creds.token:
        return False
    if creds.expired:
        return True
    if creds.expiry is None:
        return False
    # creds.expiry is a naive UTC datetime; compare against naive UTC now.
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    return creds.expiry <= now_utc + TOKEN_REFRESH_BUFFER


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


def _load_credentials_from_mongo():
    try:
        doc = oauth_tokens_collection.find_one({"_id": TOKEN_DOC_ID})
    except Exception:
        return None
    if not doc:
        return None
    doc.pop("_id", None)
    doc.pop("updated_at", None)
    return doc


def _load_credentials_from_file():
    if not os.path.exists(TOKEN_FILE):
        return None
    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as token_file:
            return json.load(token_file)
    except (OSError, json.JSONDecodeError):
        return None


def load_saved_credentials():
    # Mongo is the durable store (survives Heroku dyno restarts). Fall back to a
    # local token file for development and for one-time migration of an existing
    # file-based token: the next save_credentials() writes it through to Mongo.
    data = _load_credentials_from_mongo() or _load_credentials_from_file()
    return normalize_saved_credentials(data) if data else None


def _save_credentials_to_mongo(data):
    try:
        doc = dict(data)
        doc["_id"] = TOKEN_DOC_ID
        doc["updated_at"] = datetime.now(timezone.utc)
        oauth_tokens_collection.replace_one({"_id": TOKEN_DOC_ID}, doc, upsert=True)
    except Exception:
        pass


def _save_credentials_to_file(data):
    try:
        token_dir = os.path.dirname(TOKEN_FILE)
        if token_dir:
            os.makedirs(token_dir, exist_ok=True)
        with open(TOKEN_FILE, "w", encoding="utf-8") as token_file:
            json.dump(data, token_file)
    except OSError:
        return


def save_credentials(credentials):
    existing = load_saved_credentials() or {}
    data = credentials_to_dict(credentials)
    # A refresh response usually omits these; carry the saved values forward so
    # the long-lived refresh token (and client config) is never lost on refresh.
    for field in ("refresh_token", "token_uri", "client_id", "client_secret"):
        if not data.get(field) and existing.get(field):
            data[field] = existing.get(field)
    _save_credentials_to_mongo(data)
    _save_credentials_to_file(data)


def clear_saved_credentials():
    """Remove the stored token everywhere (used on logout or a dead refresh token)."""
    try:
        oauth_tokens_collection.delete_one({"_id": TOKEN_DOC_ID})
    except Exception:
        pass
    if os.path.exists(TOKEN_FILE):
        try:
            os.remove(TOKEN_FILE)
        except OSError:
            pass


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


def best_thumbnail_url(snippet):
    """Pick the best available thumbnail URL from a snippet's thumbnails."""
    thumbs = (snippet or {}).get("thumbnails") or {}
    for size in ("maxres", "standard", "high", "medium", "default"):
        url = (thumbs.get(size) or {}).get("url")
        if url:
            return url
    return None


def fetch_all_playlist_videos(youtube, playlist_id):
    """Return every (live) video in a playlist, paginating through ALL pages.

    Skips deleted/private videos. Costs 1 quota unit per 50 items.
    Returns a list of dicts: {videoId, title, thumbnailUrl}.
    """
    videos = []
    request_page = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=50,
    )
    while request_page is not None:
        response = request_page.execute()
        record_api_call(endpoint="playlistItems.list", call_type="list")
        for item in response.get("items", []):
            snippet = item.get("snippet") or {}
            details = item.get("contentDetails") or {}
            title = snippet.get("title") or ""
            video_id = details.get("videoId")
            if not video_id or title in ("Deleted video", "Private video"):
                continue
            videos.append({
                "videoId": video_id,
                "title": title,
                "thumbnailUrl": best_thumbnail_url(snippet),
            })
        request_page = youtube.playlistItems().list_next(request_page, response)
    return videos


def find_playlist_id_by_name(credentials_dict, name):
    """Resolve a playlist title to its id (case-insensitive, first match).

    Returns (playlist_id, available_titles). playlist_id is None if not found,
    in which case available_titles lists the user's playlist names for the error.
    Reuses the cached playlist list to avoid an extra quota hit when possible.
    """
    playlists = get_cached_playlists(credentials_dict)
    if playlists is None:
        playlists = get_playlists(credentials_dict)
        set_cached_playlists(credentials_dict, playlists)
    target = (name or "").strip().casefold()
    titles = []
    for p in playlists:
        if p.get("id") in IGNORED_PLAYLIST_IDS:
            continue
        title = (p.get("snippet") or {}).get("title") or ""
        titles.append(title)
        if title.strip().casefold() == target:
            return p.get("id"), titles
    return None, titles


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


def get_credentials_from_request():
    """Return credentials from Bearer auth (g.bearer_credentials) or session.

    Does NOT fall back to the stored token for unauthenticated callers: doing so
    would let anyone who knows the public URL act on the owner's account. The
    stored token is reachable only via a valid Bearer key (see get_bearer_user)
    or an authenticated browser session established through the OAuth login flow.
    """
    if hasattr(g, "bearer_credentials") and g.bearer_credentials:
        return g.bearer_credentials
    return session.get("credentials")


def is_api_request():
    """Check if the request expects a JSON response (API/MCP client)."""
    if hasattr(g, "bearer_credentials") and g.bearer_credentials:
        return True
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return True
    accept = request.headers.get("Accept", "")
    if "application/json" in accept:
        return True
    return False


# ---------------------------------------------------------------------------
# API Endpoints (JSON-only, for MCP clients)
# ---------------------------------------------------------------------------

@app.route("/api/playlists")
def api_playlists():
    """Return playlist list as JSON for MCP clients."""
    credentials = get_credentials_from_request()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    try:
        playlists = get_cached_playlists(credentials)
        if playlists is None:
            playlists = get_playlists(credentials)
            set_cached_playlists(credentials, playlists)
        else:
            pages = max(1, (len(playlists) + 49) // 50)
            record_savings("cache", pages, pages)
        if IGNORED_PLAYLIST_IDS:
            playlists = [p for p in playlists if p.get("id") not in IGNORED_PLAYLIST_IDS]
        # Simplify for MCP: return id, title, url, itemCount
        simplified = []
        for p in playlists:
            playlist_id = p.get("id")
            simplified.append({
                "id": playlist_id,
                "title": (p.get("snippet") or {}).get("title"),
                "url": f"https://www.youtube.com/playlist?list={playlist_id}" if playlist_id else None,
                "description": (p.get("snippet") or {}).get("description"),
                "itemCount": (p.get("contentDetails") or {}).get("itemCount", 0),
                "thumbnailUrl": (((p.get("snippet") or {}).get("thumbnails") or {}).get("default") or {}).get("url"),
            })
        return {"playlists": simplified}
    except Exception as exc:
        return {"error": str(exc)}, 500


@app.route("/api/quota")
def api_quota():
    """Return quota status as JSON for MCP clients."""
    credentials = get_credentials_from_request()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    quota_limit = get_quota_limit()
    units_used = get_api_calls_today()
    units_remaining = max(quota_limit - units_used, 0)
    saved = get_savings_today()
    return {
        "quotaLimit": quota_limit,
        "unitsUsed": units_used,
        "unitsRemaining": units_remaining,
        "writeOperationsRemaining": units_remaining // WRITE_UNIT_COST,
        "unitsSavedToday": saved["units"],
        "callsAvertedToday": saved["calls"],
        "savingsBreakdown": saved["by_source"],
    }


@app.route("/api/random-videos")
def api_random_videos():
    """Return N random videos from a named playlist, sampled across ALL its videos.

    Query params:
        playlist: the playlist's title (case-insensitive).
        count:    how many random videos to return (default 5).

    The whole playlist is paginated first, then a uniform random sample is taken,
    so the result is drawn from every video, not just the first page.
    """
    credentials = get_credentials_from_request()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    name = (request.args.get("playlist") or "").strip()
    if not name:
        return {"error": "Missing 'playlist' (playlist name)"}, 400
    try:
        count = int(request.args.get("count", "5"))
    except (TypeError, ValueError):
        return {"error": "'count' must be an integer"}, 400
    if count <= 0:
        return {"error": "'count' must be a positive integer"}, 400

    playlist_id, available = find_playlist_id_by_name(credentials, name)
    if not playlist_id:
        return {"error": f"No playlist named '{name}'", "availablePlaylists": available}, 404

    try:
        youtube, _ = get_youtube_client(credentials)
        all_videos = fetch_all_playlist_videos(youtube, playlist_id)
    except Exception as exc:
        return {"error": str(exc)}, 500

    k = min(count, len(all_videos))
    chosen = random.sample(all_videos, k) if k else []
    videos = [
        {
            "title": v["title"],
            "url": f"https://www.youtube.com/watch?v={v['videoId']}",
            "thumbnailUrl": v["thumbnailUrl"],
        }
        for v in chosen
    ]
    return {
        "playlist": name,
        "playlistId": playlist_id,
        "requested": count,
        "totalAvailable": len(all_videos),
        "returned": len(videos),
        "videos": videos,
    }


@app.route("/")
def index():
    # The browser is authorized only by an OAuth-established session. We do NOT
    # auto-log-in from the stored token here: that would seed any anonymous
    # visitor's session with the owner's credentials (and thus write access).
    credentials = session.get("credentials")
    if not credentials:
        return render_template("index.html")
    try:
        # A browser refresh (F5 / reload) sends "Cache-Control: max-age=0"
        # (hard reload: "no-cache"). Treat that as an explicit "show me the
        # current list" and bypass the cache so a refresh always reflects reality.
        # Ordinary navigations still reuse the cache to save quota.
        cache_control = (request.headers.get("Cache-Control") or "").lower()
        force_refresh = "no-cache" in cache_control or "max-age=0" in cache_control
        playlists = None if force_refresh else get_cached_playlists(credentials)
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
    credentials = get_credentials_from_request()
    if not credentials:
        if is_api_request():
            return {"error": "Not authenticated"}, 401
        return redirect(url_for("index"))
    youtube, _ = get_youtube_client(credentials)
    try:
        youtube.playlists().delete(id=playlist_id).execute()
        record_api_call(endpoint="playlists.delete", call_type="delete")
        invalidate_playlist_cache(credentials)
    except Exception as exc:
        if is_api_request():
            return {"error": str(exc)}, 400
        return render_template("error.html", error_message=str(exc))
    if is_api_request():
        return {"success": True}
    sort_by = request.args.get("sort", "title")
    sort_order = request.args.get("order", "asc")
    return redirect(url_for("index", sort=sort_by, order=sort_order))


@app.route("/delete-bulk", methods=["POST"])
def delete_bulk():
    credentials = get_credentials_from_request()
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
    credentials = get_credentials_from_request()
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
    credentials = get_credentials_from_request()
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
    credentials = get_credentials_from_request()
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
    credentials = get_credentials_from_request()
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
    credentials = get_credentials_from_request()
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
    credentials = get_credentials_from_request()
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
    credentials = get_credentials_from_request()
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
    credentials = get_credentials_from_request()
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
    credentials = get_credentials_from_request()
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
    session.permanent = True  # honor permanent_session_lifetime (30 days)
    session["credentials"] = credentials_to_dict(credentials)
    save_credentials(credentials)
    return redirect(url_for("index"))


@app.route("/logout")
def logout():
    session.clear()
    clear_saved_credentials()
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
