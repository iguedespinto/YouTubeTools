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
TOKEN_FILE = (
    os.getenv("YT_TOKEN_FILE")
    or os.getenv("TOKEN_FILE")
    or "token.json"
)
API_CALL_QUOTA = os.getenv("YT_API_CALLS_QUOTA")
API_CALL_COUNT = 0

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
mongo_db = mongo_client.get_default_database()
api_logs_collection = mongo_db["api_logs"]

# On startup, delete logged records from previous days
today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
deleted = api_logs_collection.delete_many({"timestamp": {"$lt": today_start}})
if deleted.deleted_count > 0:
    print(f"[Startup] Deleted {deleted.deleted_count} API log record(s) from previous days.")

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev_secret_key_change_me")
if IS_PRODUCTION:
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)


@app.after_request
def inject_api_stats(response):
    """Inject apiCallsToday into every JSON response so the frontend can update stats."""
    if response.content_type and "application/json" in response.content_type:
        try:
            data = response.get_json()
            if isinstance(data, dict):
                data["apiCallsToday"] = get_api_calls_today()
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
    global API_CALL_COUNT
    API_CALL_COUNT += count
    api_logs_collection.insert_one({
        "timestamp": datetime.now(timezone.utc),
        "endpoint": endpoint,
        "type": call_type,
    })


def get_api_calls_today():
    today_start_utc = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return api_logs_collection.count_documents({"timestamp": {"$gte": today_start_utc}})


def get_youtube_client(credentials_dict):
    """Build a YouTube API client from stored credentials, refreshing if needed.

    Uses a static discovery document to avoid making a discovery HTTP request
    on every call (the googleapiclient makes one network call per build() by
    default, which inflates the request count seen in the Google Cloud console).
    """
    normalized = normalize_saved_credentials(credentials_dict) or credentials_dict
    creds = Credentials(**normalized)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        save_credentials(creds)
    return build("youtube", "v3", credentials=creds, cache_discovery=True), creds


def get_flow(state=None):
    redirect_uri = url_for("oauth2callback", _external=True)
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
        playlists = get_playlists(credentials)
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
    try:
        quota = int(API_CALL_QUOTA) if API_CALL_QUOTA else None
    except ValueError:
        quota = None
    api_calls_remaining = None
    if quota is not None:
        api_calls_remaining = max(quota - api_calls_today, 0)
    return render_template(
        "playlists.html",
        playlists=playlists,
        sort_by=sort_by,
        sort_order=sort_order,
        total_playlists=total_playlists,
        total_videos=total_videos,
        api_calls_used=api_calls_today,
        api_calls_remaining=api_calls_remaining,
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
    failures = []
    for playlist_id in playlist_ids:
        try:
            youtube.playlists().delete(id=playlist_id).execute()
            record_api_call(endpoint="playlists.delete", call_type="delete")
        except Exception as exc:
            failures.append({"id": playlist_id, "error": str(exc)})
    return {"success": len(failures) == 0, "failures": failures}


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
    response = api_request.execute()
    record_api_call(endpoint="playlistItems.list", call_type="list")
    simplified = []
    deleted_item_ids = []
    for item in response.get("items", []):
        snippet = item.get("snippet") or {}
        details = item.get("contentDetails") or {}
        title = snippet.get("title") or ""
        if title in ("Deleted video", "Private video"):
            deleted_item_ids.append(item.get("id"))
            continue
        simplified.append(
            {
                "playlistItemId": item.get("id"),
                "videoId": details.get("videoId"),
                "title": title,
                "thumbnail": ((snippet.get("thumbnails") or {}).get("default") or {}).get("url"),
            }
        )
    # Auto-remove deleted/private videos from the playlist in the background
    removed_count = 0
    for pid in deleted_item_ids:
        try:
            youtube.playlistItems().delete(id=pid).execute()
            record_api_call(endpoint="playlistItems.delete", call_type="delete")
            removed_count += 1
        except Exception:
            pass
    return {
        "items": simplified,
        "nextPageToken": response.get("nextPageToken"),
        "autoRemoved": removed_count,
    }


@app.route("/playlist/<playlist_id>/dedupe", methods=["POST"])
def dedupe_playlist(playlist_id):
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    youtube, _ = get_youtube_client(credentials)
    items = []
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
    for item_id in duplicate_item_ids:
        try:
            youtube.playlistItems().delete(id=item_id).execute()
            record_api_call(endpoint="playlistItems.delete", call_type="delete")
        except Exception as exc:
            failures.append({"id": item_id, "error": str(exc)})
    removed_count = len(duplicate_item_ids) - len(failures)
    remaining_count = len(items) - removed_count
    return {
        "success": len(failures) == 0,
        "removed": removed_count,
        "duplicates": len(duplicate_item_ids),
        "remaining": remaining_count,
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
    failures = []
    for item_id in item_ids:
        try:
            youtube.playlistItems().delete(id=item_id).execute()
            record_api_call(endpoint="playlistItems.delete", call_type="delete")
        except Exception as exc:
            failures.append({"id": item_id, "error": str(exc)})
    return {"success": len(failures) == 0, "failures": failures}


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
    failures = []
    added = 0
    delete_after = []
    for item in items:
        if not isinstance(item, dict):
            continue
        video_id = item.get("videoId")
        playlist_item_id = item.get("playlistItemId")
        if not video_id:
            failures.append({"videoId": video_id, "error": "Missing videoId"})
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
            if mode == "move" and playlist_item_id:
                delete_after.append(playlist_item_id)
        except Exception as exc:
            failures.append({"videoId": video_id, "error": str(exc)})
    removed = 0
    if mode == "move" and delete_after:
        for item_id in delete_after:
            try:
                youtube.playlistItems().delete(id=item_id).execute()
                record_api_call(endpoint="playlistItems.delete", call_type="delete")
                removed += 1
            except Exception as exc:
                failures.append({"id": item_id, "error": str(exc)})
    return {
        "success": len(failures) == 0,
        "added": added,
        "removed": removed,
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

    failures = []
    added = 0
    for playlist_id in source_ids:
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
                except Exception as exc:
                    failures.append({"playlist_id": playlist_id, "video_id": video_id, "error": str(exc)})
            request_page = youtube.playlistItems().list_next(request_page, response)

    for playlist_id in source_ids:
        try:
            youtube.playlists().delete(id=playlist_id).execute()
            record_api_call(endpoint="playlists.delete", call_type="delete")
        except Exception as exc:
            failures.append({"playlist_id": playlist_id, "error": str(exc)})

    if new_name:
        try:
            youtube.playlists().update(
                part="snippet",
                body={"id": target_id, "snippet": {"title": new_name}},
            ).execute()
            record_api_call(endpoint="playlists.update", call_type="update")
        except Exception as exc:
            failures.append({"playlist_id": target_id, "error": str(exc)})

    return {"success": len(failures) == 0, "failures": failures, "added": added}


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
        # Fetch existing snippet so we preserve description and other fields
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
    failures = []
    added = 0
    for video_id in video_ids:
        if not video_id or not isinstance(video_id, str):
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
        except Exception as exc:
            failures.append({"videoId": video_id, "error": str(exc)})
    return {"success": len(failures) == 0, "added": added, "failures": failures}


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
    session["state"] = state
    return redirect(authorization_url)


@app.route("/oauth2callback")
def oauth2callback():
    error_message = get_client_secrets_error()
    if error_message:
        return render_template("error.html", error_message=error_message)
    flow = get_flow(state=session.get("state"))
    flow.fetch_token(authorization_response=request.url)
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


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=3000, debug=True)
