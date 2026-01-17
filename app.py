import json
import os
from datetime import datetime, timezone

from flask import Flask, redirect, render_template, request, session, url_for
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/youtube"]
try:
    from dotenv import load_dotenv
except ImportError:  # optional
    load_dotenv = None

if load_dotenv:
    load_dotenv()

if not os.getenv("OAUTHLIB_INSECURE_TRANSPORT"):
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

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev_secret_key_change_me")


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
        with open(TOKEN_FILE, "w", encoding="utf-8") as token_file:
            json.dump(credentials_to_dict(credentials), token_file)
    except OSError:
        return


def get_playlists(credentials_dict):
    normalized = normalize_saved_credentials(credentials_dict) or credentials_dict
    credentials = Credentials(**normalized)
    if credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
        save_credentials(credentials)
    youtube = build("youtube", "v3", credentials=credentials)
    playlists = []
    request_page = youtube.playlists().list(
        part="snippet,contentDetails",
        mine=True,
        maxResults=50,
    )
    while request_page is not None:
        response = request_page.execute()
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
    return render_template(
        "playlists.html",
        playlists=playlists,
        sort_by=sort_by,
        sort_order=sort_order,
        total_playlists=total_playlists,
        total_videos=total_videos,
    )


@app.route("/delete/<playlist_id>", methods=["POST"])
def delete_playlist(playlist_id):
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return redirect(url_for("index"))
    normalized = normalize_saved_credentials(credentials) or credentials
    creds = Credentials(**normalized)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        save_credentials(creds)
    youtube = build("youtube", "v3", credentials=creds)
    try:
        youtube.playlists().delete(id=playlist_id).execute()
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
    normalized = normalize_saved_credentials(credentials) or credentials
    creds = Credentials(**normalized)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        save_credentials(creds)
    youtube = build("youtube", "v3", credentials=creds)
    failures = []
    for playlist_id in playlist_ids:
        try:
            youtube.playlists().delete(id=playlist_id).execute()
        except Exception as exc:
            failures.append({"id": playlist_id, "error": str(exc)})
    return {"success": len(failures) == 0, "failures": failures}


@app.route("/playlist/<playlist_id>/items")
def playlist_items(playlist_id):
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    normalized = normalize_saved_credentials(credentials) or credentials
    creds = Credentials(**normalized)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        save_credentials(creds)
    youtube = build("youtube", "v3", credentials=creds)
    items = []
    request_page = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=50,
    )
    while request_page is not None:
        response = request_page.execute()
        items.extend(response.get("items", []))
        request_page = youtube.playlistItems().list_next(request_page, response)
    simplified = []
    for item in items:
        snippet = item.get("snippet") or {}
        details = item.get("contentDetails") or {}
        simplified.append(
            {
                "playlistItemId": item.get("id"),
                "videoId": details.get("videoId"),
                "title": snippet.get("title"),
                "thumbnail": ((snippet.get("thumbnails") or {}).get("default") or {}).get("url"),
            }
        )
    return {"items": simplified}


@app.route("/playlist/<playlist_id>/items/delete-bulk", methods=["POST"])
def delete_playlist_items_bulk(playlist_id):
    credentials = session.get("credentials") or load_saved_credentials()
    if not credentials:
        return {"error": "Not authenticated"}, 401
    payload = request.get_json(silent=True) or {}
    item_ids = payload.get("playlist_item_ids") or []
    if not isinstance(item_ids, list) or not item_ids:
        return {"error": "No items provided"}, 400
    normalized = normalize_saved_credentials(credentials) or credentials
    creds = Credentials(**normalized)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        save_credentials(creds)
    youtube = build("youtube", "v3", credentials=creds)
    failures = []
    for item_id in item_ids:
        try:
            youtube.playlistItems().delete(id=item_id).execute()
        except Exception as exc:
            failures.append({"id": item_id, "error": str(exc)})
    return {"success": len(failures) == 0, "failures": failures}


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
