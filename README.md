# YouTube Tools - Playlists Viewer

Simple Python web app that connects to YouTube and lists your playlists.

## Setup

1) Create OAuth credentials in Google Cloud Console:
   - OAuth consent screen
   - OAuth client ID (Web application)
   - Authorized redirect URI: `http://localhost:5000/oauth2callback`
2) Download the client secrets JSON and save it as `client_secret.json` in this folder.
3) Create a virtualenv and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
export OAUTHLIB_INSECURE_TRANSPORT=1
FLASK_SECRET_KEY="change_me" python app.py
```

Then open `http://127.0.0.1:3000` and connect your YouTube account.

## Tests

Pure helpers live in `yt_lib.py` (no Flask/Mongo imports) so they can be tested
without a database:

```bash
pip install -r requirements-dev.txt
pytest
```

## Notes

- You can set a custom path for the client secrets file using `YT_CLIENT_SECRETS`.
- `CLIENT_ID` / `CLIENT_SECRET` are also supported (compatible with NightGuardian).
- **Tokens are stored durably in MongoDB** (collection `oauth_tokens`, one document per
  `YT_TOKEN_DOC_ID`, default `"default"`). This survives Heroku dyno restarts, where the
  filesystem is ephemeral. A local `token.json` is still written/read as a dev fallback
  and for one-time migration; override its path with `YT_TOKEN_FILE`.
- The access token is refreshed automatically (proactively, just before expiry), so once
  you connect an account the integration runs unattended. **For truly unattended use the
  Google OAuth consent screen must be "Published / In production"** — in "Testing" status
  Google expires refresh tokens after 7 days regardless of where they're stored.
- **Auth model:** the stored token is never served anonymously. The web UI requires an
  OAuth-established session (sign in via "Connect"); the JSON `/api/*` and all write
  endpoints require a valid `Authorization: Bearer <key>` (mint keys with
  `scripts/mint_api_key.py`) — used by the MCP server. Requests with neither get `401`.
# YouTubeTools
