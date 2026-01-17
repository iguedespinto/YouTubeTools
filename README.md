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

## Notes

- You can set a custom path for the client secrets file using `YT_CLIENT_SECRETS`.
- `CLIENT_ID` / `CLIENT_SECRET` are also supported (compatible with NightGuardian).
- Tokens are stored in `token.json` by default. Set `YT_TOKEN_FILE` to override.
# YouTubeTools
