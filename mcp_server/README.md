# YouTubeTools MCP Server

An MCP (Model Context Protocol) server that exposes YouTubeTools functionality to AI assistants like Claude.

## Prerequisites

- Python 3.10+
- A running YouTubeTools Flask app (local or deployed)
- An API key minted via `scripts/mint_api_key.py`

## Installation

```bash
cd mcp_server
pip install -r requirements.txt
```

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `YT_BASE_URL` | Base URL of the Flask app | `http://localhost:3000` or `https://your-app.herokuapp.com` |
| `YT_API_KEY` | API key for Bearer auth | `yt_live_abc123...` |

## Minting an API Key

From the project root (with `MONGODB_CONNECTION_STRING` set):

```bash
# Mint a new key
python -m scripts.mint_api_key --name "claude-code"

# List all keys
python -m scripts.mint_api_key --list

# Revoke a key
python -m scripts.mint_api_key --revoke <key_id>
```

The key is only shown once when minted. Store it securely.

## Running the Server

```bash
YT_BASE_URL=http://localhost:3000 YT_API_KEY=yt_live_xxx python mcp_server/server.py
```

## Claude Code CLI Registration

```bash
claude mcp add youtube-tools \
  --env YT_BASE_URL=https://your-app.herokuapp.com \
  --env YT_API_KEY=yt_live_xxx \
  -- python /absolute/path/to/mcp_server/server.py
```

## Claude Desktop Configuration

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "youtube-tools": {
      "command": "python",
      "args": ["/absolute/path/to/mcp_server/server.py"],
      "env": {
        "YT_BASE_URL": "https://your-app.herokuapp.com",
        "YT_API_KEY": "yt_live_xxx"
      }
    }
  }
}
```

## Available Tools

### Read Operations (1 quota unit each)

| Tool | Description |
|------|-------------|
| `list_playlists` | List all user playlists with metadata |
| `get_playlist_items` | Fetch videos in a playlist (paginated) |
| `get_quota_status` | Check remaining API budget and savings stats |

### Write Operations (50 quota units each)

| Tool | Description |
|------|-------------|
| `rename_playlist` | Update playlist title |
| `delete_playlist` | Delete a single playlist |
| `delete_playlists` | Bulk delete multiple playlists |
| `cleanup_playlist` | Remove deleted/private videos |
| `dedupe_playlist` | Remove duplicate videos |
| `import_videos` | Add videos to a playlist by ID |
| `transfer_items` | Copy/move videos between playlists |
| `merge_playlists` | Combine multiple playlists into one |

## Testing

1. **Test Bearer auth locally:**
   ```bash
   curl -H "Authorization: Bearer yt_live_xxx" http://localhost:3000/api/playlists
   curl -H "Authorization: Bearer yt_live_xxx" http://localhost:3000/api/quota
   ```

2. **Test MCP tools in Claude Code:**
   - Use `list_playlists` to verify connection
   - Use `get_quota_status` to check quota
   - Use `rename_playlist` to test a write operation

## Security Notes

- API keys are stored as SHA-256 hashes in MongoDB (never in plaintext)
- Bearer auth only works on explicitly allowed endpoints
- Each key tracks `last_used_at` for auditing
- Revoked keys are immediately rejected
