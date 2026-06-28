#!/usr/bin/env python3
"""CLI tool to mint, list, and revoke API keys for MCP Bearer auth.

Usage:
    python -m scripts.mint_api_key --name <label>   # Mint a new key
    python -m scripts.mint_api_key --list           # List all keys (metadata only)
    python -m scripts.mint_api_key --revoke <key_id> # Revoke a key
"""

import argparse
import hashlib
import os
import secrets
import sys
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv:
    load_dotenv()

from pymongo import MongoClient

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
if not MONGODB_CONNECTION_STRING:
    print("Error: MONGODB_CONNECTION_STRING environment variable not set.", file=sys.stderr)
    sys.exit(1)

mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
mongo_db = mongo_client.get_default_database()
api_keys_collection = mongo_db["api_keys"]

PREFIX = "yt_live_"


def hash_api_key(plaintext: str) -> str:
    """Return SHA-256 hex digest of the plaintext API key."""
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


def mint_key(name: str) -> str:
    """Generate a new API key, store its hash, and return the plaintext key."""
    # Generate 32 random bytes → 64 hex chars
    random_part = secrets.token_hex(32)
    plaintext = f"{PREFIX}{random_part}"
    key_hash = hash_api_key(plaintext)
    # Store only the first 8 chars of the random part as prefix for identification
    visible_prefix = f"{PREFIX}{random_part[:8]}..."

    doc = {
        "name": name,
        "key_hash": key_hash,
        "prefix": visible_prefix,
        "created_at": datetime.now(timezone.utc),
        "last_used_at": None,
        "revoked": False,
    }
    result = api_keys_collection.insert_one(doc)
    print(f"Created API key: {name}")
    print(f"  ID: {result.inserted_id}")
    print(f"  Prefix: {visible_prefix}")
    print()
    print("IMPORTANT: Copy this key now. It will not be shown again.")
    print(f"  Key: {plaintext}")
    return plaintext


def list_keys():
    """List all non-revoked API keys (metadata only)."""
    keys = list(api_keys_collection.find({"revoked": False}))
    if not keys:
        print("No active API keys found.")
        return

    print(f"{'ID':<26} {'Name':<20} {'Prefix':<25} {'Created':<20} {'Last Used'}")
    print("-" * 120)
    for doc in keys:
        key_id = str(doc["_id"])
        name = doc.get("name", "")[:20]
        prefix = doc.get("prefix", "")[:25]
        created = doc.get("created_at")
        if created:
            created = created.strftime("%Y-%m-%d %H:%M")
        else:
            created = "-"
        last_used = doc.get("last_used_at")
        if last_used:
            last_used = last_used.strftime("%Y-%m-%d %H:%M")
        else:
            last_used = "Never"
        print(f"{key_id:<26} {name:<20} {prefix:<25} {created:<20} {last_used}")


def revoke_key(key_id: str):
    """Revoke an API key by its ID."""
    from bson import ObjectId
    try:
        oid = ObjectId(key_id)
    except Exception:
        print(f"Error: Invalid key ID format: {key_id}", file=sys.stderr)
        sys.exit(1)

    result = api_keys_collection.update_one(
        {"_id": oid, "revoked": False},
        {"$set": {"revoked": True, "revoked_at": datetime.now(timezone.utc)}},
    )
    if result.modified_count == 0:
        print(f"Error: Key not found or already revoked: {key_id}", file=sys.stderr)
        sys.exit(1)
    print(f"Revoked API key: {key_id}")


def main():
    parser = argparse.ArgumentParser(
        description="Manage API keys for MCP Bearer authentication."
    )
    parser.add_argument(
        "--name",
        type=str,
        help="Mint a new API key with this label",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        dest="list_keys",
        help="List all active API keys (metadata only)",
    )
    parser.add_argument(
        "--revoke",
        type=str,
        metavar="KEY_ID",
        help="Revoke an API key by its ID",
    )

    args = parser.parse_args()

    if args.name:
        mint_key(args.name)
    elif args.list_keys:
        list_keys()
    elif args.revoke:
        revoke_key(args.revoke)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
