#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.11"
# dependencies = ["discord.py>=2.3,<3"]
# ///

"""
Download Discord media (images & videos) from a guild within a date range.

Usage:
  ./download_discord_media.py --date-start 2025-11-05
  ./download_discord_media.py --date-start 2025-11-05 --date-end 2025-11-09

Environment variables (required):
  DISCORD_TOKEN  - Bot token
  GUILD_ID       - Numeric guild (server) ID the bot can access

Notes:
  - Date/times are treated in UTC. The date range is inclusive of the whole day(s).
  - Requires the bot to have "Read Message History" and "View Channel" on target channels.
  - Requires the "Message Content Intent" to be enabled in the Discord Developer Portal for the bot.
"""

import argparse
import asyncio
import hashlib
import os
import sqlite3
import warnings
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

import discord

# Suppress aiohttp unclosed connector warnings (common with discord.py)
warnings.filterwarnings("ignore", message="Unclosed connector", category=ResourceWarning)

# ---------- Config ----------
DOWNLOAD_DIR = Path("discord-downloads")
DB_PATH = Path("discord-downloads.db")


def parse_args():
    p = argparse.ArgumentParser(description="Download Discord media in a date range (UTC).")
    p.add_argument("--date-start", required=True, help="Start date (YYYY-MM-DD), inclusive.")
    p.add_argument("--date-end", help="End date (YYYY-MM-DD), inclusive. Defaults to 'now'.")
    return p.parse_args()


def parse_date_utc(date_str: str) -> datetime:
    """Parse YYYY-MM-DD to a UTC datetime at 00:00:00."""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return d.replace(tzinfo=timezone.utc)
    except ValueError:
        raise SystemExit(f"Invalid date format: {date_str}. Expected YYYY-MM-DD.")


def compute_filename(author_id: int, created_at: datetime, attachment_id: int, original_name: str) -> str:
    """
    Filename = sha256(f"{author_id}-{attachment_id}-{created_at_iso}")[:16] + original extension.
    Including attachment_id avoids collisions when a single message has multiple attachments.
    """
    ext = Path(original_name).suffix.lower()  # keep original extension
    base = f"{author_id}-{attachment_id}-{created_at.astimezone(timezone.utc).isoformat()}"
    digest = hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]
    return f"{digest}{ext}" if ext else digest


def is_media(attachment: discord.Attachment) -> bool:
    """
    Accept images/videos. Prefer content_type when available, else fallback to extension.
    """
    ct = (attachment.content_type or "").lower()
    if ct.startswith("image/") or ct.startswith("video/"):
        return True

    # Fallback by extension
    ext = Path(attachment.filename).suffix.lower()
    image_exts = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".heic"}
    video_exts = {".mp4", ".mov", ".m4v", ".webm", ".mkv", ".avi"}
    return ext in image_exts or ext in video_exts


# ---------- Manifest (SQLite) ----------
def db_init(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS downloads (
            attachment_id TEXT PRIMARY KEY,
            message_id TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            guild_id TEXT NOT NULL,
            url TEXT NOT NULL,
            filename TEXT NOT NULL,
            created_at_utc TEXT NOT NULL
        )
        """
    )
    conn.commit()


def db_has_attachment(conn: sqlite3.Connection, attachment_id: int) -> bool:
    cur = conn.execute("SELECT 1 FROM downloads WHERE attachment_id=?", (str(attachment_id),))
    return cur.fetchone() is not None


def db_insert(
    conn: sqlite3.Connection,
    *,
    attachment_id: int,
    message_id: int,
    channel_id: int,
    guild_id: int,
    url: str,
    filename: str,
    created_at_utc: datetime,
):
    conn.execute(
        """
        INSERT OR IGNORE INTO downloads
            (attachment_id, message_id, channel_id, guild_id, url, filename, created_at_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(attachment_id),
            str(message_id),
            str(channel_id),
            str(guild_id),
            url,
            filename,
            created_at_utc.astimezone(timezone.utc).isoformat(),
        ),
    )
    conn.commit()


async def bounded_history(channel: discord.TextChannel, *, after: Optional[datetime], before: Optional[datetime]):
    """
    Async generator over channel history between 'after' and 'before'.
    discord.py history() uses exclusive bounds; we handle inclusivity at the call site.
    """
    async for msg in channel.history(limit=None, after=after, before=before, oldest_first=True):
        yield msg


async def run():
    # ---- CLI & dates
    args = parse_args()
    start_dt = parse_date_utc(args.date_start)
    if args.date_end:
        end_dt = parse_date_utc(args.date_end) + timedelta(hours=23, minutes=59, seconds=59, microseconds=999999)
    else:
        end_dt = datetime.now(timezone.utc)

    if end_dt < start_dt:
        raise SystemExit("date-end must be on/after date-start.")

    # Because Discord history() uses exclusive bounds, pad inward to make our [start,end] inclusive
    after = start_dt - timedelta(seconds=1)
    before = end_dt + timedelta(seconds=1)

    # ---- Env vars
    token = os.environ.get("DISCORD_TOKEN")
    guild_id_str = os.environ.get("GUILD_ID")
    if not token or not guild_id_str:
        raise SystemExit("Missing DISCORD_TOKEN and/or GUILD_ID environment variables.")

    try:
        guild_id = int(guild_id_str)
    except ValueError:
        raise SystemExit("GUILD_ID must be an integer.")

    # ---- Storage setup
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    db_init(conn)

    # ---- Discord client intents
    intents = discord.Intents.none()
    intents.guilds = True
    intents.messages = True  # required to read message history
    intents.message_content = True  # required to see attachments and embeds in messages

    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        print(f"Logged in as {client.user} (id={client.user.id})")
        guild = client.get_guild(guild_id)
        if not guild:
            print(f"Bot is not in guild {guild_id}.")
            await client.close()
            return

        print(f"Scanning guild: {guild.name} ({guild.id})")
        total_seen = 0
        total_downloaded = 0
        total_skipped = 0

        # Iterate text channels the bot can see & read history for
        channels = [c for c in guild.text_channels if c.permissions_for(guild.me).read_message_history]
        for ch in channels:
            print(f" - #{ch.name} ({ch.id})")
            try:
                async for msg in bounded_history(ch, after=after, before=before):
                    total_seen += 1
                    if not msg.attachments:
                        continue

                    # Only consider messages created within the exact inclusive window
                    msg_created_utc = msg.created_at.replace(tzinfo=timezone.utc)
                    if not (start_dt <= msg_created_utc <= end_dt):
                        continue

                    for att in msg.attachments:
                        if not is_media(att):
                            continue

                        if db_has_attachment(conn, att.id):
                            total_skipped += 1
                            continue

                        safe_name = compute_filename(msg.author.id, msg.created_at, att.id, att.filename)
                        dest = DOWNLOAD_DIR / safe_name
                        if dest.exists():
                            # If a previous run without DB happened to write this name, mark manifest & skip
                            db_insert(
                                conn,
                                attachment_id=att.id,
                                message_id=msg.id,
                                channel_id=ch.id,
                                guild_id=guild.id,
                                url=att.url,
                                filename=str(dest.name),
                                created_at_utc=msg_created_utc,
                            )
                            total_skipped += 1
                            continue

                        try:
                            await att.save(dest)
                            db_insert(
                                conn,
                                attachment_id=att.id,
                                message_id=msg.id,
                                channel_id=ch.id,
                                guild_id=guild.id,
                                url=att.url,
                                filename=str(dest.name),
                                created_at_utc=msg_created_utc,
                            )
                            print(f"   saved: {dest.name}  (from @{msg.author} • {msg.created_utc if hasattr(msg,'created_utc') else msg.created_at.isoformat()})")
                            total_downloaded += 1
                        except Exception as e:
                            print(f"   ERROR saving attachment {att.id} from message {msg.id}: {e}")

            except discord.Forbidden:
                print(f"   Skipping #{ch.name}: missing permissions.")
            except discord.HTTPException as e:
                print(f"   HTTP error on #{ch.name}: {e}")

        print("\nSummary")
        print("-------")
        print(f"Messages scanned:   {total_seen}")
        print(f"Files downloaded:   {total_downloaded}")
        print(f"Already downloaded: {total_skipped}")

        # Close database connection
        conn.close()

        # Close the Discord client
        await client.close()

    await client.start(token)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
