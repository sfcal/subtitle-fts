"""Subtitle FTS indexer — scans SRT files and pushes them to MeiliSearch."""

import hashlib
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)

MEILI_URL = os.environ.get("MEILI_URL", "http://localhost:7700")
MEILI_MASTER_KEY = os.environ.get("MEILI_MASTER_KEY", "")
MEDIA_PATH = Path(os.environ.get("MEDIA_PATH", "/media"))
INDEX_INTERVAL = int(os.environ.get("INDEX_INTERVAL", "21600"))
STATE_FILE = Path(os.environ.get("STATE_FILE", "/data/index_state.json"))
INDEX_NAME = "subtitles"
BATCH_SIZE = 20

# SRT timestamp pattern: 00:01:23,456 --> 00:01:25,789
SRT_TS_RE = re.compile(
    r"(\d{2}:\d{2}:\d{2}),\d{3}\s*-->\s*(\d{2}:\d{2}:\d{2}),\d{3}"
)

# TV path pattern: .../tv/Show Name/Season 01/Show Name - S01E02 - Title.srt
TV_SEASON_EP_RE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,3})")


def meili_headers() -> dict:
    headers = {"Content-Type": "application/json"}
    if MEILI_MASTER_KEY:
        headers["Authorization"] = f"Bearer {MEILI_MASTER_KEY}"
    return headers


def configure_index() -> None:
    """Set searchable, displayed, and filterable attributes on the index."""
    url = f"{MEILI_URL}/indexes/{INDEX_NAME}"
    headers = meili_headers()

    requests.post(
        f"{MEILI_URL}/indexes",
        headers=headers,
        json={"uid": INDEX_NAME, "primaryKey": "id"},
    )

    time.sleep(1)

    requests.patch(
        f"{url}/settings",
        headers=headers,
        json={
            "searchableAttributes": ["content", "title"],
            "displayedAttributes": [
                "id",
                "title",
                "media_type",
                "season",
                "episode",
                "file_path",
                "content",
                "timestamps",
            ],
            "filterableAttributes": ["media_type", "season", "episode"],
            "sortableAttributes": ["title"],
        },
    )
    log.info("index settings configured")


def read_srt(path: Path) -> str | None:
    """Read an SRT file, handling common encodings."""
    for encoding in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
        try:
            return path.read_text(encoding=encoding)
        except (UnicodeDecodeError, ValueError):
            continue
    log.warning("failed to decode %s", path)
    return None


def parse_srt(text: str) -> tuple[str, list[dict]]:
    """Parse SRT content into plain text and timestamped entries."""
    blocks = re.split(r"\n\s*\n", text.strip())
    timestamps = []
    lines = []

    for block in blocks:
        block_lines = block.strip().splitlines()
        if len(block_lines) < 2:
            continue

        ts_match = SRT_TS_RE.search(block_lines[1] if len(block_lines) > 1 else "")
        if not ts_match:
            ts_match = SRT_TS_RE.search(block_lines[0])
            if not ts_match:
                continue
            subtitle_text = " ".join(block_lines[1:])
        else:
            subtitle_text = " ".join(block_lines[2:])

        subtitle_text = re.sub(r"<[^>]+>", "", subtitle_text).strip()
        if not subtitle_text:
            continue

        lines.append(subtitle_text)
        timestamps.append(
            {
                "start": ts_match.group(1),
                "end": ts_match.group(2),
                "text": subtitle_text,
            }
        )

    return " ".join(lines), timestamps


def extract_metadata(path: Path) -> dict:
    """Extract title, media_type, season, and episode from file path."""
    rel = path.relative_to(MEDIA_PATH)
    parts = rel.parts

    media_type = "movie"
    season = None
    episode = None
    title = path.stem

    if len(parts) >= 1 and parts[0].lower() == "tv":
        media_type = "tv"
        if len(parts) >= 2:
            title = parts[1]
        match = TV_SEASON_EP_RE.search(str(rel))
        if match:
            season = int(match.group(1))
            episode = int(match.group(2))
    elif len(parts) >= 1 and parts[0].lower() == "movies":
        media_type = "movie"
        if len(parts) >= 2:
            title = parts[1]

    return {
        "media_type": media_type,
        "title": title,
        "season": season,
        "episode": episode,
    }


def file_id(path: Path) -> str:
    """Stable document ID from file path."""
    return hashlib.sha256(str(path).encode()).hexdigest()[:16]


def load_state() -> dict:
    """Load {path: mtime} state from disk."""
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state))


def scan_srt_files() -> dict[str, float]:
    """Return {str(path): mtime} for all SRT files under MEDIA_PATH."""
    found = {}
    for srt in MEDIA_PATH.rglob("*.srt"):
        try:
            found[str(srt)] = srt.stat().st_mtime
        except OSError:
            continue
    return found


def push_batch(batch: list[dict]) -> None:
    """Push a batch of documents to MeiliSearch."""
    resp = requests.post(
        f"{MEILI_URL}/indexes/{INDEX_NAME}/documents",
        headers=meili_headers(),
        json=batch,
        timeout=120,
    )
    resp.raise_for_status()


def run_index_cycle() -> None:
    """Run one full index cycle: scan, diff, index new, remove stale."""
    state = load_state()
    current = scan_srt_files()
    log.info("scanned %d srt files on disk", len(current))

    # Find new or modified paths
    new_or_modified = [
        p for p, mtime in current.items() if state.get(p) != mtime
    ]

    # Find deleted paths
    deleted = set(state.keys()) - set(current.keys())

    if not new_or_modified and not deleted:
        log.info("no changes detected")
        return

    log.info("found %d new/modified, %d deleted", len(new_or_modified), len(deleted))

    # Process new/modified in streaming batches
    batch = []
    batch_paths = []
    total_pushed = 0

    for path_str in new_or_modified:
        path = Path(path_str)
        try:
            text = read_srt(path)
        except Exception:
            log.warning("error reading %s, skipping", path_str)
            continue

        if text is None:
            state[path_str] = current[path_str]
            continue

        content, timestamps = parse_srt(text)
        del text  # free memory immediately

        if not content:
            state[path_str] = current[path_str]
            continue

        meta = extract_metadata(path)
        batch.append(
            {
                "id": file_id(path),
                "title": meta["title"],
                "media_type": meta["media_type"],
                "season": meta["season"],
                "episode": meta["episode"],
                "file_path": path_str,
                "content": content,
                "timestamps": timestamps,
            }
        )
        batch_paths.append(path_str)

        if len(batch) >= BATCH_SIZE:
            push_batch(batch)
            total_pushed += len(batch)
            for bp in batch_paths:
                state[bp] = current[bp]
            save_state(state)
            log.info("pushed %d / %d", total_pushed, len(new_or_modified))
            batch.clear()
            batch_paths.clear()

    if batch:
        push_batch(batch)
        total_pushed += len(batch)
        for bp in batch_paths:
            state[bp] = current[bp]
        save_state(state)
        log.info("pushed %d / %d", total_pushed, len(new_or_modified))

    # Handle deletions
    if deleted:
        stale_ids = [file_id(Path(p)) for p in deleted]
        resp = requests.post(
            f"{MEILI_URL}/indexes/{INDEX_NAME}/documents/delete-batch",
            headers=meili_headers(),
            json=stale_ids,
        )
        resp.raise_for_status()
        for p in deleted:
            state.pop(p, None)
        save_state(state)
        log.info("deleted %d stale documents", len(deleted))

    log.info("index cycle complete — %d files tracked", len(state))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )

    log.info("subtitle-fts indexer starting")
    log.info("meili_url=%s media_path=%s interval=%ds", MEILI_URL, MEDIA_PATH, INDEX_INTERVAL)

    configure_index()

    RETRY_DELAY = 60
    while True:
        try:
            run_index_cycle()
            log.info("sleeping %ds until next cycle", INDEX_INTERVAL)
            time.sleep(INDEX_INTERVAL)
        except Exception:
            log.exception("index cycle failed, retrying in %ds", RETRY_DELAY)
            time.sleep(RETRY_DELAY)


if __name__ == "__main__":
    main()
