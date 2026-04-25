#!/usr/bin/env python3
import os
import sys
import time
import json
from typing import Any, Dict, List, Optional

import requests
from pathlib import Path

env_path = Path("/opt/stacks/torrent-stack/automation/qbit-cleanup.env")
if env_path.exists():
    for line in env_path.read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

QBIT_URL = os.environ.get("QBIT_URL", "http://127.0.0.1:8080")
QBIT_USERNAME = os.environ.get("QBIT_USERNAME", "QBIT_USERNAME")
QBIT_PASSWORD = os.environ.get("QBIT_PASSWORD", "QBIT_PASSWORD")

STALL_MINUTES = int(os.environ.get("STALL_MINUTES", "120"))
NO_SEEDS_MINUTES = int(os.environ.get("NO_SEEDS_MINUTES", "180"))
STRIKE_LIMIT = int(os.environ.get("STRIKE_LIMIT", "3"))

CATEGORY_FILTER = {
    c.strip() for c in os.environ.get("CATEGORY_FILTER", "").split(",") if c.strip()
}

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() != "false"
DELETE_FILES = os.environ.get("DELETE_FILES", "false").lower() == "true"

STATE_FILE = os.environ.get(
    "STATE_FILE",
    "/opt/stacks/torrent-stack/automation/state/qbit-cleanup-state.json",
)

RADARR_URL = os.environ.get("RADARR_URL", "").rstrip("/")
RADARR_API_KEY = os.environ.get("RADARR_API_KEY", "")
RADARR_RESEARCH_ENABLED = os.environ.get("RADARR_RESEARCH_ENABLED", "false").lower() == "true"
RADARR_RESEARCH_DRY_RUN = os.environ.get("RADARR_RESEARCH_DRY_RUN", "true").lower() != "false"
RADARR_RESEARCH_COOLDOWN_HOURS = int(os.environ.get("RADARR_RESEARCH_COOLDOWN_HOURS", "12"))
RADARR_RESEARCH_MAX_RETRIES = int(os.environ.get("RADARR_RESEARCH_MAX_RETRIES", "2"))

SONARR_URL = os.environ.get("SONARR_URL", "").rstrip("/")
SONARR_API_KEY = os.environ.get("SONARR_API_KEY", "")
SONARR_RESEARCH_ENABLED = os.environ.get("SONARR_RESEARCH_ENABLED", "false").lower() == "true"
SONARR_RESEARCH_DRY_RUN = os.environ.get("SONARR_RESEARCH_DRY_RUN", "true").lower() != "false"
SONARR_RESEARCH_COOLDOWN_HOURS = int(os.environ.get("SONARR_RESEARCH_COOLDOWN_HOURS", "12"))
SONARR_RESEARCH_MAX_RETRIES = int(os.environ.get("SONARR_RESEARCH_MAX_RETRIES", "2"))
RESEARCH_RETENTION_DAYS = int(os.environ.get("RESEARCH_RETENTION_DAYS", "14"))

STALL_STATES = {
    "stalledDL",
    "metaDL",
    "checkingDL",
    "queuedDL",
    "forcedMetaDL",
    "missingFiles",
}

TIMEOUT = 20
RADARR_STATE_KEY = "__radarr_research__"
SONARR_STATE_KEY = "__sonarr_research__"


def log(level: str, message: str) -> None:
    print(f"[qbit-cleanup] [{level}] {message}")


def login(session: requests.Session) -> None:
    url = f"{QBIT_URL}/api/v2/auth/login"
    resp = session.post(
        url,
        data={"username": QBIT_USERNAME, "password": QBIT_PASSWORD},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    if resp.text.strip() != "Ok.":
        raise RuntimeError("qBittorrent login failed")


def get_torrents(session: requests.Session) -> List[Dict[str, Any]]:
    url = f"{QBIT_URL}/api/v2/torrents/info"
    resp = session.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def remove_torrent(session: requests.Session, torrent_hash: str, name: str, reason: str, strikes: int) -> None:
    if DRY_RUN:
        log(
            "INFO",
            f"DRY RUN: would remove name='{name}' hash={torrent_hash} strikes={strikes} reason='{reason}' delete_files={DELETE_FILES}",
        )
        return

    url = f"{QBIT_URL}/api/v2/torrents/delete"
    resp = session.post(
        url,
        data={"hashes": torrent_hash, "deleteFiles": str(DELETE_FILES).lower()},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    log(
        "INFO",
        f"Removed name='{name}' hash={torrent_hash} strikes={strikes} reason='{reason}' delete_files={DELETE_FILES}",
    )


def category_allowed(category: str) -> bool:
    if not CATEGORY_FILTER:
        return True
    return category in CATEGORY_FILTER


def is_incomplete(t: Dict[str, Any]) -> bool:
    progress = float(t.get("progress", 0))
    return progress < 1.0


def minutes_since(unix_ts: Optional[int]) -> float:
    if not unix_ts or unix_ts <= 0:
        return 0.0
    return (time.time() - unix_ts) / 60.0


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return {}

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        log("WARN", f"State file did not contain a JSON object, resetting: {STATE_FILE}")
        return {}
    except json.JSONDecodeError:
        log("WARN", f"State file is invalid JSON, resetting: {STATE_FILE}")
        return {}
    except Exception as exc:
        log("WARN", f"Failed to load state file {STATE_FILE}: {exc}")
        return {}


def save_state(state: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    tmp_file = f"{STATE_FILE}.tmp"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp_file, STATE_FILE)


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


def current_downloaded_bytes(t: Dict[str, Any]) -> int:
    return int(t.get("downloaded", 0) or 0)


def should_flag(t: Dict[str, Any]) -> Optional[str]:
    category = t.get("category", "")
    state = t.get("state", "")
    num_seeds = int(t.get("num_seeds", 0))
    added_on = int(t.get("added_on", 0))
    last_activity = int(t.get("last_activity", 0))

    if not category_allowed(category):
        return None

    if not is_incomplete(t):
        return None

    age_mins = minutes_since(added_on)
    inactive_mins = minutes_since(last_activity)

    if num_seeds == 0 and age_mins >= NO_SEEDS_MINUTES:
        return f"0 seeds observed; torrent age={age_mins:.0f}m threshold={NO_SEEDS_MINUTES}m"

    if state in STALL_STATES and inactive_mins >= STALL_MINUTES:
        return f"state={state}; inactive={inactive_mins:.0f}m threshold={STALL_MINUTES}m"

    return None


def build_state_entry(t: Dict[str, Any], reason: str, previous: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    previous = previous or {}
    prev_strikes = int(previous.get("strikes", 0))
    strikes = min(prev_strikes + 1, STRIKE_LIMIT)

    return {
        "name": t.get("name", "<unknown>"),
        "category": t.get("category", ""),
        "reason": reason,
        "strikes": strikes,
        "first_flagged_at": previous.get("first_flagged_at", now_iso()),
        "last_flagged_at": now_iso(),
        "last_seen_state": t.get("state", ""),
        "last_seen_progress": float(t.get("progress", 0)),
        "last_seen_downloaded_bytes": current_downloaded_bytes(t),
        "last_seen_num_seeds": int(t.get("num_seeds", 0)),
        "last_seen_added_on": int(t.get("added_on", 0)),
        "last_seen_last_activity": int(t.get("last_activity", 0)),
        "last_action": previous.get("last_action", "flagged"),
    }


def get_meta_bucket(state: Dict[str, Any], key: str) -> Dict[str, Any]:
    meta = state.get(key)
    if not isinstance(meta, dict):
        meta = {}
        state[key] = meta
    return meta


def radarr_headers() -> Dict[str, str]:
    return {"X-Api-Key": RADARR_API_KEY}


def sonarr_headers() -> Dict[str, str]:
    return {"X-Api-Key": SONARR_API_KEY}


def radarr_lookup_history_by_hash(download_hash: str) -> Optional[Dict[str, Any]]:
    if not RADARR_URL or not RADARR_API_KEY:
        return None
    url = f"{RADARR_URL}/api/v3/history?page=1&pageSize=100&sortKey=date&sortDirection=descending"
    resp = requests.get(url, headers=radarr_headers(), timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    target = download_hash.lower()
    for rec in data.get("records", []):
        if str(rec.get("downloadId", "")).lower() == target:
            return rec
    return None


def sonarr_lookup_history_by_hash(download_hash: str) -> Optional[Dict[str, Any]]:
    if not SONARR_URL or not SONARR_API_KEY:
        return None
    url = f"{SONARR_URL}/api/v3/history?page=1&pageSize=100&sortKey=date&sortDirection=descending"
    resp = requests.get(url, headers=sonarr_headers(), timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    target = download_hash.lower()
    for rec in data.get("records", []):
        if str(rec.get("downloadId", "")).lower() == target:
            return rec
    return None


def retry_allowed(meta_bucket: Dict[str, Any], item_key: str, removed_hash: str, cooldown_hours: int, max_retries: int, label: str) -> bool:
    item_meta = meta_bucket.setdefault(item_key, {
        "retry_count": 0,
        "last_attempt_ts": 0,
        "attempted_hashes": [],
        "failed_titles": [],
        "failed_release_groups": [],
        "last_nudge_ts": 0,
        "nudge_count": 0,
        "nudge_exhausted": False,
    })

    now_ts = int(time.time())
    cooldown_seconds = cooldown_hours * 3600

    if removed_hash in item_meta.get("attempted_hashes", []):
        log("INFO", f"{label} re-search skipped for key={item_key}: hash already attempted")
        return False

    last_attempt_ts = int(item_meta.get("last_attempt_ts", 0))
    if last_attempt_ts and (now_ts - last_attempt_ts) < cooldown_seconds:
        remaining = cooldown_seconds - (now_ts - last_attempt_ts)
        log("INFO", f"{label} re-search skipped for key={item_key}: cooldown active remaining_seconds={remaining}")
        return False

    retry_count = int(item_meta.get("retry_count", 0))
    if retry_count >= max_retries:
        log("INFO", f"{label} re-search skipped for key={item_key}: max retries reached retry_count={retry_count}")
        return False

    return True


def record_retry(meta_bucket: Dict[str, Any], item_key: str, removed_hash: str, source_title: str, history_id: int, release_group: str) -> None:
    item_meta = meta_bucket.setdefault(item_key, {
        "retry_count": 0,
        "last_attempt_ts": 0,
        "attempted_hashes": [],
        "failed_titles": [],
        "failed_release_groups": [],
        "last_failed_history_id": 0,
        "last_nudge_ts": 0,
        "nudge_count": 0,
        "nudge_exhausted": False,
    })

    attempted_hashes = item_meta.setdefault("attempted_hashes", [])
    failed_titles = item_meta.setdefault("failed_titles", [])
    failed_release_groups = item_meta.setdefault("failed_release_groups", [])

    if removed_hash not in attempted_hashes:
        attempted_hashes.append(removed_hash)

    if source_title and source_title not in failed_titles:
        failed_titles.append(source_title)

    if release_group and release_group not in failed_release_groups:
        failed_release_groups.append(release_group)

    item_meta["retry_count"] = int(item_meta.get("retry_count", 0)) + 1
    item_meta["last_attempt_ts"] = int(time.time())
    item_meta["last_failed_history_id"] = int(history_id or 0)


def title_already_failed(meta_bucket: Dict[str, Any], item_key: str, source_title: str) -> bool:
    if not source_title:
        return False

    item_meta = meta_bucket.setdefault(item_key, {
        "retry_count": 0,
        "last_attempt_ts": 0,
        "attempted_hashes": [],
        "failed_titles": [],
        "failed_release_groups": [],
        "last_failed_history_id": 0,
        "last_nudge_ts": 0,
        "nudge_count": 0,
        "nudge_exhausted": False,
    })

    failed_titles = item_meta.setdefault("failed_titles", [])
    return source_title in failed_titles


def release_group_already_failed(meta_bucket: Dict[str, Any], item_key: str, release_group: str) -> bool:
    if not release_group:
        return False

    item_meta = meta_bucket.setdefault(item_key, {
        "retry_count": 0,
        "last_attempt_ts": 0,
        "attempted_hashes": [],
        "failed_titles": [],
        "failed_release_groups": [],
        "last_failed_history_id": 0,
        "last_nudge_ts": 0,
        "nudge_count": 0,
        "nudge_exhausted": False,
    })

    failed_release_groups = item_meta.setdefault("failed_release_groups", [])
    return release_group in failed_release_groups


def radarr_mark_history_failed(history_id: int) -> None:
    url = f"{RADARR_URL}/api/v3/history/failed/{history_id}"
    resp = requests.post(url, headers=radarr_headers(), timeout=TIMEOUT)
    resp.raise_for_status()


def radarr_trigger_movie_search(movie_id: int) -> None:
    url = f"{RADARR_URL}/api/v3/command"
    payload = {"name": "MoviesSearch", "movieIds": [movie_id]}
    resp = requests.post(url, headers=radarr_headers(), json=payload, timeout=TIMEOUT)
    resp.raise_for_status()


def sonarr_mark_history_failed(history_id: int) -> None:
    url = f"{SONARR_URL}/api/v3/history/failed/{history_id}"
    resp = requests.post(url, headers=sonarr_headers(), timeout=TIMEOUT)
    resp.raise_for_status()


def sonarr_trigger_episode_search(episode_id: int) -> None:
    url = f"{SONARR_URL}/api/v3/command"
    payload = {"name": "EpisodeSearch", "episodeIds": [episode_id]}
    resp = requests.post(url, headers=sonarr_headers(), json=payload, timeout=TIMEOUT)
    resp.raise_for_status()


def radarr_movie_should_nudge(movie_id: int) -> bool:
    if not RADARR_URL or not RADARR_API_KEY:
        return False
    url = f"{RADARR_URL}/api/v3/movie/{movie_id}"
    resp = requests.get(url, headers=radarr_headers(), timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    return bool(data.get("monitored")) and not bool(data.get("hasFile"))


def sonarr_episode_should_nudge(series_id: int, episode_id: int) -> bool:
    if not SONARR_URL or not SONARR_API_KEY:
        return False

    series_url = f"{SONARR_URL}/api/v3/series/{series_id}"
    episode_url = f"{SONARR_URL}/api/v3/episode/{episode_id}"

    series_resp = requests.get(series_url, headers=sonarr_headers(), timeout=TIMEOUT)
    series_resp.raise_for_status()
    series_data = series_resp.json()

    episode_resp = requests.get(episode_url, headers=sonarr_headers(), timeout=TIMEOUT)
    episode_resp.raise_for_status()
    episode_data = episode_resp.json()

    return (
        bool(series_data.get("monitored"))
        and bool(episode_data.get("monitored"))
        and not bool(episode_data.get("hasFile"))
    )


def handle_radarr_research(state: Dict[str, Any], torrent_hash: str, torrent_name: str, category: str) -> None:
    if category != "movies" or not RADARR_RESEARCH_ENABLED:
        return

    try:
        rec = radarr_lookup_history_by_hash(torrent_hash)
    except Exception as exc:
        log("ERROR", f"Radarr history lookup failed for hash={torrent_hash}: {exc}")
        return

    if not rec:
        log("INFO", f"Radarr history lookup found no match for removed hash={torrent_hash} name='{torrent_name}'")
        return

    movie_id = rec.get("movieId")
    history_id = rec.get("id")
    source_title = rec.get("sourceTitle", "")
    event_type = rec.get("eventType", "")
    download_client = rec.get("data", {}).get("downloadClientName", "")
    release_group = rec.get("data", {}).get("releaseGroup", "") or ""

    if not movie_id or not history_id:
        log("INFO", f"Radarr history lookup missing movieId/historyId for hash={torrent_hash} name='{torrent_name}'")
        return

    bucket = get_meta_bucket(state, RADARR_STATE_KEY)
    item_key = str(movie_id)

    if not retry_allowed(bucket, item_key, torrent_hash, RADARR_RESEARCH_COOLDOWN_HOURS, RADARR_RESEARCH_MAX_RETRIES, "Radarr"):
        return

    if title_already_failed(bucket, item_key, source_title):
        log("INFO", f"Radarr re-search skipped for key={item_key}: source title already failed sourceTitle='{source_title}'")
        return

    if release_group_already_failed(bucket, item_key, release_group):
        log("INFO", f"Radarr re-search skipped for key={item_key}: release group already failed releaseGroup='{release_group}'")
        return

    if RADARR_RESEARCH_DRY_RUN:
        log(
            "INFO",
            f"RADARR DRY RUN: would blocklist/mark failed and would search movieId={movie_id} "
            f"historyId={history_id} sourceTitle='{source_title}' eventType='{event_type}' "
            f"downloadClient='{download_client}' removedHash={torrent_hash}"
        )
        record_retry(bucket, item_key, torrent_hash, source_title, int(history_id), release_group)
        return

    try:
        radarr_mark_history_failed(int(history_id))
        log(
            "INFO",
            f"Radarr marked history failed historyId={history_id} movieId={movie_id} "
            f"sourceTitle='{source_title}' removedHash={torrent_hash}"
        )
        radarr_trigger_movie_search(int(movie_id))
        log(
            "INFO",
            f"Radarr triggered movie search movieId={movie_id} sourceTitle='{source_title}' removedHash={torrent_hash}"
        )
        record_retry(bucket, item_key, torrent_hash, source_title, int(history_id), release_group)
    except Exception as exc:
        log(
            "ERROR",
            f"Radarr live re-search failed movieId={movie_id} historyId={history_id} "
            f"sourceTitle='{source_title}' removedHash={torrent_hash}: {exc}"
        )


def handle_sonarr_research(state: Dict[str, Any], torrent_hash: str, torrent_name: str, category: str) -> None:
    if category != "tv" or not SONARR_RESEARCH_ENABLED:
        return

    try:
        rec = sonarr_lookup_history_by_hash(torrent_hash)
    except Exception as exc:
        log("ERROR", f"Sonarr history lookup failed for hash={torrent_hash}: {exc}")
        return

    if not rec:
        log("INFO", f"Sonarr history lookup found no match for removed hash={torrent_hash} name='{torrent_name}'")
        return

    series_id = rec.get("seriesId")
    episode_id = rec.get("episodeId")
    history_id = rec.get("id")
    source_title = rec.get("sourceTitle", "")
    event_type = rec.get("eventType", "")
    download_client = rec.get("data", {}).get("downloadClientName", "")
    release_group = rec.get("data", {}).get("releaseGroup", "") or ""

    if not series_id or not episode_id or not history_id:
        log("INFO", f"Sonarr history lookup missing seriesId/episodeId/historyId for hash={torrent_hash} name='{torrent_name}'")
        return

    bucket = get_meta_bucket(state, SONARR_STATE_KEY)
    item_key = f"{series_id}:{episode_id}"

    if not retry_allowed(bucket, item_key, torrent_hash, SONARR_RESEARCH_COOLDOWN_HOURS, SONARR_RESEARCH_MAX_RETRIES, "Sonarr"):
        return

    if title_already_failed(bucket, item_key, source_title):
        log("INFO", f"Sonarr re-search skipped for key={item_key}: source title already failed sourceTitle='{source_title}'")
        return

    if release_group_already_failed(bucket, item_key, release_group):
        log("INFO", f"Sonarr re-search skipped for key={item_key}: release group already failed releaseGroup='{release_group}'")
        return

    if SONARR_RESEARCH_DRY_RUN:
        log(
            "INFO",
            f"SONARR DRY RUN: would blocklist/mark failed and would search seriesId={series_id} "
            f"episodeId={episode_id} historyId={history_id} sourceTitle='{source_title}' "
            f"eventType='{event_type}' downloadClient='{download_client}' removedHash={torrent_hash}"
        )
        record_retry(bucket, item_key, torrent_hash, source_title, int(history_id), release_group)
        return

    try:
        sonarr_mark_history_failed(int(history_id))
        log(
            "INFO",
            f"Sonarr marked history failed historyId={history_id} seriesId={series_id} "
            f"episodeId={episode_id} sourceTitle='{source_title}' removedHash={torrent_hash}"
        )
        sonarr_trigger_episode_search(int(episode_id))
        log(
            "INFO",
            f"Sonarr triggered episode search seriesId={series_id} episodeId={episode_id} "
            f"sourceTitle='{source_title}' removedHash={torrent_hash}"
        )
        record_retry(bucket, item_key, torrent_hash, source_title, int(history_id), release_group)
    except Exception as exc:
        log(
            "ERROR",
            f"Sonarr live re-search failed seriesId={series_id} episodeId={episode_id} historyId={history_id} "
            f"sourceTitle='{source_title}' removedHash={torrent_hash}: {exc}"
        )


def active_tracked_count(state: Dict[str, Any]) -> int:
    return len([k for k in state.keys() if not k.startswith("__")])

def metadata_last_ts(item_meta: Dict[str, Any]) -> int:
    return max(
        int(item_meta.get("last_nudge_ts", 0) or 0),
        int(item_meta.get("last_attempt_ts", 0) or 0),
    )


def prune_meta_bucket(bucket: Dict[str, Any], label: str) -> None:
    now_ts = int(time.time())
    retention_seconds = RESEARCH_RETENTION_DAYS * 86400
    to_delete = []

    for item_key, item_meta in list(bucket.items()):
        if not isinstance(item_meta, dict):
            to_delete.append(item_key)
            continue

        if bool(item_meta.get("no_longer_wanted", False)):
            log("INFO", f"Pruning {label} metadata key={item_key}: no longer wanted")
            to_delete.append(item_key)
            continue

        if bool(item_meta.get("nudge_exhausted", False)):
            log("INFO", f"Pruning {label} metadata key={item_key}: nudge exhausted")
            to_delete.append(item_key)
            continue

        last_ts = metadata_last_ts(item_meta)
        if last_ts and (now_ts - last_ts) > retention_seconds:
            log("INFO", f"Pruning {label} metadata key={item_key}: older than retention window")
            to_delete.append(item_key)

    for item_key in to_delete:
        bucket.pop(item_key, None)

def should_nudge(item_meta: Dict[str, Any], cooldown_hours: int, max_retries: int) -> bool:
    now_ts = int(time.time())
    last_nudge_ts = int(item_meta.get("last_nudge_ts", 0))
    nudge_count = int(item_meta.get("nudge_count", 0))
    nudge_exhausted = bool(item_meta.get("nudge_exhausted", False))

    if nudge_exhausted:
        return False

    if nudge_count >= max_retries:
        item_meta["nudge_exhausted"] = True
        return False

    if last_nudge_ts and (now_ts - last_nudge_ts) < (cooldown_hours * 3600):
        return False

    return True


def record_nudge(item_meta: Dict[str, Any], max_retries: int) -> None:
    item_meta["last_nudge_ts"] = int(time.time())
    item_meta["nudge_count"] = int(item_meta.get("nudge_count", 0)) + 1
    if int(item_meta.get("nudge_count", 0)) >= max_retries:
        item_meta["nudge_exhausted"] = True

def run_nudge_searches(state: Dict[str, Any]) -> None:
    if os.environ.get("NUDGE_ENABLED", "false").lower() != "true":
        return

    cooldown = int(os.environ.get("NUDGE_COOLDOWN_HOURS", "6"))
    max_retries = int(os.environ.get("NUDGE_MAX_RETRIES", "3"))

    # ---- RADARR ----
    radarr_bucket = state.get(RADARR_STATE_KEY, {})
    for movie_id, meta in radarr_bucket.items():
        if not should_nudge(meta, cooldown, max_retries):
            continue

        try:
            if not radarr_movie_should_nudge(int(movie_id)):
                meta["no_longer_wanted"] = True
                log("INFO", f"NUDGE: skipping Radarr movieId={movie_id} because movie is no longer wanted")
                continue

            log("INFO", f"NUDGE: triggering Radarr search movieId={movie_id}")
            radarr_trigger_movie_search(int(movie_id))
            record_nudge(meta, max_retries)
        except Exception as exc:
            log("ERROR", f"NUDGE Radarr failed movieId={movie_id}: {exc}")

    # ---- SONARR ----
    sonarr_bucket = state.get(SONARR_STATE_KEY, {})
    for key, meta in sonarr_bucket.items():
        if not should_nudge(meta, cooldown, max_retries):
            continue

        try:
            series_id, episode_id = key.split(":")
            if not sonarr_episode_should_nudge(int(series_id), int(episode_id)):
                meta["no_longer_wanted"] = True
                log("INFO", f"NUDGE: skipping Sonarr key={key} because episode is no longer wanted")
                continue

            log("INFO", f"NUDGE: triggering Sonarr search seriesId={series_id} episodeId={episode_id}")
            sonarr_trigger_episode_search(int(episode_id))
            record_nudge(meta, max_retries)
        except Exception as exc:
            log("ERROR", f"NUDGE Sonarr failed key={key}: {exc}")

def main() -> int:
    try:
        state = load_state()
        get_meta_bucket(state, RADARR_STATE_KEY)
        get_meta_bucket(state, SONARR_STATE_KEY)

        with requests.Session() as session:
            login(session)
            torrents = get_torrents(session)
            log("INFO", f"Fetched {len(torrents)} torrents")

            current_hashes = set()

            for t in torrents:
                torrent_hash = t.get("hash")
                name = t.get("name", "<unknown>")
                category = t.get("category", "")
                progress = float(t.get("progress", 0))
                num_seeds = int(t.get("num_seeds", 0))
                downloaded_bytes = current_downloaded_bytes(t)

                if not torrent_hash:
                    continue

                current_hashes.add(torrent_hash)

                if not category_allowed(category):
                    continue

                if not is_incomplete(t):
                    if torrent_hash in state:
                        state.pop(torrent_hash, None)
                    continue

                previous = state.get(torrent_hash)

                if previous:
                    prev_bytes = int(previous.get("last_seen_downloaded_bytes", 0))
                    if downloaded_bytes > prev_bytes:
                        log(
                            "INFO",
                            f"Downloaded bytes increased, clearing strikes for name='{name}' hash={torrent_hash}"
                        )
                        state.pop(torrent_hash, None)
                        previous = None

                reason = should_flag(t)

                if reason:
                    entry = build_state_entry(t, reason, previous)
                    state[torrent_hash] = entry

                    log(
                        "INFO",
                        f"Flagged name='{name}' hash={torrent_hash} progress={progress:.3f} "
                        f"bytes={downloaded_bytes} seeds={num_seeds} strikes={entry['strikes']}/{STRIKE_LIMIT} "
                        f"reason='{reason}'"
                    )

                    if entry["strikes"] >= STRIKE_LIMIT and entry.get("last_action") not in {"remove_attempted", "removed"}:
                        remove_torrent(session, torrent_hash, name, reason, entry["strikes"])
                        state[torrent_hash]["last_action"] = "remove_attempted" if DRY_RUN else "removed"
                        if not DRY_RUN:
                            handle_radarr_research(state, torrent_hash, name, category)
                            handle_sonarr_research(state, torrent_hash, name, category)

                    continue

                if previous:
                    state.pop(torrent_hash, None)

            stale_hashes = [
                torrent_hash
                for torrent_hash in list(state.keys())
                if not torrent_hash.startswith("__") and torrent_hash not in current_hashes
            ]
            for torrent_hash in stale_hashes:
                entry = state.get(torrent_hash, {})
                log(
                    "INFO",
                    f"Removing stale state for hash={torrent_hash} name='{entry.get('name', '<unknown>')}' because torrent is no longer present"
                )
                state.pop(torrent_hash, None)

        run_nudge_searches(state)
        prune_meta_bucket(get_meta_bucket(state, RADARR_STATE_KEY), "Radarr")
        prune_meta_bucket(get_meta_bucket(state, SONARR_STATE_KEY), "Sonarr")

        save_state(state)
        log("INFO", f"Run complete. Active tracked torrents in state: {active_tracked_count(state)}")
        return 0

    except Exception as exc:
        log("ERROR", f"{exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
