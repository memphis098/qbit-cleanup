# qBittorrent Cleanup Script

A simple Python script for removing stalled or no-seed incomplete qBittorrent downloads after a configurable number of strikes.

The script is designed to be readable. All logic is contained in `qbit-cleanup.py`, and all settings are controlled through `qbit-cleanup.env`.

## Features

- Tracks stalled or no-seed incomplete torrents
- Uses a strike system before removing anything
- Supports category filtering
- Optional file deletion
- Optional Radarr and Sonarr failed-download handling
- Optional re-search nudges
- Keeps state in a local JSON file
- Dry-run mode by default

## Safety defaults

The example configuration ships with:

```env
DRY_RUN=true
DELETE_FILES=false
RADARR_RESEARCH_ENABLED=false
SONARR_RESEARCH_ENABLED=false
NUDGE_ENABLED=false