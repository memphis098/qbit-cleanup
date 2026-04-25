# qBittorrent Cleanup Script

A simple Python script for removing stalled or no-seed incomplete qBittorrent downloads after a configurable number of strikes.

The script is designed to be readable. All logic is contained in `qbit-cleanup.py`, and all settings are controlled through `qbit-cleanup.env`.

## Warning
Start with DRY_RUN=true and DELETE_FILES=false

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

## Installation
## Create install directory

sudo mkdir -p /opt/qbit-cleanup/state

## Copy files

sudo cp qbit-cleanup.py /opt/qbit-cleanup/
sudo cp qbit-cleanup.env.example /opt/qbit-cleanup/qbit-cleanup.env
sudo chmod +x /opt/qbit-cleanup/qbit-cleanup.py

## Edit configuration

sudo nano /opt/qbit-cleanup/qbit-cleanup.env

Update at minimum:
QBIT_URL=http://127.0.0.1:8080
QBIT_USERNAME=your_username
QBIT_PASSWORD=your_password


## Configuration Guide
## Strike system

STRIKE_LIMIT=3
Number of failed checks before a torrent is removed.

## Timing

STALL_MINUTES=120
NO_SEEDS_MINUTES=180
STALL_MINUTES: how long a torrent can be inactive
NO_SEEDS_MINUTES: how long with 0 seeds before removal

## Category filtering

CATEGORY_FILTER=movies,tv
Only manage these categories.

Leave empty to manage all:
CATEGORY_FILTER=


## Safety settings (IMPORTANT)
## Start with:

DRY_RUN=true
DELETE_FILES=false

Then switch to live mode:
DRY_RUN=false

Delete files as well:
DELETE_FILES=true


## Manual Run
sudo /bin/bash -lc 'set -a; source /opt/qbit-cleanup/qbit-cleanup.env; set +a; /opt/qbit-cleanup/qbit-cleanup.py'


## Automatic Execution (systemd)
1. Install service + timer

sudo cp systemd/qbit-cleanup.service /etc/systemd/system/
sudo cp systemd/qbit-cleanup.timer /etc/systemd/system/

2. Enable timer
sudo systemctl daemon-reload
sudo systemctl enable --now qbit-cleanup.timer

3. Check status
systemctl list-timers qbit-cleanup.timer

4. View logs
journalctl -u qbit-cleanup.service -n 100 --no-pager


## Optional Features
Radarr / Sonarr integration

Disabled by default.

Enable only after testing:

RADARR_RESEARCH_ENABLED=true
SONARR_RESEARCH_ENABLED=true
Nudge searches

Retries failed downloads periodically:

NUDGE_ENABLED=true
NUDGE_COOLDOWN_HOURS=12
NUDGE_MAX_RETRIES=2
Metadata cleanup

Automatically cleans old entries:

RESEARCH_RETENTION_DAYS=14


## Disclaimer

Use at your own risk.

Always start with:

DRY_RUN=true
DELETE_FILES=false
