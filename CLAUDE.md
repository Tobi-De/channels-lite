# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`channels-lite` is a lightweight Django Channels layer implementation backed by SQLite database instead of Redis. It provides asynchronous messaging and WebSocket support for Django applications without requiring external infrastructure like Redis.

**Key Design Principles:**
- SQLite-backed channel layer for simplified deployment
- Asynchronous message passing using Django's async ORM
- Process-specific channel buffering with background polling
- Supports Django Channels extensions: `groups` and `flush`

## Development Commands

### Package Management
This project uses `uv` for dependency management:
```bash
uv sync              # Install dependencies (with dev dependencies)
uv build             # Build package for distribution
```

### Testing
Tests are located in the `tests/` directory:
```bash
uv run pytest                      # Run all tests
uv run pytest tests/test_core_layer.py  # Run specific test file
uv run pytest -k test_send_receive      # Run tests matching pattern
uv run pytest --tb=short -v            # Run with short traceback and verbose output
```

### Demo Application
The `demo/` directory contains a Django app for testing:
```bash
just runserver                    # Run demo server (cd demo && uv run python manage.py runserver)
just dj <command>                 # Run any Django management command in demo
```

### Release Process
```bash
just release patch|minor|major    # Bump version, commit, tag, and push
just release X.Y.Z                # Release specific version
```

## Architecture

### Core Components

**1. Channel Layer (`src/channels_lite/layers/core.py`)**
- `SQLiteChannelLayer`: Main channel layer implementation
- Implements Django Channels layer spec using SQLite database
- Key mechanism: Background polling tasks (`_poll_and_distribute`) for process-specific channels
- Process-specific channels (containing `!`) use buffering with `asyncio.Queue`
- Normal channels use direct database polling in `receive()`

**2. Database Models (`src/channels_lite/models.py`)**
- `Event`: Stores channel messages with expiry and delivery tracking
  - Indexed on `(channel_name, expires_at)` for efficient polling
  - `delivered` flag prevents duplicate message delivery
- `GroupMembership`: Manages channel group memberships
  - Indexed on `(group_name, joined_at)`
  - Supports group broadcast functionality

**3. Database Router (`src/channels_lite/router.py`)**
- `ChannelsRouter`: Routes channel layer models to dedicated database
- Reads database alias from `CHANNELS_LAYER["default"]["OPTIONS"]["database"]`
- Ensures channel layer tables are isolated from main application database

### Message Flow

**Sending:**
1. Message dict is validated and copied
2. For process-specific channels (`!`), full channel name stored in `__asgi_channel__` field
3. Event created in database with expiry timestamp

**Receiving (Process-Specific Channels):**
1. Background polling task (`_poll_and_distribute`) continuously queries database for prefix
2. Messages routed to appropriate channel buffer (`asyncio.Queue`) based on full channel name
3. Auto-shutdown after 30 minutes of inactivity (no receivers + no messages)

**Receiving (Normal Channels):**
1. Direct polling loop queries database for channel
2. Atomic update ensures single delivery using `delivered=False` filter
3. Cleanup of expired messages during polling intervals

### Critical Implementation Details

**Concurrency Handling:**
- Uses atomic database updates to prevent duplicate delivery (src/channels_lite/layers/core.py:103-105)
- Process-specific channels use dedicated polling tasks per prefix
- `_active_receivers` counter tracks active consumers

**Expiry & Cleanup:**
- Messages expire after `expiry` seconds (default 60)
- Group memberships expire after `group_expiry` seconds (default 86400)
- Auto-trim runs during polling if `auto_trim=True`
- Inactive channels (with unread expired messages) removed from groups with 30-second grace period

**SQLite Configuration Requirements:**
- WAL mode required for better concurrency: `PRAGMA journal_mode=WAL`
- Recommended pragmas: `synchronous=NORMAL`, `busy_timeout=5000`
- Transaction mode should be `IMMEDIATE` for write operations

## Configuration

### Django Settings Example
```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_lite.layers.core.SQLiteChannelLayer",
        "OPTIONS": {
            "database": "default",  # Database alias from DATABASES setting
            "polling_interval": 0.1,
            "expiry": 60,
            "group_expiry": 86400,
        },
    },
}

# Add to INSTALLED_APPS
INSTALLED_APPS = [
    "channels_lite",  # App label is channels_lite (not channels_db)
    # ...
]

# Database configuration for channels
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "OPTIONS": {
            "transaction_mode": "IMMEDIATE",
            "timeout": 5,
            "init_command": """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
            """,
        },
    },
}
```

### Package Metadata
- Requires Python >=3.12
- Compatible with Django 4.2 - 6.0
- Main dependency: `channels[daphne,types]>=4.3.2`
- Development status: Alpha

## Known Limitations & TODO Items

See README.md for comprehensive TODO list. Key items:
- Replace bare `except:` with specific exception handling
- Add proper logging instead of print statements
- Implement retry logic for database lock timeouts
- Add buffer size limits to prevent unbounded memory growth
- Document performance characteristics vs Redis layer
