# channels-lite

[![PyPI - Version](https://img.shields.io/pypi/v/channels-lite.svg)](https://pypi.org/project/channels-lite)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/channels-lite.svg)](https://pypi.org/project/channels-lite)
[![PyPI - Versions from Framework Classifiers](https://img.shields.io/pypi/frameworkversions/django/channels-lite)](https://pypi.org/project/channels-lite/)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/Tobi-De/channels-lite/blob/main/LICENSE.txt)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Django Channels layer backed by SQLite database.

**channels-lite** provides Django Channels channel layers that use SQLite as a backing store, eliminating the need for Redis infrastructure. Perfect for small to medium Django applications, development environments, or deployments where simplicity is valued.

There are two available implementations:

* **`SQLiteChannelLayer`** is the Django ORM-based layer, easy to set up with no extra dependencies.
* **`AIOSQLiteChannelLayer`** uses aiosqlite with connection pooling for better performance through direct async SQLite access.

Both layers support the full Channels layer specification including groups, flush, and capacity enforcement.


## Installation

### Basic Installation (Django ORM Layer)

```bash
pip install channels-lite
```

This installs the Django ORM-based channel layer.

### High-Performance Installation (AIOSQLite Layer)

```bash
pip install channels-lite[aio]
```

This installs the aiosqlite-based layer with `aiosqlite`, `aiosqlitepool`, and `msgspec` for better performance.

## Usage

Set up the channel layer in your Django settings file like so:

### Django ORM Layer

```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_lite.layers.core.SQLiteChannelLayer",
        "CONFIG": {
            "database": "default",
        },
    },
}
```

### AIOSQLite Layer (High Performance)

```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_lite.layers.aio.AIOSQLiteChannelLayer",
        "CONFIG": {
            "database": "default",
        },
    },
}
```

## Configuration Options

### Common Options (Both Layers)

#### `database`

**Required.** The Django database alias to use for channel storage. Must be a SQLite database.

```python
"CONFIG": {
    "database": "default",  # or "channels" for separate database
}
```

If using a separate database for channels (**recommended**), configure it in `DATABASES`:

```python
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    },
    "channels": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "channels.sqlite3",
        "OPTIONS": {
            "init_command": (
                "PRAGMA journal_mode=WAL; "
                "PRAGMA synchronous=NORMAL; "
                "PRAGMA busy_timeout=5000;"
            ),
        },
    },
}
```

Don't forget to add `channels_lite` to `INSTALLED_APPS` and run migrations:

```python
INSTALLED_APPS = [
    # ...
    "channels_lite",
]
```

```bash
python manage.py migrate
```

#### `expiry`

Message expiry in seconds. Defaults to `60`. Messages that remain undelivered after this time are automatically cleaned up.

```python
"CONFIG": {
    "database": "default",
    "expiry": 60,
}
```

You generally shouldn't need to change this, but you may want to turn it down if you have peaky traffic you wish to drop, or up if you have peaky traffic you want to backlog until you get to it.

#### `group_expiry`

Group membership expiry in seconds. Defaults to `86400` (24 hours). Channels will be removed from groups after this amount of time; it's recommended you reduce it for a healthier system that encourages disconnections.

```python
"CONFIG": {
    "database": "default",
    "group_expiry": 86400,
}
```

This value should not be lower than the relevant timeouts in the interface server (e.g. the `--websocket-timeout` to daphne).

#### `capacity`

Default channel capacity. Defaults to `100`. Once a channel is at capacity, it will refuse more messages by raising `ChannelFull`.

```python
"CONFIG": {
    "database": "default",
    "capacity": 100,
}
```

How this affects different parts of the system varies; a HTTP server will refuse connections, for example, while Django sending a response will raise an exception.

#### `enforce_capacity`

Whether to enforce capacity limits. Defaults to `True`. When `False`, allows unlimited message accumulation in the database (capacity checks are skipped).

```python
"CONFIG": {
    "database": "default",
    "capacity": 100,
    "enforce_capacity": True,  # Set to False to disable capacity checks
}
```

Disabling capacity enforcement may be useful for deployments where database growth is not a concern and you want guaranteed message delivery without backpressure.

#### `channel_capacity`

Per-channel capacity configuration. This lets you tweak the channel capacity based on the channel name, and supports glob patterns.

It should be a dict mapping channel name pattern (glob-style) to desired capacity:

```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_lite.layers.core.SQLiteChannelLayer",
        "CONFIG": {
            "database": "default",
            "capacity": 100,
            "channel_capacity": {
                "http.request": 200,
                "http.response!*": 10,
                "websocket.send!*": 20,
            },
        },
    },
}
```

This example sets `http.request` to 200, all `http.response!` channels to 10, and all `websocket.send!` channels to 20.

#### `auto_trim`

Whether to automatically clean up expired messages during polling. Defaults to `True`. When enabled, expired messages and group memberships are periodically removed during normal operation.

```python
"CONFIG": {
    "database": "default",
    "auto_trim": True,
}
```

You can also manually trigger cleanup:

```python
from channels.layers import get_channel_layer

channel_layer = get_channel_layer()
await channel_layer.clean_expired()
```

Or use the management command:

```bash
python manage.py clean_channels
```

#### `symmetric_encryption_keys`

Pass this to enable optional symmetric encryption of messages. Requires the `cryptography` package:

```bash
pip install channels-lite[cryptography]
```

```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_lite.layers.core.SQLiteChannelLayer",
        "CONFIG": {
            "database": "default",
            "symmetric_encryption_keys": [SECRET_KEY],
        },
    },
}
```

`symmetric_encryption_keys` should be a list of strings, with each string being an encryption key. The first key is always used for encryption; all are considered for decryption, so you can rotate keys without downtime.

Keys **should have at least 32 bytes of entropy** - they are passed through the SHA256 hash function before being used as an encryption key.

Data is encrypted both at rest in the database and during serialization. The channel and group key names are not encrypted.

#### `serializer_format`

Serialization format for messages. Defaults to `"json"` for the ORM layer and `"msgpack"` for the AIO layer.

```python
"CONFIG": {
    "database": "default",
    "serializer_format": "json",  # or "msgpack"
}
```

**Note:** MessagePack requires `msgspec` package (included with `[aio]` extra).

#### `polling_interval`

Time in seconds between database polls when waiting for messages. Defaults to `0.1`.

```python
"CONFIG": {
    "database": "default",
    "polling_interval": 0.1,
}
```

Lower values reduce latency but increase database load. Higher values save resources but increase message delivery delay.

#### `polling_idle_timeout`

Time in seconds before an idle polling task shuts down. Defaults to `1800` (30 minutes).

```python
"CONFIG": {
    "database": "default",
    "polling_idle_timeout": 1800,
}
```

Process-specific channels use background polling tasks. These automatically shut down after this timeout to free resources.

### AIOSQLite Layer Specific Options

#### `pool_size`

Number of connections in the connection pool. Defaults to `10`.

```python
"CONFIG": {
    "database": "default",
    "pool_size": 10,
}
```

Larger pool sizes allow more concurrent operations but use more file descriptors.

## Database Configuration

### SQLite Performance Settings

For best performance, configure your SQLite database with these settings:

```python
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
        "OPTIONS": {
            "transaction_mode": "IMMEDIATE",  # Better for write-heavy workloads
            "timeout": 5,  # 5 second busy timeout
            "init_command": (
                "PRAGMA journal_mode=WAL; "      # Write-Ahead Logging for concurrency
                "PRAGMA synchronous=NORMAL; "     # Balanced durability/performance
                "PRAGMA cache_size=10000; "       # ~40MB cache
                "PRAGMA temp_store=MEMORY; "      # Temp tables in memory
                "PRAGMA busy_timeout=5000;"       # 5 second lock timeout
            ),
        },
    },
}
```

**Important:** WAL mode (`journal_mode=WAL`) is required for better concurrency when multiple processes access the database.

## Features

- **Full Channels Layer Specification**: Supports channels, groups, flush, and capacity enforcement
- **No External Dependencies**: Uses SQLite - no Redis installation required
- **Two Implementation Options**: 
  - Django ORM layer (simple setup, integrates with Django)
  - AIOSQLite layer (better performance, connection pooling)
- **Process-Specific Channels**: Unique channel names per process for scaling
- **Group Messaging**: Broadcast messages to multiple channels efficiently
- **Message Expiry**: Automatic cleanup of expired messages
- **Capacity Enforcement**: Backpressure mechanism matching Redis layer behavior
- **Encryption Support**: Optional symmetric encryption for sensitive data
- **Database Router**: Automatically routes channel tables to correct database

## Management Commands

### Clean Expired Messages

Remove expired messages and group memberships, and optionally optimize the database:

```bash
python manage.py clean_channels
```

This command performs two maintenance tasks:
1. Removes expired messages and group memberships from the database
2. Runs SQLite VACUUM to reclaim disk space and optimize the database

Options:
- `--no-vacuum`: Skip the VACUUM operation (only clean expired messages)

Examples:
```bash
# Full cleanup with VACUUM
python manage.py clean_channels

# Only clean expired messages, skip VACUUM
python manage.py clean_channels --no-vacuum
```

**Note:** The VACUUM operation can take time on large databases and briefly locks the database. Use `--no-vacuum` if you need to run cleanup frequently or during high-traffic periods.
