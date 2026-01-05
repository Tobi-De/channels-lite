# channels-lite

Django Channels layer backed by SQLite database.

## Installation

### Basic Installation (Django ORM Layer)

```bash
pip install channels-lite
```

This installs the Django ORM-based channel layer that works with your existing Django database.

### High-Performance Installation (AioSQLite Layer)

```bash
pip install channels-lite[aio]
```

This installs the aiosqlite-based layer with `aiosqlite`, `aiosqlitepool`, and `msgspec` for better performance through direct async SQLite access and MessagePack encoding.

## Usage

### Django ORM Layer

Configure in your Django settings:

```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_lite.layers.core.SqliteChannelLayer",
        "CONFIG": {
            "database": "default",  # Django database alias
            "expiry": 60,
            "capacity": 100,
        },
    },
}
```

### AioSQLite Layer (High Performance)

```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_lite.layers.aio.AioSqliteChannelLayer",
        "CONFIG": {
            "db_path": "channels.db",  # Direct database file path
            "expiry": 60,
            "capacity": 100,
            "pool_size": 10,
            "polling_interval": 0.1,
        },
    },
}
```

## Features

- **Two implementation options**: Django ORM (easy setup) or aiosqlite (high performance)
- **Process-local channel support**: Unique channel names per process
- **Group messaging**: Send messages to multiple channels at once
- **Message expiry**: Automatic cleanup of expired messages
- **Connection pooling**: (aiosqlite layer) Efficient connection management
- **MessagePack encoding**: (aiosqlite layer) Fast binary serialization

## TODO

### High Priority (Robustness)

- [ ] Replace bare `except:` clauses with specific exception handling (`ChannelEmpty`, `OperationalError`, etc.)
- [ ] Replace print statements with proper logging (use `logging` module)
- [ ] Add retry logic for database OperationalError (lock timeouts)
- [ ] Implement receive cancellation handling to prevent message loss
- [ ] Add buffer size limits to prevent unbounded memory growth
- [ ] Implement periodic VACUUM for database maintenance (reclaim space from deleted messages)

### Documentation Needed

- [ ] Document SQLite WAL mode requirement for better concurrency
  - Users must configure: `PRAGMA journal_mode=WAL`
  - Recommended settings: `PRAGMA synchronous=NORMAL`, `PRAGMA busy_timeout=5000`
- [ ] Document Django database configuration requirements
- [ ] Add usage examples and setup guide
- [ ] Document limitations vs Redis layer

### Future Enhancements

- [x] Create advanced layer using `aiosqlite` + `aiosqlitepool` for better async support ✅
  - Direct connection pooling ✅
  - Better concurrent write handling ✅
  - More control over SQLite pragmas ✅
- [ ] Consider adding encryption support for sensitive messages
- [ ] Add health check endpoint for monitoring
- [ ] Add metrics/instrumentation for observability

### Performance Optimizations

- [ ] Profile and optimize hot paths (send/receive loops)
- [ ] Consider batch operations for group_send
- [ ] Evaluate index optimization for Event and GroupMembership tables
- [ ] Benchmark against Redis layer for common workloads
