"""
AIOSQLite-based channel layer implementation.

This implementation uses aiosqlite and aiosqlitepool directly
for potentially better performance compared to Django ORM.

Requires installation with the [aio] extra:
    pip install channels-lite[aio]
"""

import asyncio
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

try:
    import aiosqlite
    from aiosqlitepool import SQLiteConnectionPool
except ImportError as e:
    raise ImportError(
        "The AIOSQLiteChannelLayer requires additional dependencies. "
        "Install them with: pip install channels-lite[aio]"
    ) from e

from channels.exceptions import ChannelFull

from . import BaseSQLiteChannelLayer, ChannelEmpty


class SQLiteLoopLayer:
    """
    Per-event-loop state container for AIOSQLiteChannelLayer.

    Each event loop gets its own instance with isolated pool and locks.
    This pattern follows channels_redis.core.RedisLoopLayer.
    """

    def __init__(self, channel_layer):
        self._lock = asyncio.Lock()
        self.channel_layer = channel_layer
        self.pool = None

    async def get_pool(self):
        """
        Lazily create the connection pool for this event loop.

        Note: SQLite only supports one writer at a time, so we use a small
        pool size to reduce lock contention across multiple event loops.
        """
        if self.pool is None:

            async def connection_factory():
                conn = await aiosqlite.connect(self.channel_layer.db_path)
                conn.row_factory = aiosqlite.Row

                # Check if user provided custom init_command in database OPTIONS
                init_command = self.channel_layer.db_settings.get("OPTIONS", {}).get(
                    "init_command",
                    """
                    PRAGMA journal_mode=WAL;
                    PRAGMA synchronous=NORMAL;
                    PRAGMA cache_size=10000;
                    PRAGMA temp_store=MEMORY;
                    PRAGMA mmap_size=268435456;
                    PRAGMA page_size=4096;
                    PRAGMA busy_timeout=30000;
                    """,
                )

                await conn.executescript(init_command)
                return conn

            # Use smaller pool size to reduce write lock contention
            # SQLite only allows one writer at a time, so large pools don't help
            effective_pool_size = min(self.channel_layer.pool_size, 3)
            self.pool = SQLiteConnectionPool(
                connection_factory, pool_size=effective_pool_size
            )

        return self.pool

    async def close(self):
        """Close the connection pool for this event loop."""
        if self.pool:
            await self.pool.close()
            self.pool = None


class AIOSQLiteChannelLayer(BaseSQLiteChannelLayer):
    """
    Channel layer backed by SQLite using aiosqlite and connection pooling.
    """

    def __init__(self, *, serializer_format="msgpack", pool_size=10, **kwargs):
        super().__init__(serializer_format=serializer_format, **kwargs)
        self.pool_size = pool_size
        self.db_path = self.db_settings["NAME"]
        # Per-event-loop state (pools, locks, etc.)
        # Follows the pattern from channels_redis.core.RedisChannelLayer
        self._layers = {}

    def _get_layer(self):
        """
        Get or create the SQLiteLoopLayer for the current event loop.
        """
        loop = asyncio.get_running_loop()
        if loop not in self._layers:
            self._layers[loop] = SQLiteLoopLayer(self)
        return self._layers[loop]

    @asynccontextmanager
    async def connection(self):
        """
        Context manager that returns a connection from the current event loop's pool.
        """
        layer = self._get_layer()
        pool = await layer.get_pool()
        async with pool.connection() as conn:
            yield conn

    async def _execute_write_with_retry(self, operation, max_retries=3):
        """
        Execute a write operation with retry logic for SQLite lock errors.

        SQLite only supports one writer at a time. With multiple event loops
        and connection pools, lock contention can occur. This helper retries
        with exponential backoff.

        Args:
            operation: Async function that takes a connection and performs writes
            max_retries: Maximum number of retry attempts

        Returns:
            Result from the operation

        Raises:
            sqlite3.OperationalError: If still locked after all retries
        """
        last_error = None
        for attempt in range(max_retries):
            try:
                async with self.connection() as conn:
                    return await operation(conn)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    last_error = e
                    if attempt < max_retries - 1:
                        # Exponential backoff: 50ms, 100ms, 200ms
                        await asyncio.sleep(0.05 * (2**attempt))
                        continue
                raise
        # If we get here, all retries failed
        if last_error:
            raise last_error

    def _to_django_datetime(self, dt=None):
        """Convert datetime to Django's ISO format string."""
        if dt is None:
            dt = datetime.now()
        # Django stores datetimes as ISO 8601 strings in SQLite
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    def _from_django_datetime(self, dt_str):
        """Convert Django's ISO format string to datetime."""
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")

    # Channel layer API

    async def send(self, channel, message, expiry=None):
        """Send a message onto a (general or specific) channel."""
        # Validate and prepare message (without capacity check)
        assert isinstance(message, dict), "message is not a dict"
        self.require_valid_channel_name(channel)
        assert "__asgi_channel__" not in message

        # Prepare message
        channel_non_local_name = channel
        prepared_message = message.copy()

        # Handle process-local channels
        if "!" in channel:
            prepared_message["__asgi_channel__"] = channel
            channel_non_local_name = self.non_local_name(channel)

        data_bytes = self.serialize(prepared_message)
        created_at = self._to_django_datetime()
        expires_at = self._to_django_datetime(
            expiry or datetime.now() + timedelta(seconds=self.expiry)
        )

        async def _send_operation(conn):
            # Check capacity (if enforcement is enabled)
            if self.enforce_capacity:
                capacity = self.get_capacity(channel)
                now = self._to_django_datetime()
                cursor = await conn.execute(
                    """
                    SELECT COUNT(*) FROM channels_lite_event
                    WHERE channel_name = ? AND delivered = 0 AND expires_at >= ?
                    """,
                    (channel_non_local_name, now),
                )
                row = await cursor.fetchone()
                pending_count = row[0] if row else 0

                if pending_count >= capacity:
                    raise ChannelFull(f"Channel {channel} is at capacity ({capacity})")

            await conn.execute(
                """
                INSERT INTO channels_lite_event (created_at, expires_at, channel_name, data, delivered)
                VALUES (?, ?, ?, ?, 0)
                """,
                (created_at, expires_at, channel_non_local_name, data_bytes),
            )
            await conn.commit()

        # Use retry wrapper for write operation
        await self._execute_write_with_retry(_send_operation)

    async def _receive_single_from_db(self, channel):
        """Pull a single message from the database for the given channel."""
        async with self.connection() as conn:
            now = self._to_django_datetime()

            # Find first non-delivered, non-expired message
            cursor = await conn.execute(
                """
                SELECT id, data FROM channels_lite_event
                WHERE channel_name = ? AND delivered = 0 AND expires_at >= ?
                ORDER BY expires_at ASC
                LIMIT 1
                """,
                (channel, now),
            )
            row = await cursor.fetchone()

            if row:
                event_id = row[0]
                data_json = row[1]

                # Mark as delivered
                await conn.execute(
                    "UPDATE channels_lite_event SET delivered = 1 WHERE id = ? AND delivered = 0",
                    (event_id,),
                )
                await conn.commit()

                # Check if update was successful
                if conn.total_changes > 0:
                    message = self.deserialize(data_json)
                    full_channel = self._extract_message_channel(message, channel)
                    return full_channel, message

            raise ChannelEmpty()

    async def _get_channel_pending_count(self, channel):
        """Get the number of pending (undelivered) messages for a channel."""
        async with self.connection() as conn:
            now = self._to_django_datetime()
            cursor = await conn.execute(
                """
                SELECT COUNT(*) FROM channels_lite_event
                WHERE channel_name = ? AND delivered = 0 AND expires_at >= ?
                """,
                (channel, now),
            )
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def clean_expired(self):
        """Remove expired events and group memberships."""
        async with self.connection() as conn:
            now = self._to_django_datetime()
            await conn.execute(
                "DELETE FROM channels_lite_event WHERE expires_at < ?", (now,)
            )
            await conn.execute(
                "DELETE FROM channels_lite_groupmembership WHERE expires_at < ?", (now,)
            )
            # remove from all groups channel with unread messages
            grace_period = self._to_django_datetime(
                datetime.now() - timedelta(seconds=30)
            )
            await conn.execute(
                """
            DELETE FROM channels_lite_groupmembership
            WHERE channel_name IN (
             SELECT events.channel_name
              FROM channels_lite_event events
               WHERE id = (
                   SELECT events2.id FROM channels_lite_event events2
                    WHERE events2.channel_name = events.channel_name
                    ORDER BY events2.created_at DESC
                    LIMIT 1
                )
                AND events.expires_at < ?
                AND events.delivered = 0
             );
            """,
                (grace_period,),
            )
            await conn.commit()

    async def flush(self):
        """Flush all messages and groups."""
        async with self.connection() as conn:
            await conn.execute("DELETE FROM channels_lite_event")
            await conn.execute("DELETE FROM channels_lite_groupmembership")
            await conn.commit()

    async def close(self):
        """Close the channel layer and clean up resources."""
        # Call parent's close to reset event loop tracking
        await super().close()

        # Close all per-event-loop pools
        for layer in list(self._layers.values()):
            await layer.close()

        # Clear the layers dict
        self._layers.clear()

    # Groups extension

    async def group_add(self, group, channel):
        """Add a channel to a group."""
        self.require_valid_group_name(group)
        self.require_valid_channel_name(channel)

        expires_at = self._to_django_datetime(
            datetime.now() + timedelta(seconds=self.group_expiry)
        )
        joined_at = self._to_django_datetime()

        async with self.connection() as conn:
            # Use INSERT OR REPLACE to handle unique constraint
            await conn.execute(
                """
                INSERT OR REPLACE INTO channels_lite_groupmembership
                (group_name, channel_name, expires_at, joined_at)
                VALUES (?, ?, ?, ?)
                """,
                (group, channel, expires_at, joined_at),
            )
            await conn.commit()

    async def group_discard(self, group, channel):
        """Remove a channel from a group."""
        self.require_valid_channel_name(channel)
        self.require_valid_group_name(group)

        async with self.connection() as conn:
            await conn.execute(
                "DELETE FROM channels_lite_groupmembership WHERE group_name = ? AND channel_name = ?",
                (group, channel),
            )
            await conn.commit()

    async def group_send(self, group, message):
        """Send a message to all channels in a group."""
        assert isinstance(message, dict), "Message is not a dict"
        self.require_valid_group_name(group)

        async def _group_send_operation(conn):
            now = self._to_django_datetime()

            # Get all channels in the group
            cursor = await conn.execute(
                """
                SELECT channel_name FROM channels_lite_groupmembership
                WHERE group_name = ? AND expires_at >= ?
                """,
                (group, now),
            )
            channels = [row[0] for row in await cursor.fetchall()]

            if not channels:
                return

            # Prepare events for bulk insert
            created_at = self._to_django_datetime()
            expiry = self._to_django_datetime(
                datetime.now() + timedelta(seconds=self.expiry)
            )
            events = []
            channels_over_capacity = 0

            for channel in channels:
                # Handle process-local channels
                if "!" in channel:
                    msg_to_send = message.copy()
                    msg_to_send["__asgi_channel__"] = channel
                    channel_name = self.non_local_name(channel)
                else:
                    msg_to_send = message
                    channel_name = channel

                # Check capacity - silently drop if over capacity (per spec)
                # Only check if enforce_capacity is enabled
                if self.enforce_capacity:
                    capacity = self.get_capacity(channel)
                    # Check capacity using the same connection
                    cursor = await conn.execute(
                        """
                        SELECT COUNT(*) FROM channels_lite_event
                        WHERE channel_name = ? AND delivered = 0 AND expires_at >= ?
                        """,
                        (channel_name, now),
                    )
                    row = await cursor.fetchone()
                    pending_count = row[0] if row else 0

                    if pending_count >= capacity:
                        channels_over_capacity += 1
                        continue

                # Serialize message to bytes
                data_bytes = self.serialize(msg_to_send)
                events.append((created_at, expiry, channel_name, data_bytes, 0))

            # Bulk insert
            if events:
                await conn.executemany(
                    """
                    INSERT INTO channels_lite_event (created_at, expires_at, channel_name, data, delivered)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    events,
                )
                await conn.commit()

        # Use retry wrapper for write operation
        await self._execute_write_with_retry(_group_send_operation)
