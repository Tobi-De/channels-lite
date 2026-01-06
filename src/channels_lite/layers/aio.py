"""
AIOSQLite-based channel layer implementation.

This implementation uses aiosqlite and aiosqlitepool directly
for potentially better performance compared to Django ORM.

Requires installation with the [aio] extra:
    pip install channels-lite[aio]
"""

import asyncio
import random
from copy import deepcopy
from datetime import datetime, timedelta

try:
    import aiosqlite
    from aiosqlitepool import SQLiteConnectionPool
except ImportError as e:
    raise ImportError(
        "The AIOSQLiteChannelLayer requires additional dependencies. "
        "Install them with: pip install channels-lite[aio]"
    ) from e

from . import BaseSQLiteChannelLayer, ChannelEmpty


class AIOSQLiteChannelLayer(BaseSQLiteChannelLayer):
    """
    Channel layer backed by SQLite using aiosqlite and connection pooling.
    """

    def __init__(
        self,
        *,
        database,
        expiry=60,
        capacity=100,
        channel_capacity=None,
        group_expiry=86400,
        pool_size=10,
        polling_interval=0.1,
        auto_trim=True,
        serializer_format="msgpack",
        symmetric_encryption_keys=None,
        **kwargs,
    ):
        super().__init__(
            database=database,
            expiry=expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
            group_expiry=group_expiry,
            polling_interval=polling_interval,
            auto_trim=auto_trim,
            serializer_format=serializer_format,
            symmetric_encryption_keys=symmetric_encryption_keys,
            **kwargs,
        )
        self.pool_size = pool_size
        self.pool = None
        # Get database path from Django settings (db_settings already set by parent)
        self.db_path = self.db_settings["NAME"]

    async def _ensure_pool(self):
        """Ensure the connection pool is initialized."""
        if self.pool is None:

            async def connection_factory():
                conn = await aiosqlite.connect(self.db_path)
                conn.row_factory = aiosqlite.Row

                # Check if user provided custom init_command in database OPTIONS
                init_command = self.db_settings.get("OPTIONS", {}).get("init_command")

                if init_command:
                    # Use user-provided init commands (supports multiple statements)
                    await conn.executescript(init_command)
                else:
                    # Use default optimized PRAGMA settings
                    await conn.executescript(
                        """
                        PRAGMA journal_mode=WAL;
                        PRAGMA synchronous=NORMAL;
                        PRAGMA cache_size=10000;
                        PRAGMA temp_store=MEMORY;
                        PRAGMA mmap_size=268435456;
                        PRAGMA page_size=4096;
                        PRAGMA busy_timeout=5000;
                        """
                    )

                return conn

            self.pool = SQLiteConnectionPool(
                connection_factory, pool_size=self.pool_size
            )

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
        assert isinstance(message, dict), "message is not a dict"
        self.require_valid_channel_name(channel)

        await self._ensure_pool()

        # Handle process-local channels
        assert "__asgi_channel__" not in message
        channel_non_local_name = channel

        # Only deepcopy if needed for process-local channels
        if "!" in channel:
            msg_to_send = deepcopy(message)
            msg_to_send["__asgi_channel__"] = channel
            channel_non_local_name = self.non_local_name(channel)
        else:
            msg_to_send = message

        created_at = self._to_django_datetime()
        if expiry:
            expires_at = self._to_django_datetime(expiry)
        else:
            expires_at = self._to_django_datetime(
                datetime.now() + timedelta(seconds=self.expiry)
            )
        # Serialize message to bytes
        data_bytes = self.serialize(msg_to_send)

        async with self.pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO channels_lite_event (created_at, expires_at, channel_name, data, delivered)
                VALUES (?, ?, ?, ?, 0)
                """,
                (created_at, expires_at, channel_non_local_name, data_bytes),
            )
            await conn.commit()

    async def receive(self, channel):
        """Receive the first message that arrives on the channel."""
        self.require_valid_channel_name(channel)
        await self._ensure_pool()

        real_channel = channel
        if "!" in channel:
            real_channel = self.non_local_name(channel)
            assert real_channel.endswith(self.client_prefix + "!"), (
                "Wrong client prefix"
            )

        # For process-specific channels, use buffering mechanism
        if "!" in channel:
            prefix = real_channel
            self._active_receivers[prefix] += 1

            try:
                # Start polling task if not already running
                if prefix not in self._polling_tasks:
                    self._polling_tasks[prefix] = asyncio.create_task(
                        self._poll_process_channel(prefix)
                    )
                return await self.receive_buffer[channel].get()
            except asyncio.CancelledError:
                # Task was cancelled, clean up
                if prefix in self._polling_tasks:
                    del self._polling_tasks[prefix]
                raise
            finally:
                self._active_receivers[prefix] -= 1

        # Regular channels
        try:
            channel_name, message = await self._receive_single_from_db(real_channel)

            # Clean expired messages periodically
            if self.auto_trim and random.random() < 0.01:
                await self._clean_expired()

            return message
        except ChannelEmpty:
            if self.auto_trim and random.random() < 0.01:
                await self._clean_expired()
            raise

    async def _receive_single_from_db(self, channel):
        """Pull a single message from the database for the given channel."""
        async with self.pool.connection() as conn:
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
                    # Deserialize message data
                    message = self.deserialize(data_json)
                    return channel, message

            raise ChannelEmpty()

    async def _poll_process_channel(self, prefix):
        """Poll the database for messages destined for process-local channels."""
        while self._active_receivers[prefix] > 0:
            try:
                channel_name, message = await self._receive_single_from_db(prefix)
                target_channel = message.get("__asgi_channel__")

                if target_channel and target_channel in self.receive_buffer:
                    await self.receive_buffer[target_channel].put(message)
            except ChannelEmpty:
                await asyncio.sleep(self.polling_interval)  # Wait before polling again
            except Exception:
                # Log error in production, for now just continue
                await asyncio.sleep(self.polling_interval)

    async def _clean_expired(self):
        """Remove expired events and group memberships."""
        async with self.pool.connection() as conn:
            now = self._to_django_datetime()
            await conn.execute(
                "DELETE FROM channels_lite_event WHERE expires_at < ?", (now,)
            )
            await conn.execute(
                "DELETE FROM channels_lite_groupmembership WHERE expires_at < ?", (now,)
            )
            await conn.commit()

    async def flush(self):
        """Flush all messages and groups."""
        await self._ensure_pool()
        async with self.pool.connection() as conn:
            await conn.execute("DELETE FROM channels_lite_event")
            await conn.execute("DELETE FROM channels_lite_groupmembership")
            await conn.commit()

        # Clear local state
        self._polling_tasks.clear()
        self._active_receivers.clear()
        self.receive_buffer.clear()

    async def close(self):
        """Close the channel layer and clean up resources."""
        # Close the connection pool first
        if self.pool:
            await self.pool.close()
            self.pool = None

        # Call parent's close to handle task cancellation and cleanup
        await super().close()

    # Groups extension

    async def group_add(self, group, channel):
        """Add a channel to a group."""
        self.require_valid_group_name(group)
        self.require_valid_channel_name(channel)
        await self._ensure_pool()

        expires_at = self._to_django_datetime(
            datetime.now() + timedelta(seconds=self.group_expiry)
        )
        joined_at = self._to_django_datetime()

        async with self.pool.connection() as conn:
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
        await self._ensure_pool()

        async with self.pool.connection() as conn:
            await conn.execute(
                "DELETE FROM channels_lite_groupmembership WHERE group_name = ? AND channel_name = ?",
                (group, channel),
            )
            await conn.commit()

    async def group_send(self, group, message):
        """Send a message to all channels in a group."""
        assert isinstance(message, dict), "Message is not a dict"
        self.require_valid_group_name(group)
        await self._ensure_pool()

        # Get all channels in the group
        async with self.pool.connection() as conn:
            now = self._to_django_datetime()
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

        for channel in channels:
            # Handle process-local channels
            if "!" in channel:
                msg = deepcopy(message)
                msg["__asgi_channel__"] = channel
                channel_name = self.non_local_name(channel)
            else:
                msg = deepcopy(message)
                channel_name = channel

            # Serialize message to bytes
            data_bytes = self.serialize(msg)
            events.append((created_at, expiry, channel_name, data_bytes, 0))

        # Bulk insert
        async with self.pool.connection() as conn:
            await conn.executemany(
                """
                INSERT INTO channels_lite_event (created_at, expires_at, channel_name, data, delivered)
                VALUES (?, ?, ?, ?, ?)
                """,
                events,
            )
            await conn.commit()
