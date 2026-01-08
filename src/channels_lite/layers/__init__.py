"""
Base channel layer implementation for SQLite-based layers.
"""

import asyncio
import random
import time
import uuid
from collections import defaultdict
from copy import deepcopy

from channels.exceptions import InvalidChannelLayerError, ChannelFull
from channels.layers import BaseChannelLayer
from django.conf import settings

from ..serializers import registry


class ChannelEmpty(Exception):
    """Exception raised when a channel is empty."""

    pass


class BoundedQueue(asyncio.Queue):
    """
    A queue that drops the oldest message when full.

    This prevents unbounded memory growth when consumers stop reading.
    """

    def put_nowait(self, item):
        if self.full():
            # Drop the oldest message to make room
            self.get_nowait()
        return super().put_nowait(item)


class BaseSQLiteChannelLayer(BaseChannelLayer):
    """
    Base class for SQLite-based channel layers.

    Provides common functionality for both Django ORM-based and
    aiosqlite-based implementations.
    """

    def __init__(
        self,
        *,
        database,
        expiry=60,
        capacity=100,
        channel_capacity=None,
        group_expiry=86400,
        polling_interval=0.1,
        polling_idle_timeout=1800,
        auto_trim=True,
        enforce_capacity=True,
        serializer_format="json",
        symmetric_encryption_keys=None,
        **kwargs,
    ):
        super().__init__(
            expiry=expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
            **kwargs,
        )

        # Database configuration
        self.database = database
        try:
            self.db_settings = settings.DATABASES[self.database]
            assert "sqlite3" in self.db_settings["ENGINE"]
        except KeyError:
            raise InvalidChannelLayerError(
                f"{self.database} is an invalid database alias"
            )
        except AssertionError:
            raise InvalidChannelLayerError(
                "SQLite database engine is required to use this channel layer"
            )
        # Timing and cleanup
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.polling_interval = polling_interval
        self.polling_idle_timeout = polling_idle_timeout
        self.auto_trim = auto_trim
        self.enforce_capacity = enforce_capacity
        self.capacity = capacity
        self.channel_capacity = self.compile_capacities(channel_capacity or {})

        # Process-local channel support
        self.client_prefix = uuid.uuid4().hex

        # Buffering and polling infrastructure
        self.receive_buffer = {}  # Dict[channel_name, BoundedQueue]
        self._polling_tasks = {}  # Dict[prefix, asyncio.Task]
        self._polling_locks = defaultdict(asyncio.Lock)  # Lock per prefix
        self._active_receivers = defaultdict(int)  # Active receivers per prefix

        # Event loop tracking for receive (single event loop enforcement)
        # Follows the pattern from channels_redis.core.RedisChannelLayer
        self.receive_count = 0
        self.receive_event_loop = None

        # Serialization
        self._serializer = registry.get_serializer(
            serializer_format,
            symmetric_encryption_keys=symmetric_encryption_keys,
            expiry=self.expiry,
        )

    extensions = ["groups", "flush"]

    async def new_channel(self, prefix="specific"):
        """
        Returns a new channel name that can be used by something in our
        process as a specific channel.
        """
        return f"{prefix}.{self.client_prefix}!{uuid.uuid4().hex}"

    async def close(self):
        """Clean up any running background polling tasks."""
        # Cancel all polling tasks
        for task in list(self._polling_tasks.values()):
            task.cancel()

        # Wait for all tasks to complete cancellation
        if self._polling_tasks:
            await asyncio.gather(*self._polling_tasks.values(), return_exceptions=True)

        # Clear tracking dictionaries
        self._polling_tasks.clear()
        self._active_receivers.clear()
        self.receive_buffer.clear()

        # Reset event loop tracking
        self.receive_count = 0
        self.receive_event_loop = None

    async def _execute_with_retry(self, operation, max_retries=3):
        """
        Execute an async operation with retry logic for SQLite lock errors.

        SQLite only supports one writer at a time. Under high load, multiple
        concurrent writes can cause lock contention. This helper retries
        operations with exponential backoff.

        Works with both Django ORM (OperationalError) and aiosqlite (sqlite3.OperationalError).

        Args:
            operation: Async callable (function, lambda, or coroutine function)
            max_retries: Maximum number of retry attempts

        Returns:
            Result from the operation

        Raises:
            Exception: Original exception if not a lock error or retries exhausted
        """
        last_error = None
        for attempt in range(max_retries):
            try:
                return await operation()
            except Exception as e:
                # Check for SQLite lock errors (works for both sqlite3 and Django)
                error_str = str(e).lower()
                if "database is locked" in error_str or "locked" in error_str:
                    last_error = e
                    if attempt < max_retries - 1:
                        # Exponential backoff: 50ms, 100ms, 200ms
                        await asyncio.sleep(0.05 * (2**attempt))
                        continue
                # Not a lock error, re-raise immediately
                raise
        # If we get here, all retries failed with lock errors
        if last_error:
            raise last_error

    async def receive(self, channel):
        """
        Receive the first message that arrives on the channel.
        Uses direct database polling for all channel types.
        """
        self.require_valid_channel_name(channel)
        real_channel = channel

        # For process-specific channels, verify client prefix
        if "!" in channel:
            real_channel = self.non_local_name(channel)
            assert real_channel.endswith(self.client_prefix + "!"), (
                "Wrong client prefix"
            )
            prefix = real_channel

            # Enforce single event loop for receive
            # Follows the pattern from channels_redis.core.RedisChannelLayer
            loop = asyncio.get_running_loop()
            self.receive_count += 1

            try:
                if self.receive_count == 1:
                    # First receiver - record the event loop
                    self.receive_event_loop = loop
                else:
                    # Subsequent receivers - verify same event loop
                    if self.receive_event_loop != loop:
                        raise RuntimeError(
                            "Cannot receive on process-specific channels from multiple event loops. "
                        )

                self._active_receivers[prefix] += 1

                try:
                    # Start polling task if not running (with lock to prevent races)
                    async with self._polling_locks[prefix]:
                        if (
                            prefix not in self._polling_tasks
                            or self._polling_tasks[prefix].done()
                        ):
                            # Previous task died or never started - (re)start it
                            self._polling_tasks[prefix] = asyncio.create_task(
                                self._poll_and_distribute(prefix)
                            )

                    # Get or create buffer for this channel
                    buff = self.receive_buffer.get(channel)
                    if buff is None:
                        buff = BoundedQueue(maxsize=self.get_capacity(channel))
                        self.receive_buffer[channel] = buff

                    return await buff.get()
                finally:
                    self._active_receivers[prefix] -= 1

            finally:
                self.receive_count -= 1
                # If we're the last receiver, clear the event loop tracking
                if self.receive_count == 0:
                    self.receive_event_loop = None

        # For normal channels, use direct polling
        while True:
            try:
                _, message = await self._receive_single_from_db(real_channel)
                return message
            except ChannelEmpty:
                # No message available, occasionally run cleanup and sleep
                if self.auto_trim and random.random() < 0.01:
                    await self.clean_expired()
                await asyncio.sleep(self.polling_interval)

    async def _receive_single_from_db(self, channel):
        """
        Pull a single message from the database for the given channel.

        This is an abstract method that must be implemented by subclasses.

        Args:
            channel: The channel name to receive from

        Returns:
            tuple: (full_channel_name, message_dict)

        Raises:
            ChannelEmpty: If no message is available
        """
        raise NotImplementedError("Subclasses must implement _receive_single_from_db")

    async def _poll_and_distribute(self, prefix):
        """
        Background task that polls database for messages on a prefix
        and distributes them to appropriate channel buffers.

        Auto-shuts down after polling_idle_timeout seconds of inactivity
        (no active receivers and no messages).

        Integrates with _execute_with_retry() for database lock handling.
        """
        last_activity = time.time()

        try:
            while True:
                # Check shutdown: no active receivers + idle timeout exceeded
                if (
                    self._active_receivers[prefix] == 0
                    and time.time() - last_activity > self.polling_idle_timeout
                ):
                    # Clean up buffers for this prefix
                    for channel_name in list(self.receive_buffer.keys()):
                        if channel_name.startswith(prefix):
                            del self.receive_buffer[channel_name]

                    # Clean up task tracking
                    if prefix in self._polling_tasks:
                        del self._polling_tasks[prefix]
                    if prefix in self._active_receivers:
                        del self._active_receivers[prefix]
                    if prefix in self._polling_locks:
                        del self._polling_locks[prefix]
                    return

                try:
                    # Pull one message with retry wrapper
                    async def _db_operation():
                        return await self._receive_single_from_db(prefix)

                    msg_channel, message = await self._execute_with_retry(_db_operation)

                    # Route to appropriate buffer
                    buff = self.receive_buffer.get(msg_channel)
                    if buff is None:
                        buff = BoundedQueue(maxsize=self.get_capacity(msg_channel))
                        self.receive_buffer[msg_channel] = buff
                    await buff.put(message)
                    last_activity = time.time()  # Reset idle timer

                except ChannelEmpty:
                    # No message available
                    if self.auto_trim and random.random() < 0.01:
                        await self.clean_expired()
                    await asyncio.sleep(self.polling_interval)
                except Exception as e:
                    # Log error but keep task alive for robustness
                    # TODO: Replace with proper logging once logging is implemented
                    print(f"Error in _poll_and_distribute for {prefix}: {e}")
                    await asyncio.sleep(self.polling_interval)

        except asyncio.CancelledError:
            # Task was cancelled during close(), clean up
            if prefix in self._polling_tasks:
                del self._polling_tasks[prefix]
            if prefix in self._polling_locks:
                del self._polling_locks[prefix]
            raise

    async def _get_channel_pending_count(self, channel):
        """
        Get the number of pending (undelivered) messages for a channel.

        This is an abstract method that must be implemented by subclasses.

        Args:
            channel: The channel name to check

        Returns:
            int: Number of pending messages
        """
        raise NotImplementedError(
            "Subclasses must implement _get_channel_pending_count"
        )

    async def clean_expired(self):
        """
        Remove expired events and group memberships.

        This is a public API method that can be called manually for maintenance.
        It's also called automatically during polling if auto_trim is enabled.

        This is an abstract method that should be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement clean_expired")

    # Helper methods for common send/receive patterns

    async def _prepare_message_for_send(self, channel, message):
        """
        Validate and prepare message for sending.

        Returns:
            tuple: (channel_non_local_name, prepared_message)

        Raises:
            ChannelFull: If the channel is at or over capacity
        """
        # Validation
        assert isinstance(message, dict), "message is not a dict"
        self.require_valid_channel_name(channel)
        assert "__asgi_channel__" not in message

        # Prepare message
        channel_non_local_name = channel
        prepared_message = deepcopy(message)

        # Handle process-local channels
        if "!" in channel:
            prepared_message["__asgi_channel__"] = channel
            channel_non_local_name = self.non_local_name(channel)

        # Check capacity (if enforcement is enabled)
        if self.enforce_capacity:
            capacity = self.get_capacity(channel)
            pending_count = await self._get_channel_pending_count(
                channel_non_local_name
            )
            if pending_count >= capacity:
                raise ChannelFull(f"Channel {channel} is at capacity ({capacity})")

        return channel_non_local_name, prepared_message

    def _extract_message_channel(self, message, default_channel):
        """
        Extract full channel name from message and remove __asgi_channel__ key.

        Args:
            message: The deserialized message dict
            default_channel: The channel to use if __asgi_channel__ is not present

        Returns:
            str: The full channel name
        """
        full_channel = default_channel
        if "__asgi_channel__" in message:
            full_channel = message["__asgi_channel__"]
            del message["__asgi_channel__"]
        return full_channel

    async def _prepare_group_send_events(self, channels, message):
        """
        Async generator that yields prepared event data for each channel in a group.

        Args:
            channels: List of channel names in the group
            message: The message dict to send

        Yields:
            tuple: (channel_name, serialized_data_bytes) for each valid channel
        """
        for channel in channels:
            # Handle process-local channels (with "!")
            if "!" in channel:
                msg_to_send = message.copy()
                msg_to_send["__asgi_channel__"] = channel
                channel_name = self.non_local_name(channel)
            else:
                msg_to_send = message
                channel_name = channel

            # Check capacity - silently drop if over capacity (per spec)
            if self.enforce_capacity:
                capacity = self.get_capacity(channel)
                pending_count = await self._get_channel_pending_count(channel_name)
                if pending_count >= capacity:
                    continue  # Skip this channel

            # Serialize message to bytes
            data_bytes = self.serialize(msg_to_send)
            yield (channel_name, data_bytes)

    # Serialization methods

    def serialize(self, message):
        """Serializes message to a byte string."""
        return self._serializer.serialize(message)

    def deserialize(self, message):
        """Deserializes from a byte string."""
        return self._serializer.deserialize(message)
