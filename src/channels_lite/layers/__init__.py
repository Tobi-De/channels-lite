"""
Base channel layer implementation for SQLite-based layers.
"""

import time
import asyncio
import random
import uuid
from collections import defaultdict
from copy import deepcopy

from channels.exceptions import InvalidChannelLayerError
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
        self.capacity = capacity
        self.channel_capacity = self.compile_capacities(channel_capacity or {})

        # Process-local channel support
        self.client_prefix = uuid.uuid4().hex
        self.receive_buffer = {}
        self._polling_tasks = {}
        self._polling_locks = defaultdict(asyncio.Lock)  # Lock per prefix
        self._active_receivers = defaultdict(int)

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

    async def receive(self, channel):
        """
        Receive the first message that arrives on the channel.
        If more than one coroutine is waiting, lock and start poll and distribute task
        """

        self.require_valid_channel_name(channel)
        real_channel = channel

        # For process-specific channels, use buffering mechanism
        if "!" in channel:
            real_channel = self.non_local_name(channel)
            assert real_channel.endswith(self.client_prefix + "!"), (
                "Wrong client prefix"
            )
            prefix = real_channel
            self._active_receivers[prefix] += 1

            try:
                # Start polling task if not already running (with lock to prevent races)
                async with self._polling_locks[prefix]:
                    if prefix not in self._polling_tasks:
                        self._polling_tasks[prefix] = asyncio.create_task(
                            self._poll_and_distribute(prefix)

                        )
                buff = self.receive_buffer.get(channel)
                if buff is None:
                    buff = BoundedQueue(maxsize=self.get_capacity(channel))
                    self.receive_buffer[channel] = buff
                return await buff.get()
            finally:
                self._active_receivers[prefix] -= 1

        # For normal channels, use direct polling
        while True:
            try:
                _, message = await self._receive_single_from_db(real_channel)
                return message
            except ChannelEmpty:
                # No message available, occasionally run cleanup and sleep
                if self.auto_trim and random.random() < 0.01:
                    await self._clean_expired()
                await asyncio.sleep(self.polling_interval)

    async def _poll_and_distribute(self, prefix):
        """
        Background task that polls the database for messages on a given prefix
        and distributes them to the appropriate channel buffers.

        Auto-shuts down after polling_idle_timeout seconds of inactivity (no receivers + no messages).

        This method should be called by concrete implementations.
        """

        last_activity = time.time()

        try:
            while True:
                # Check shutdown condition: no active receivers and idle timeout exceeded
                if (
                    self._active_receivers[prefix] == 0
                    and time.time() - last_activity > self.polling_idle_timeout
                ):
                    # Clean up and exit
                    if prefix in self._polling_tasks:
                        del self._polling_tasks[prefix]
                    if prefix in self._active_receivers:
                        del self._active_receivers[prefix]
                    if prefix in self._polling_locks:
                        del self._polling_locks[prefix]
                    return

                try:
                    # Pull one message from database
                    # Subclasses implement _receive_single_from_db
                    msg_channel, message = await self._receive_single_from_db(prefix)

                    # Route to the appropriate buffer based on full channel name
                    buff = self.receive_buffer.get(msg_channel)
                    if buff is None:
                        buff = BoundedQueue(maxsize=self.get_capacity(msg_channel))
                        self.receive_buffer[msg_channel] = buff
                    await buff.put(message)
                    last_activity = time.time()  # Reset idle timer

                except ChannelEmpty:
                    # No message available, occasionally run cleanup and sleep
                    if self.auto_trim and random.random() < 0.01:
                        await self._clean_expired()
                    await asyncio.sleep(self.polling_interval)

        except asyncio.CancelledError:
            # Task was cancelled, clean up
            if prefix in self._polling_tasks:
                del self._polling_tasks[prefix]
            if prefix in self._polling_locks:
                del self._polling_locks[prefix]
            raise

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

    async def _clean_expired(self):
        """
        Remove expired events and group memberships.

        This is an abstract method that should be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement _clean_expired")

    # Helper methods for common send/receive patterns

    def _prepare_message_for_send(self, channel, message):
        """
        Validate and prepare message for sending.

        Returns:
            tuple: (channel_non_local_name, prepared_message)
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

    # Serialization methods

    def serialize(self, message):
        """Serializes message to a byte string."""
        return self._serializer.serialize(message)

    def deserialize(self, message):
        """Deserializes from a byte string."""
        return self._serializer.deserialize(message)
