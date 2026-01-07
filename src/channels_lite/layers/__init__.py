"""
Base channel layer implementation for SQLite-based layers.
"""

import asyncio
import random
import uuid
from copy import deepcopy

from channels.exceptions import InvalidChannelLayerError, ChannelFull
from channels.layers import BaseChannelLayer
from django.conf import settings

from ..serializers import registry


class ChannelEmpty(Exception):
    """Exception raised when a channel is empty."""

    pass


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
        """Clean up channel layer resources."""
        # Reset event loop tracking
        self.receive_count = 0
        self.receive_event_loop = None

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
                            "This typically happens when using async_to_sync() with receive(). "
                            "Use async_to_sync() only for send/group_send operations."
                        )

                # Direct polling loop
                while True:
                    try:
                        _, message = await self._receive_single_from_db(real_channel)
                        return message
                    except ChannelEmpty:
                        # No message available, occasionally run cleanup and sleep
                        if self.auto_trim and random.random() < 0.01:
                            await self.clean_expired()
                        await asyncio.sleep(self.polling_interval)

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

    # Serialization methods

    def serialize(self, message):
        """Serializes message to a byte string."""
        return self._serializer.serialize(message)

    def deserialize(self, message):
        """Deserializes from a byte string."""
        return self._serializer.deserialize(message)
