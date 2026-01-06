"""
Base channel layer implementation for SQLite-based layers.
"""

import asyncio
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
        self.auto_trim = auto_trim

        # Process-local channel support
        self.client_prefix = uuid.uuid4().hex
        self.receive_buffer = defaultdict(lambda: BoundedQueue(maxsize=self.capacity))
        self._polling_tasks = {}
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
