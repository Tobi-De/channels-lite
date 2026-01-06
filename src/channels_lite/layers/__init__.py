"""
Base channel layer implementation for SQLite-based layers.
"""

import asyncio
import uuid
from collections import defaultdict

from channels.exceptions import InvalidChannelLayerError
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
        self._validate_database()

        # Timing and cleanup
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.polling_interval = polling_interval
        self.auto_trim = auto_trim

        # Process-local channel support
        self.client_prefix = uuid.uuid4().hex
        self.receive_buffer = defaultdict(lambda: asyncio.Queue())
        self._polling_tasks = {}
        self._active_receivers = defaultdict(int)

        # Serialization
        self._serializer = registry.get_serializer(
            serializer_format,
            symmetric_encryption_keys=symmetric_encryption_keys,
            expiry=self.expiry,
        )

    def _validate_database(self):
        """Validate that the database alias exists and is SQLite."""
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

    def serialize(self, message):
        """Serializes message to a byte string."""
        return self._serializer.serialize(message)

    def deserialize(self, message):
        """Deserializes from a byte string."""
        return self._serializer.deserialize(message)
