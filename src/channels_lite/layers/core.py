import asyncio
import json
import time
import uuid
from collections import defaultdict
from copy import deepcopy
from datetime import timedelta

from asgiref.sync import sync_to_async
from channels.exceptions import InvalidChannelLayerError
from channels.layers import BaseChannelLayer
from django.conf import settings
from django.db import models
from django.utils import timezone

from ..models import Event, GroupMembership


class ChannelEmpty(Exception):
    pass


class SqliteChannelLayer(BaseChannelLayer):
    def __init__(
        self,
        *,
        database,
        polling_interval=0.1,
        auto_trim=True,
        expiry=60,
        group_expiry=86400,
        capacity=100,
        channel_capacity=None,
        **kwargs,
    ):
        super().__init__(
            expiry=expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
            **kwargs,
        )
        self.database = database
        self.polling_interval = polling_interval
        self.auto_trim = (auto_trim,)
        self.expiry = expiry
        self.group_expiry = group_expiry
        # Decide on a unique client prefix to use in ! sections
        self.client_prefix = uuid.uuid4().hex

        # Buffering for process-specific channels
        self.receive_buffer = defaultdict(
            lambda: asyncio.Queue()
        )  # Dict[channel_name, asyncio.Queue]
        self._polling_tasks = {}  # Dict[prefix, asyncio.Task]
        self._active_receivers = defaultdict(int)  # Dict[prefix, int]

        try:
            self.db_settings = settings.DATABASES[self.database]
            assert "sqlite3" in self.db_settings["ENGINE"]
        except KeyError:
            raise InvalidChannelLayerError(
                "%s is an invalid database alias" % self.database
            )
        except AssertionError:
            raise InvalidChannelLayerError(
                "Sqlite3 Database Engine is Needed to use this channel layer"
            )

    extensions = ["groups", "flush"]

    async def send(self, channel, message, expiry=None):
        """
        Send a message onto a (general or specific) channel.
        """
        # Typecheck
        assert isinstance(message, dict), "message is not a dict"
        self.require_valid_channel_name(channel)
        # If it's a process-local channel, strip off local part and stick full
        # name in message
        assert "__asgi_channel__" not in message
        channel_non_local_name = channel
        message = deepcopy(message)
        if "!" in channel:
            message["__asgi_channel__"] = channel
            channel_non_local_name = self.non_local_name(channel)

        # Encode message as JSON bytes
        data_bytes = json.dumps(message).encode("utf-8")

        await Event.objects.acreate(
            channel_name=channel_non_local_name,
            data=data_bytes,
            expires_at=expiry or (timezone.now() + timedelta(seconds=self.expiry)),
        )

    async def _receive_single_from_db(self, channel):
        """
        Pull a single message from the database for the given channel.
        Returns a tuple of (channel_name, message).
        Raises ChannelEmpty if no message is available after one poll cycle.
        """
        event_qs = Event.objects.filter(
            delivered=False,
            channel_name=channel,
        ).order_by("expires_at")

        event = await event_qs.filter(expires_at__gte=timezone.now()).afirst()
        if event:
            # if update was successful, the event is considered delivered
            updated = await Event.objects.filter(id=event.id, delivered=False).aupdate(
                delivered=True
            )
            if updated:
                # Decode JSON data
                message = json.loads(event.data.decode("utf-8"))
                # Get the full channel name from __asgi_channel__ if present
                full_channel = channel
                if "__asgi_channel__" in message:
                    full_channel = message["__asgi_channel__"]
                    del message["__asgi_channel__"]
                return full_channel, message

        # No message available
        raise ChannelEmpty()

    async def _poll_and_distribute(self, prefix):
        """
        Background task that polls the database for messages on a given prefix
        and distributes them to the appropriate channel buffers.

        Auto-shuts down after 30 minutes of inactivity (no receivers + no messages).
        """
        last_activity = time.time()
        idle_timeout = 1800  # 30 minutes in seconds

        try:
            while True:
                # Check shutdown condition: no active receivers and idle for 30min
                if (
                    self._active_receivers[prefix] == 0
                    and time.time() - last_activity > idle_timeout
                ):
                    # Clean up and exit
                    if prefix in self._polling_tasks:
                        del self._polling_tasks[prefix]
                    if prefix in self._active_receivers:
                        del self._active_receivers[prefix]
                    return

                try:
                    # Pull one message from database
                    msg_channel, message = await self._receive_single_from_db(prefix)

                    # Route to the appropriate buffer based on full channel name
                    await self.receive_buffer[msg_channel].put(message)
                    last_activity = time.time()  # Reset idle timer

                except Exception:
                    # No message available or error, run cleanup and sleep
                    if self.auto_trim:
                        await self._clean_expired()
                    await asyncio.sleep(self.polling_interval)

        except asyncio.CancelledError:
            # Task was cancelled, clean up
            if prefix in self._polling_tasks:
                del self._polling_tasks[prefix]
            raise

    async def receive(self, channel):
        """
        Receive the first message that arrives on the channel.
        If more than one coroutine waits on the same channel, a random one
        of the waiting coroutines will get the result.
        """
        self.require_valid_channel_name(channel)

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
                        self._poll_and_distribute(prefix)
                    )

                # Wait for message in our buffer
                return await self.receive_buffer[channel].get()

            finally:
                self._active_receivers[prefix] -= 1

        # For normal channels, use direct polling
        while True:
            try:
                _, message = await self._receive_single_from_db(real_channel)
                return message
            except:
                # No message available, run cleanup and sleep
                if self.auto_trim:
                    await self._clean_expired()
                await asyncio.sleep(self.polling_interval)

    async def new_channel(self, prefix="specific."):
        """
        Returns a new channel name that can be used by something in our
        process as a specific channel.
        """
        return f"{prefix}.{self.client_prefix}!{uuid.uuid4().hex}"

    # Expire cleanup

    async def _clean_expired(self):
        """
        Goes through all messages and groups and removes those that are expired.
        Any channel with an expired message is removed from all groups.
        """
        now = timezone.now()
        # Channel cleanup
        await Event.objects.filter(expires_at__lt=now).adelete()

        # Group Expiration
        await GroupMembership.objects.filter(expires_at__lt=now).adelete()
        # remove from all groups channel with unread messages
        await self._remove_from_group_inactive_channels(now)

    @sync_to_async
    def _remove_from_group_inactive_channels(self, now):
        grace_period = now - timedelta(seconds=30)
        last_message_ids = (
            Event.objects.filter(channel_name=models.OuterRef("channel_name"))
            .order_by("-created_at")
            .values("id")[:1]
        )
        channels_to_remove = Event.objects.filter(
            id__in=models.Subquery(last_message_ids),
            expires_at__lt=grace_period,
            delivered=False,
        ).values_list("channel_name", flat=True)
        GroupMembership.objects.filter(
            channel_name__in=list(channels_to_remove)
        ).delete()

    # Flush extension

    async def flush(self):
        await Event.objects.all().adelete()
        await GroupMembership.objects.all().adelete()

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

    # Groups extension

    async def group_add(self, group, channel):
        """
        Adds the channel name to a group.
        """
        # Check the inputs
        self.require_valid_group_name(group)
        self.require_valid_channel_name(channel)
        await GroupMembership.objects.aupdate_or_create(
            channel_name=channel,
            group_name=group,
            defaults={
                "expires_at": timezone.now() + timedelta(seconds=self.group_expiry)
            },
        )

    async def group_discard(self, group, channel):
        # Both should be text and valid
        self.require_valid_channel_name(channel)
        self.require_valid_group_name(group)
        await GroupMembership.objects.filter(
            group_name=group, channel_name=channel
        ).adelete()

    async def group_send(self, group, message):
        # Check types
        assert isinstance(message, dict), "Message is not a dict"
        self.require_valid_group_name(group)

        # Send to each channel
        @sync_to_async
        def _get_channels():
            return list(
                GroupMembership.objects.filter(
                    group_name=group, expires_at__gte=timezone.now()
                ).values_list("channel_name", flat=True)
            )

        channels = await _get_channels()

        if not channels:
            return

        # Optimize: use bulk_create instead of individual sends
        expiry = timezone.now() + timedelta(seconds=self.expiry)
        events = []

        for channel in channels:
            # Handle process-local channels (with "!")
            if "!" in channel:
                msg = deepcopy(message)
                msg["__asgi_channel__"] = channel
                channel_name = self.non_local_name(channel)
            else:
                msg = deepcopy(message)
                channel_name = channel

            # Encode message as JSON bytes
            data_bytes = json.dumps(msg).encode("utf-8")

            events.append(
                Event(
                    channel_name=channel_name,
                    data=data_bytes,
                    expires_at=expiry,
                )
            )

        await Event.objects.abulk_create(events)
