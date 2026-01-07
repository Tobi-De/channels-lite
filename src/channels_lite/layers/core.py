from copy import deepcopy
from datetime import timedelta

from asgiref.sync import sync_to_async
from django.db import models
from django.utils import timezone

from . import BaseSQLiteChannelLayer, ChannelEmpty
from ..models import Event, GroupMembership


class SQLiteChannelLayer(BaseSQLiteChannelLayer):
    def __init__(self, *, serializer_format="json", **kwargs):
        super().__init__(serializer_format=serializer_format, **kwargs)

    async def send(self, channel, message, expiry=None):
        """
        Send a message onto a (general or specific) channel.
        """
        channel_non_local_name, prepared_message = await self._prepare_message_for_send(
            channel, message
        )
        data_bytes = self.serialize(prepared_message)
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
                message = self.deserialize(event.data)
                full_channel = self._extract_message_channel(message, channel)
                return full_channel, message

        raise ChannelEmpty()

    async def _get_channel_pending_count(self, channel):
        """
        Get the number of pending (undelivered) messages for a channel.
        """
        return await Event.objects.filter(
            channel_name=channel,
            delivered=False,
            expires_at__gte=timezone.now(),
        ).acount()

    # Expire cleanup

    async def clean_expired(self):
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

        expiry = timezone.now() + timedelta(seconds=self.expiry)
        events = []

        for channel in channels:
            # Handle process-local channels (with "!")
            if "!" in channel:
                msg_to_send = deepcopy(message)
                msg_to_send["__asgi_channel__"] = channel
                channel_name = self.non_local_name(channel)
            else:
                msg_to_send = message
                channel_name = channel

            # Check capacity - silently drop if over capacity (per spec)
            # Only check if enforce_capacity is enabled
            if self.enforce_capacity:
                capacity = self.get_capacity(channel)
                pending_count = await self._get_channel_pending_count(channel_name)
                if pending_count >= capacity:
                    continue

            data_bytes = self.serialize(msg_to_send)
            events.append(
                Event(
                    channel_name=channel_name,
                    data=data_bytes,
                    expires_at=expiry,
                )
            )

        if events:
            await Event.objects.abulk_create(events)
