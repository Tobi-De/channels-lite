from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer


def send_event(channel: str, event_type: str, data: dict | None = None):
    data = data or {}
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.send)(channel, {"type": event_type, **data})
