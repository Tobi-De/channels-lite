"""
Parametrized tests for all channel layer implementations.
Ensures both Django ORM and aiosqlite layers are spec-compliant.
"""

from channels_lite.layers import BoundedQueue

import asyncio
import random

import async_timeout
import pytest
from asgiref.sync import async_to_sync

from channels_lite.layers.aio import AIOSQLiteChannelLayer
from channels_lite.layers.core import SQLiteChannelLayer


async def send_three_messages_with_delay(channel_name, channel_layer, delay):
    await channel_layer.send(channel_name, {"type": "test.message", "text": "First!"})
    await asyncio.sleep(delay)
    await channel_layer.send(channel_name, {"type": "test.message", "text": "Second!"})
    await asyncio.sleep(delay)
    await channel_layer.send(channel_name, {"type": "test.message", "text": "Third!"})


async def group_send_three_messages_with_delay(group_name, channel_layer, delay):
    await channel_layer.group_send(
        group_name, {"type": "test.message", "text": "First!"}
    )
    await asyncio.sleep(delay)
    await channel_layer.group_send(
        group_name, {"type": "test.message", "text": "Second!"}
    )
    await asyncio.sleep(delay)
    await channel_layer.group_send(
        group_name, {"type": "test.message", "text": "Third!"}
    )


# Parametrized fixture that provides both layer implementations
@pytest.fixture(
    params=[
        "django_orm",
        "aiosqlite",
    ],
    ids=["django_orm", "aiosqlite"],
)
async def channel_layer(request):
    """
    Parametrized fixture that provides both layer implementations.
    Each test will run twice - once with each layer.
    """
    if request.param == "django_orm":
        layer = SQLiteChannelLayer(
            database="default", capacity=100, channel_capacity={"tiny*": 1}
        )
    else:  # aiosqlite
        layer = AIOSQLiteChannelLayer(
            database="default", capacity=100, channel_capacity={"tiny*": 1}
        )

    yield layer

    await layer.flush()
    await layer.close()


@pytest.mark.asyncio
async def test_send_receive(channel_layer):
    """Test basic send and receive functionality."""
    await channel_layer.send(
        "test-channel-1", {"type": "test.message", "text": "Ahoy-hoy!"}
    )
    message = await channel_layer.receive("test-channel-1")
    assert message["type"] == "test.message"
    assert message["text"] == "Ahoy-hoy!"


@pytest.mark.asyncio
async def test_double_receive(channel_layer):
    """Test receiving from two different process-local channel names."""
    channel_name_1 = await channel_layer.new_channel()
    channel_name_2 = await channel_layer.new_channel()

    await channel_layer.send(channel_name_1, {"type": "test.message.1"})
    await channel_layer.send(channel_name_2, {"type": "test.message.2"})

    message_1 = await channel_layer.receive(channel_name_1)
    message_2 = await channel_layer.receive(channel_name_2)

    assert message_1["type"] == "test.message.1"
    assert message_2["type"] == "test.message.2"


@pytest.mark.asyncio
async def test_process_local_send_receive(channel_layer):
    """Test process-local channels."""
    channel_name = await channel_layer.new_channel()
    await channel_layer.send(
        channel_name, {"type": "test.message", "text": "Local only please"}
    )
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Local only please"


@pytest.mark.asyncio
async def test_multi_send_receive(channel_layer):
    """Test sending and receiving multiple messages in order."""
    await channel_layer.send("test-channel-3", {"type": "message.1"})
    await channel_layer.send("test-channel-3", {"type": "message.2"})
    await channel_layer.send("test-channel-3", {"type": "message.3"})
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.1"
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.2"
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.3"


@pytest.mark.asyncio
async def test_reject_bad_channel(channel_layer):
    """Test that invalid channel names are rejected."""
    with pytest.raises(TypeError):
        await channel_layer.send("=+135!", {"type": "foom"})
    with pytest.raises(TypeError):
        await channel_layer.receive("=+135!")


@pytest.mark.asyncio
async def test_reject_bad_client_prefix(channel_layer):
    """Test that receiving on a non-prefixed local channel is rejected."""
    with pytest.raises(AssertionError):
        await channel_layer.receive("not-client-prefix!local_part")


@pytest.mark.asyncio
async def test_groups_basic(channel_layer):
    """Test basic group operations."""
    channel_name1 = await channel_layer.new_channel(prefix="test-gr-chan-1")
    channel_name2 = await channel_layer.new_channel(prefix="test-gr-chan-2")
    channel_name3 = await channel_layer.new_channel(prefix="test-gr-chan-3")

    await channel_layer.group_add("test-group", channel_name1)
    await channel_layer.group_add("test-group", channel_name2)
    await channel_layer.group_add("test-group", channel_name3)
    await channel_layer.group_discard("test-group", channel_name2)
    await channel_layer.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on the two channels that were in
    async with async_timeout.timeout(1):
        assert (await channel_layer.receive(channel_name1))["type"] == "message.1"
        assert (await channel_layer.receive(channel_name3))["type"] == "message.1"

    # Make sure the removed channel did not get the message
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_name2)


@pytest.mark.asyncio
async def test_groups_same_prefix(channel_layer):
    """Test group_send with multiple channels with same channel prefix."""
    channel_name1 = await channel_layer.new_channel(prefix="test-gr-chan")
    channel_name2 = await channel_layer.new_channel(prefix="test-gr-chan")
    channel_name3 = await channel_layer.new_channel(prefix="test-gr-chan")

    await channel_layer.group_add("test-group", channel_name1)
    await channel_layer.group_add("test-group", channel_name2)
    await channel_layer.group_add("test-group", channel_name3)
    await channel_layer.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on all channels
    async with async_timeout.timeout(1):
        assert (await channel_layer.receive(channel_name1))["type"] == "message.1"
        assert (await channel_layer.receive(channel_name2))["type"] == "message.1"
        assert (await channel_layer.receive(channel_name3))["type"] == "message.1"


@pytest.mark.parametrize(
    "num_channels,timeout",
    [
        (1, 1),  # Edge case - single channel
        (10, 1),
        (100, 10),
    ],
)
@pytest.mark.asyncio
async def test_groups_performance(channel_layer, num_channels, timeout):
    """Test that group_send can efficiently send to multiple channels."""
    channels = []
    for i in range(num_channels):
        channel = await channel_layer.new_channel(prefix=f"channel{i}")
        await channel_layer.group_add("test-group", channel)
        channels.append(channel)

    async with async_timeout.timeout(timeout):
        await channel_layer.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on all channels
    async with async_timeout.timeout(timeout):
        for channel in channels:
            assert (await channel_layer.receive(channel))["type"] == "message.1"


@pytest.mark.asyncio
async def test_random_reset__channel_name(channel_layer):
    """Test that resetting random seed does not make us reuse channel names."""
    random.seed(1)
    channel_name_1 = await channel_layer.new_channel()
    random.seed(1)
    channel_name_2 = await channel_layer.new_channel()
    assert channel_name_1 != channel_name_2


@pytest.mark.asyncio
async def test_message_expiry__earliest_message_expires(channel_layer):
    """Test that the earliest message expires first."""
    # Need to create new layer with custom expiry
    if isinstance(channel_layer, SQLiteChannelLayer):
        layer = SQLiteChannelLayer(database="default", expiry=3)
    else:
        layer = AIOSQLiteChannelLayer(database="default", expiry=3)

    try:
        channel_name = await layer.new_channel()

        task = asyncio.ensure_future(
            send_three_messages_with_delay(channel_name, layer, 2)
        )
        await asyncio.wait_for(task, None)

        # The first message should have expired, only second and third should be there
        message = await layer.receive(channel_name)
        assert message["type"] == "test.message"
        assert message["text"] == "Second!"

        message = await layer.receive(channel_name)
        assert message["type"] == "test.message"
        assert message["text"] == "Third!"

        # Make sure there's no third message
        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(1):
                await layer.receive(channel_name)
    finally:
        await layer.flush()
        await layer.close()


@pytest.mark.asyncio
async def test_message_expiry__all_messages_under_expiration_time(channel_layer):
    """Test that all messages are preserved when under expiration time."""
    if isinstance(channel_layer, SQLiteChannelLayer):
        layer = SQLiteChannelLayer(database="default", expiry=3)
    else:
        layer = AIOSQLiteChannelLayer(database="default", expiry=3)

    try:
        channel_name = await layer.new_channel()

        task = asyncio.ensure_future(
            send_three_messages_with_delay(channel_name, layer, 1)
        )
        await asyncio.wait_for(task, None)

        # All messages should be there
        message = await layer.receive(channel_name)
        assert message["type"] == "test.message"
        assert message["text"] == "First!"

        message = await layer.receive(channel_name)
        assert message["type"] == "test.message"
        assert message["text"] == "Second!"

        message = await layer.receive(channel_name)
        assert message["type"] == "test.message"
        assert message["text"] == "Third!"
    finally:
        await layer.flush()
        await layer.close()


@pytest.mark.asyncio
async def test_message_expiry__group_send(channel_layer):
    """Test message expiry with group_send."""
    if isinstance(channel_layer, SQLiteChannelLayer):
        layer = SQLiteChannelLayer(database="default", expiry=3)
    else:
        layer = AIOSQLiteChannelLayer(database="default", expiry=3)

    try:
        channel_name = await layer.new_channel()
        await layer.group_add("test-group", channel_name)

        task = asyncio.ensure_future(
            group_send_three_messages_with_delay("test-group", layer, 2)
        )
        await asyncio.wait_for(task, None)

        # First message should have expired
        message = await layer.receive(channel_name)
        assert message["type"] == "test.message"
        assert message["text"] == "Second!"

        message = await layer.receive(channel_name)
        assert message["type"] == "test.message"
        assert message["text"] == "Third!"

        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(1):
                await layer.receive(channel_name)
    finally:
        await layer.flush()
        await layer.close()


@pytest.mark.asyncio
async def test_message_expiry__group_send__one_channel_expires_message(channel_layer):
    """Test message expiry with controlled timing using auto_trim=False."""
    if isinstance(channel_layer, SQLiteChannelLayer):
        layer = SQLiteChannelLayer(database="default", expiry=3, auto_trim=False)
    else:
        layer = AIOSQLiteChannelLayer(database="default", expiry=3, auto_trim=False)

    try:
        channel_1 = await layer.new_channel()
        channel_2 = await layer.new_channel(prefix="channel_2")

        await layer.group_add("test-group", channel_1)
        await layer.group_add("test-group", channel_2)

        # Send initial message to channel_1
        await layer.send(channel_1, {"type": "test.message", "text": "Zero!"})

        # Wait long enough that "Zero!" will be expired
        await asyncio.sleep(3.5)

        # Manually trigger cleanup
        await layer.clean_expired()

        # Now send three messages to the group
        await group_send_three_messages_with_delay("test-group", layer, 0)

        # channel_1: "Zero!" should be cleaned up, only group messages remain
        message = await layer.receive(channel_1)
        assert message["type"] == "test.message"
        assert message["text"] == "First!"

        message = await layer.receive(channel_1)
        assert message["type"] == "test.message"
        assert message["text"] == "Second!"

        message = await layer.receive(channel_1)
        assert message["type"] == "test.message"
        assert message["text"] == "Third!"

        # Make sure there's no fourth message
        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(0.5):
                await layer.receive(channel_1)

        # channel_2: should receive all three group messages
        message = await layer.receive(channel_2)
        assert message["type"] == "test.message"
        assert message["text"] == "First!"

        message = await layer.receive(channel_2)
        assert message["type"] == "test.message"
        assert message["text"] == "Second!"

        message = await layer.receive(channel_2)
        assert message["type"] == "test.message"
        assert message["text"] == "Third!"
    finally:
        await layer.flush()
        await layer.close()


@pytest.mark.asyncio
async def test_extensions(channel_layer):
    """Test that the layer properly declares its extensions."""
    assert "groups" in channel_layer.extensions
    assert "flush" in channel_layer.extensions


@pytest.mark.asyncio
async def test_flush(channel_layer):
    """Test that flush clears all messages and groups."""
    await channel_layer.send("test-channel", {"type": "test.message"})
    channel_name = await channel_layer.new_channel()
    await channel_layer.group_add("test-group", channel_name)
    await channel_layer.group_send("test-group", {"type": "test.message"})

    await channel_layer.flush()

    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(0.5):
            await channel_layer.receive("test-channel")


@pytest.mark.asyncio
async def test_concurrent_receives_same_channel(channel_layer):
    """Test that multiple concurrent receives on the same channel only get one message each."""
    # Send 3 messages
    await channel_layer.send("test-channel", {"type": "message.1"})
    await channel_layer.send("test-channel", {"type": "message.2"})
    await channel_layer.send("test-channel", {"type": "message.3"})

    # Create 3 concurrent receivers
    async def receive_one():
        return await channel_layer.receive("test-channel")

    results = await asyncio.gather(
        receive_one(),
        receive_one(),
        receive_one(),
    )

    # Verify we got all three messages, no duplicates
    message_types = sorted([r["type"] for r in results])
    assert message_types == ["message.1", "message.2", "message.3"]


@pytest.mark.asyncio
async def test_process_local_channel_prefix(channel_layer):
    """Test that process-local channels have the correct prefix format."""
    channel_name = await channel_layer.new_channel(prefix="myprefix")
    assert channel_name.startswith("myprefix.")
    assert "!" in channel_name
    assert channel_layer.client_prefix in channel_name


@pytest.mark.asyncio
async def test_polling_task_lifecycle(channel_layer):
    """Test that polling tasks start and stop correctly."""
    channel_name = await channel_layer.new_channel()

    # Send and receive to start polling task
    await channel_layer.send(channel_name, {"type": "test.message"})
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"

    # Verify polling task was created
    prefix = channel_layer.non_local_name(channel_name)
    assert prefix in channel_layer._polling_tasks

    # Close should cancel polling tasks
    await channel_layer.close()
    assert len(channel_layer._polling_tasks) == 0


@pytest.mark.asyncio
async def test_group_send_empty(channel_layer):
    """Test that sending to an empty group doesn't error."""
    await channel_layer.group_send("empty-group", {"type": "test.message"})
    # Should not raise an error


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_capacity_enforced_on_send(channel_layer):
    """
    Test that send() raises ChannelFull when channel is at capacity.
    """
    from channels.exceptions import ChannelFull

    # Use a small capacity for testing
    test_layer = SQLiteChannelLayer(database="default", capacity=3)

    try:
        # Fill channel to capacity
        await test_layer.send(
            "test-channel-full", {"type": "test.message", "text": "1"}
        )
        await test_layer.send(
            "test-channel-full", {"type": "test.message", "text": "2"}
        )
        await test_layer.send(
            "test-channel-full", {"type": "test.message", "text": "3"}
        )

        # Next send should raise ChannelFull
        with pytest.raises(ChannelFull):
            await test_layer.send(
                "test-channel-full", {"type": "test.message", "text": "4"}
            )

        # After receiving one message, should be able to send again
        msg = await test_layer.receive("test-channel-full")
        assert msg["text"] == "1"

        # Now we can send again
        await test_layer.send(
            "test-channel-full", {"type": "test.message", "text": "4"}
        )
    finally:
        await test_layer.flush()
        await test_layer.close()


@pytest.mark.asyncio
async def test_capacity_enforced_process_specific(channel_layer):
    """
    Test that capacity is enforced for process-specific channels.
    Capacity applies to the prefix (non-local part).
    """
    from channels.exceptions import ChannelFull

    test_layer = SQLiteChannelLayer(database="default", capacity=2)

    try:
        # Create process-specific channels
        channel1 = await test_layer.new_channel("test")
        channel2 = await test_layer.new_channel("test")

        # Both share the same prefix, so capacity is shared
        await test_layer.send(channel1, {"type": "test.message", "text": "1"})
        await test_layer.send(channel2, {"type": "test.message", "text": "2"})

        # Next send should raise ChannelFull (capacity is 2, shared across prefix)
        with pytest.raises(ChannelFull):
            await test_layer.send(channel1, {"type": "test.message", "text": "3"})
    finally:
        await test_layer.flush()
        await test_layer.close()


@pytest.mark.asyncio
async def test_capacity_not_raised_on_group_send(channel_layer):
    """
    Test that group_send silently drops messages when channels are over capacity,
    instead of raising ChannelFull (per spec).
    """
    test_layer = SQLiteChannelLayer(database="default", capacity=2)

    try:
        # Add channels to group
        await test_layer.group_add("test-group", "channel1")
        await test_layer.group_add("test-group", "channel2")

        # Fill channel1 to capacity
        await test_layer.send("channel1", {"type": "test.message", "text": "1"})
        await test_layer.send("channel1", {"type": "test.message", "text": "2"})

        # group_send should NOT raise, even though channel1 is full
        # It should silently drop the message for channel1
        await test_layer.group_send(
            "test-group", {"type": "test.message", "text": "group"}
        )

        # channel2 should receive the message
        msg = await test_layer.receive("channel2")
        assert msg["text"] == "group"

        # channel1 should not have received it (it was over capacity)
        # Check that channel1 only has the original 2 messages
        msg1 = await test_layer.receive("channel1")
        assert msg1["text"] == "1"
        msg2 = await test_layer.receive("channel1")
        assert msg2["text"] == "2"

        # No more messages in channel1
        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(0.5):
                await test_layer.receive("channel1")
    finally:
        await test_layer.flush()
        await test_layer.close()


@pytest.mark.asyncio
async def test_channel_specific_capacity_enforcement(channel_layer):
    """
    Test that channel-specific capacity overrides work correctly.
    The fixture configures channel_capacity={"tiny*": 1}
    """
    from channels.exceptions import ChannelFull

    # Send to tiny channel (capacity 1)
    await channel_layer.send("tiny-channel", {"type": "test.message", "text": "1"})

    # Second send should raise ChannelFull
    with pytest.raises(ChannelFull):
        await channel_layer.send("tiny-channel", {"type": "test.message", "text": "2"})

    # Normal channel should allow more (capacity 100)
    for i in range(10):
        await channel_layer.send(
            "normal-channel", {"type": "test.message", "text": str(i)}
        )


@pytest.mark.asyncio
async def test_capacity_enforcement_can_be_disabled(channel_layer):
    """
    Test that capacity enforcement can be disabled with enforce_capacity=False.
    """
    from channels.exceptions import ChannelFull

    # Create layer with enforcement disabled
    test_layer = SQLiteChannelLayer(
        database="default", capacity=2, enforce_capacity=False
    )

    try:
        # Fill beyond capacity - should NOT raise since enforcement is disabled
        await test_layer.send("test-channel", {"type": "test.message", "text": "1"})
        await test_layer.send("test-channel", {"type": "test.message", "text": "2"})
        await test_layer.send("test-channel", {"type": "test.message", "text": "3"})
        await test_layer.send("test-channel", {"type": "test.message", "text": "4"})

        # All messages should be in DB
        msg1 = await test_layer.receive("test-channel")
        msg2 = await test_layer.receive("test-channel")
        msg3 = await test_layer.receive("test-channel")
        msg4 = await test_layer.receive("test-channel")

        assert msg1["text"] == "1"
        assert msg2["text"] == "2"
        assert msg3["text"] == "3"
        assert msg4["text"] == "4"
    finally:
        await test_layer.flush()
        await test_layer.close()


@pytest.mark.parametrize("layer_type", ["django_orm", "aiosqlite"])
@pytest.mark.asyncio
async def test_random_reset__client_prefix(layer_type):
    """Test that resetting random seed does not make us reuse client_prefixes."""
    if layer_type == "django_orm":
        random.seed(1)
        layer_1 = SQLiteChannelLayer(database="default")
        random.seed(1)
        layer_2 = SQLiteChannelLayer(database="default")
    else:  # aiosqlite
        random.seed(1)
        layer_1 = AIOSQLiteChannelLayer(database="default")
        random.seed(1)
        layer_2 = AIOSQLiteChannelLayer(database="default")

    try:
        assert layer_1.client_prefix != layer_2.client_prefix
    finally:
        await layer_1.close()
        await layer_2.close()


def test_repeated_group_send_with_async_to_sync(channel_layer):
    """Test repeated group_send calls wrapped in async_to_sync (Django ORM layer only)."""
    try:
        async_to_sync(channel_layer.group_send)(
            "channel_name_1", {"type": "test.message.1"}
        )
        async_to_sync(channel_layer.group_send)(
            "channel_name_2", {"type": "test.message.2"}
        )
    except RuntimeError as exc:
        pytest.fail(f"repeated async_to_sync wrapped group_send calls raised {exc}")
    finally:
        async_to_sync(channel_layer.close)()


async def test_receive_buffer_respects_capacity(channel_layer):
    """Test that BoundedQueue respects capacity and drops oldest messages."""
    try:
        buff = BoundedQueue(channel_layer.capacity)
        channel_layer.receive_buffer["test-channel"] = buff

        # Add way more messages than capacity
        for i in range(10000):
            buff.put_nowait(i)

        capacity = 100
        assert channel_layer.capacity == capacity
        assert buff.full() is True
        assert buff.qsize() == capacity

        # Should only have the last 100 messages (9900-9999)
        # because BoundedQueue drops oldest when full
        messages = [buff.get_nowait() for _ in range(capacity)]
        assert list(range(9900, 10000)) == messages
    finally:
        await channel_layer.close()


@pytest.mark.asyncio
async def test_channel_specific_capacity(channel_layer):
    """Test that channel-specific capacity limits work correctly."""
    # The fixture configures channel_capacity={"tiny*": 1}
    # This means channels matching "tiny*" (glob pattern) should have capacity of 1

    # Verify that get_capacity returns the correct capacity for different channels
    tiny_channel = await channel_layer.new_channel(prefix="tiny")
    normal_channel = await channel_layer.new_channel(prefix="normal")

    # Check that tiny channel has capacity of 1
    assert channel_layer.get_capacity(tiny_channel) == 1

    # Check that normal channel has default capacity of 100
    assert channel_layer.get_capacity(normal_channel) == 100

    # Test that the buffer respects channel-specific capacity
    # Create a buffer for the tiny channel and fill it beyond capacity
    buff = BoundedQueue(maxsize=channel_layer.get_capacity(tiny_channel))
    channel_layer.receive_buffer[tiny_channel] = buff

    # Add more messages than capacity
    for i in range(10):
        buff.put_nowait({"type": f"message.{i}"})

    # Should only have 1 message (the last one) because capacity is 1
    assert buff.qsize() == 1
    message = buff.get_nowait()
    assert message["type"] == "message.9"

    # Verify buffer is now empty
    assert buff.empty()

    # Test with normal channel buffer (capacity 100)
    buff_normal = BoundedQueue(maxsize=channel_layer.get_capacity(normal_channel))
    channel_layer.receive_buffer[normal_channel] = buff_normal

    # Add 10 messages (well under capacity of 100)
    for i in range(10):
        buff_normal.put_nowait({"type": f"message.{i}"})

    # All 10 messages should be available
    assert buff_normal.qsize() == 10
    for i in range(10):
        message = buff_normal.get_nowait()
        assert message["type"] == f"message.{i}"


@pytest.mark.asyncio
async def test_channel_specific_capacity_with_named_channels(channel_layer):
    """Test channel-specific capacity with named/specific channels (non-process-local)."""
    from channels.exceptions import ChannelFull

    # The fixture configures channel_capacity={"tiny*": 1}

    # Test with a specific named channel matching the pattern
    tiny_specific_channel = "tiny_test_channel"
    normal_specific_channel = "normal_test_channel"

    # Verify capacity detection
    assert channel_layer.get_capacity(tiny_specific_channel) == 1
    assert channel_layer.get_capacity(normal_specific_channel) == 100

    # Send one message to the tiny specific channel (capacity 1)
    await channel_layer.send(tiny_specific_channel, {"type": "message.1"})

    # Second send should raise ChannelFull since capacity is 1
    with pytest.raises(ChannelFull):
        await channel_layer.send(tiny_specific_channel, {"type": "message.2"})

    # Receive the first message
    message1 = await channel_layer.receive(tiny_specific_channel)
    assert message1["type"] == "message.1"

    # Now we can send again since capacity is available
    await channel_layer.send(tiny_specific_channel, {"type": "message.2"})
    message2 = await channel_layer.receive(tiny_specific_channel)
    assert message2["type"] == "message.2"

    # Verify no more messages
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(0.5):
            await channel_layer.receive(tiny_specific_channel)

    # Test with normal specific channel (capacity 100)
    for i in range(10):
        await channel_layer.send(normal_specific_channel, {"type": f"test.{i}"})

    # Receive all messages
    for i in range(10):
        msg = await channel_layer.receive(normal_specific_channel)
        assert msg["type"] == f"test.{i}"
