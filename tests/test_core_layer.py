import asyncio
import random

import async_timeout
import pytest
from asgiref.sync import async_to_sync
from channels_lite.layers.core import ChannelEmpty, SqliteChannelLayer


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


@pytest.fixture()
async def channel_layer():
    """
    Channel layer fixture that flushes automatically.
    """
    channel_layer = SqliteChannelLayer(
        database="default", capacity=3, channel_capacity={"tiny": 1}
    )
    yield channel_layer
    await channel_layer.flush()
    await channel_layer.close()


@pytest.mark.asyncio
async def test_send_receive(channel_layer):
    """
    Makes sure we can send a message to a normal channel then receive it.
    """
    await channel_layer.send(
        "test-channel-1", {"type": "test.message", "text": "Ahoy-hoy!"}
    )
    message = await channel_layer.receive("test-channel-1")
    assert message["type"] == "test.message"
    assert message["text"] == "Ahoy-hoy!"


@pytest.mark.asyncio
async def test_double_receive(channel_layer):
    """
    Makes sure we can receive from two different process-local channel names.
    """
    # Create two process-local channels
    channel_name_1 = await channel_layer.new_channel()
    channel_name_2 = await channel_layer.new_channel()

    # Send messages to both channels
    await channel_layer.send(channel_name_1, {"type": "test.message.1"})
    await channel_layer.send(channel_name_2, {"type": "test.message.2"})

    # Receive from both channels
    message_1 = await channel_layer.receive(channel_name_1)
    message_2 = await channel_layer.receive(channel_name_2)

    assert message_1["type"] == "test.message.1"
    assert message_2["type"] == "test.message.2"


@pytest.mark.asyncio
async def test_process_local_send_receive(channel_layer):
    """
    Makes sure we can send a message to a process-local channel then receive it.
    """
    channel_name = await channel_layer.new_channel()
    await channel_layer.send(
        channel_name, {"type": "test.message", "text": "Local only please"}
    )
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Local only please"


@pytest.mark.asyncio
async def test_multi_send_receive(channel_layer):
    """
    Tests overlapping sends and receives, and ordering.
    """
    channel_layer = SqliteChannelLayer(database="default")
    await channel_layer.send("test-channel-3", {"type": "message.1"})
    await channel_layer.send("test-channel-3", {"type": "message.2"})
    await channel_layer.send("test-channel-3", {"type": "message.3"})
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.1"
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.2"
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.3"
    await channel_layer.flush()
    await channel_layer.close()


@pytest.mark.asyncio
async def test_reject_bad_channel(channel_layer):
    """
    Makes sure sending/receiving on an invalid channel name fails.
    """
    with pytest.raises(TypeError):
        await channel_layer.send("=+135!", {"type": "foom"})
    with pytest.raises(TypeError):
        await channel_layer.receive("=+135!")


@pytest.mark.asyncio
async def test_reject_bad_client_prefix(channel_layer):
    """
    Makes sure receiving on a non-prefixed local channel is not allowed.
    """
    with pytest.raises(AssertionError):
        await channel_layer.receive("not-client-prefix!local_part")


@pytest.mark.asyncio
async def test_groups_basic(channel_layer):
    """
    Tests basic group operation.
    """
    channel_layer = SqliteChannelLayer(database="default")
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
    await channel_layer.flush()
    await channel_layer.close()


@pytest.mark.asyncio
async def test_groups_same_prefix(channel_layer):
    """
    Tests group_send with multiple channels with same channel prefix
    """
    channel_layer = SqliteChannelLayer(database="default")
    channel_name1 = await channel_layer.new_channel(prefix="test-gr-chan")
    channel_name2 = await channel_layer.new_channel(prefix="test-gr-chan")
    channel_name3 = await channel_layer.new_channel(prefix="test-gr-chan")
    await channel_layer.group_add("test-group", channel_name1)
    await channel_layer.group_add("test-group", channel_name2)
    await channel_layer.group_add("test-group", channel_name3)
    await channel_layer.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on the channels that were in
    async with async_timeout.timeout(1):
        assert (await channel_layer.receive(channel_name1))["type"] == "message.1"
        assert (await channel_layer.receive(channel_name2))["type"] == "message.1"
        assert (await channel_layer.receive(channel_name3))["type"] == "message.1"

    await channel_layer.flush()
    await channel_layer.close()


@pytest.mark.parametrize(
    "num_channels,timeout",
    [
        (1, 1),  # Edge cases - make sure we can send to a single channel
        (10, 1),
        (100, 10),
    ],
)
@pytest.mark.asyncio
async def test_groups_performance(channel_layer, num_channels, timeout):
    """
    Tests group operation: can send efficiently to multiple channels
    within a certain timeout
    """
    channel_layer = SqliteChannelLayer(database="default", capacity=100)

    channels = []
    for i in range(0, num_channels):
        channel = await channel_layer.new_channel(prefix="channel%s" % i)
        await channel_layer.group_add("test-group", channel)
        channels.append(channel)

    async with async_timeout.timeout(timeout):
        await channel_layer.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message all the channels
    async with async_timeout.timeout(timeout):
        for channel in channels:
            assert (await channel_layer.receive(channel))["type"] == "message.1"

    await channel_layer.flush()
    await channel_layer.close()


def test_repeated_group_send_with_async_to_sync(channel_layer):
    """
    Makes sure repeated group_send calls wrapped in async_to_sync work correctly.
    """
    channel_layer = SqliteChannelLayer(database="default", capacity=3)

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


@pytest.mark.asyncio
async def test_random_reset__channel_name(channel_layer):
    """
    Makes sure resetting random seed does not make us reuse channel names.
    """

    channel_layer = SqliteChannelLayer(database="default")
    random.seed(1)
    channel_name_1 = await channel_layer.new_channel()
    random.seed(1)
    channel_name_2 = await channel_layer.new_channel()

    assert channel_name_1 != channel_name_2
    await channel_layer.close()


@pytest.mark.asyncio
async def test_random_reset__client_prefix(channel_layer):
    """
    Makes sure resetting random seed does not make us reuse client_prefixes.
    """

    random.seed(1)
    channel_layer_1 = SqliteChannelLayer(database="default")
    random.seed(1)
    channel_layer_2 = SqliteChannelLayer(database="default")
    assert channel_layer_1.client_prefix != channel_layer_2.client_prefix
    await channel_layer_1.close()
    await channel_layer_2.close()


@pytest.mark.asyncio
async def test_message_expiry__earliest_message_expires(channel_layer):
    expiry = 3
    delay = 2
    channel_layer = SqliteChannelLayer(database="default", expiry=expiry)
    channel_name = await channel_layer.new_channel()

    task = asyncio.ensure_future(
        send_three_messages_with_delay(channel_name, channel_layer, delay)
    )
    await asyncio.wait_for(task, None)

    # the first message should have expired, we should only see the second message and the third
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"

    # Make sure there's no third message even out of order
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_name)

    await channel_layer.close()


@pytest.mark.asyncio
async def test_message_expiry__all_messages_under_expiration_time(channel_layer):
    expiry = 3
    delay = 1
    channel_layer = SqliteChannelLayer(database="default", expiry=expiry)
    channel_name = await channel_layer.new_channel()

    task = asyncio.ensure_future(
        send_three_messages_with_delay(channel_name, channel_layer, delay)
    )
    await asyncio.wait_for(task, None)

    # expiry = 3, total delay under 3, all messages there
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "First!"

    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"

    await channel_layer.close()


@pytest.mark.asyncio
async def test_message_expiry__group_send(channel_layer):
    expiry = 3
    delay = 2
    channel_layer = SqliteChannelLayer(database="default", expiry=expiry)
    channel_name = await channel_layer.new_channel()

    await channel_layer.group_add("test-group", channel_name)

    task = asyncio.ensure_future(
        group_send_three_messages_with_delay("test-group", channel_layer, delay)
    )
    await asyncio.wait_for(task, None)

    # the first message should have expired, we should only see the second message and the third
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"

    # Make sure there's no third message even out of order
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_name)

    await channel_layer.close()


@pytest.mark.asyncio
async def test_message_expiry__group_send__one_channel_expires_message(channel_layer):
    """
    Test message expiry with controlled timing.
    Uses auto_trim=False to manually control when cleanup happens.
    """
    expiry = 3

    # Disable auto_trim to have full control over when messages expire
    channel_layer = SqliteChannelLayer(database="default", expiry=expiry, auto_trim=False)
    channel_1 = await channel_layer.new_channel()
    channel_2 = await channel_layer.new_channel(prefix="channel_2")

    await channel_layer.group_add("test-group", channel_1)
    await channel_layer.group_add("test-group", channel_2)

    # Send initial message to channel_1
    await channel_layer.send(channel_1, {"type": "test.message", "text": "Zero!"})

    # Wait long enough that "Zero!" will be expired
    await asyncio.sleep(3.5)

    # Manually trigger cleanup to remove expired messages
    await channel_layer._clean_expired()

    # Now send three messages to the group
    await group_send_three_messages_with_delay("test-group", channel_layer, 0)
    
    # channel_1: "Zero!" should have been cleaned up, only group messages remain
    message = await channel_layer.receive(channel_1)
    assert message["type"] == "test.message"
    assert message["text"] == "First!"

    message = await channel_layer.receive(channel_1)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_1)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"

    # Make sure there's no fourth message
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(0.5):
            await channel_layer.receive(channel_1)

    # channel_2: should receive all three group messages
    message = await channel_layer.receive(channel_2)
    assert message["type"] == "test.message"
    assert message["text"] == "First!"

    message = await channel_layer.receive(channel_2)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_2)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"

    await channel_layer.close()


@pytest.mark.asyncio
async def test_extensions():
    """Test that the layer properly declares its extensions."""
    channel_layer = SqliteChannelLayer(database="default")
    assert "groups" in channel_layer.extensions
    assert "flush" in channel_layer.extensions
    await channel_layer.close()


@pytest.mark.asyncio
async def test_flush(channel_layer):
    """Test that flush clears all messages and groups."""
    # Send some messages
    await channel_layer.send("test-channel", {"type": "test.message"})
    channel_name = await channel_layer.new_channel()
    await channel_layer.group_add("test-group", channel_name)
    await channel_layer.group_send("test-group", {"type": "test.message"})

    # Flush everything
    await channel_layer.flush()

    # Verify everything is cleared
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(0.5):
            await channel_layer.receive("test-channel")


@pytest.mark.asyncio
async def test_concurrent_receives_same_channel():
    """Test that multiple concurrent receives on the same channel only get one message each."""
    channel_layer = SqliteChannelLayer(database="default")

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

    await channel_layer.flush()
    await channel_layer.close()


@pytest.mark.asyncio
async def test_process_local_channel_prefix():
    """Test that process-local channels have the correct prefix format."""
    channel_layer = SqliteChannelLayer(database="default")
    channel_name = await channel_layer.new_channel(prefix="myprefix")

    assert channel_name.startswith("myprefix.")
    assert "!" in channel_name
    assert channel_layer.client_prefix in channel_name

    await channel_layer.close()


@pytest.mark.asyncio
async def test_polling_task_lifecycle():
    """Test that polling tasks start and stop correctly."""
    channel_layer = SqliteChannelLayer(database="default", polling_interval=0.1)
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
async def test_capacity_not_enforced():
    """
    SQLite layer doesn't enforce capacity like Redis does (no ChannelFull exception).
    This test documents the current behavior.
    """
    channel_layer = SqliteChannelLayer(database="default", capacity=3)

    # Send more than capacity - should not raise
    await channel_layer.send("test-channel-1", {"type": "test.message"})
    await channel_layer.send("test-channel-1", {"type": "test.message"})
    await channel_layer.send("test-channel-1", {"type": "test.message"})
    await channel_layer.send("test-channel-1", {"type": "test.message"})  # No exception

    await channel_layer.flush()
    await channel_layer.close()
