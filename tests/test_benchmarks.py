"""
Benchmark tests for channels-lite.

Run with: just bench
"""

import asyncio

import pytest


def run_async(coro):
    """Helper to run async coroutines in sync benchmark context."""
    return asyncio.run(coro)


@pytest.mark.benchmark(group="point-to-point")
@pytest.mark.timeout(300)  # 5 minutes timeout for benchmarks
def test_benchmark_point_to_point_sqlite(benchmark):
    """
    Benchmark SQLite: Send and receive 10,000 messages through a single channel.
    Tests raw throughput for point-to-point messaging.
    """
    from channels_lite.layers.core import SqliteChannelLayer

    async def run_benchmark():
        # Disable auto_trim to avoid cleanup issues during benchmarking
        layer = SqliteChannelLayer(database="default", capacity=10000, auto_trim=False)
        # Use a regular named channel (not process-local) to avoid polling complexity
        channel_name = "benchmark-channel"

        try:
            # Send 10,000 messages
            for i in range(10000):
                await layer.send(channel_name, {"type": "test.message", "num": i})

            # Receive all messages
            received = 0
            for i in range(10000):
                msg = await layer.receive(channel_name)
                assert msg["type"] == "test.message"
                received += 1

            return received
        finally:
            # Clean up within the async context
            try:
                await layer.flush()
            except:
                pass
            try:
                await layer.close()
            except:
                pass

    result = benchmark(lambda: run_async(run_benchmark()))
    assert result == 10000


@pytest.mark.benchmark(group="point-to-point")
@pytest.mark.timeout(300)  # 5 minutes timeout for benchmarks
def test_benchmark_point_to_point_redis(benchmark):
    """
    Benchmark Redis: Send and receive 10,000 messages through a single channel.
    Tests raw throughput for point-to-point messaging.
    """
    from channels_redis.core import RedisChannelLayer

    async def run_benchmark():
        # Create layer with high capacity for benchmarking
        layer = RedisChannelLayer(
            hosts=[("localhost", 6379)],
            capacity=10000,
        )
        channel_name = "benchmark-channel-redis"

        try:
            # Send 10,000 messages
            for i in range(10000):
                await layer.send(channel_name, {"type": "test.message", "num": i})

            # Receive all messages
            received = 0
            for i in range(10000):
                msg = await layer.receive(channel_name)
                assert msg["type"] == "test.message"
                received += 1

            return received
        finally:
            try:
                await layer.flush()
            except:
                pass

    result = benchmark(lambda: run_async(run_benchmark()))
    assert result == 10000


@pytest.mark.benchmark(group="point-to-point")
@pytest.mark.timeout(300)  # 5 minutes timeout for benchmarks
def test_benchmark_point_to_point_aiosqlite(benchmark):
    """
    Benchmark AioSQLite: Send and receive 10,000 messages through a single channel.
    Tests raw throughput for point-to-point messaging using aiosqlite.
    """
    from django.conf import settings
    from channels_lite.layers.aio import AioSqliteChannelLayer

    async def run_benchmark():
        # Use the Django test database
        db_path = settings.DATABASES["default"]["NAME"]
        layer = AioSqliteChannelLayer(db_path=db_path, capacity=10000, auto_trim=False)
        channel_name = "benchmark-channel-aio"

        try:
            # Send 10,000 messages
            for i in range(10000):
                await layer.send(channel_name, {"type": "test.message", "num": i})

            # Receive all messages
            received = 0
            for i in range(10000):
                msg = await layer.receive(channel_name)
                assert msg["type"] == "test.message"
                received += 1

            return received
        finally:
            try:
                await layer.flush()
            except:
                pass
            try:
                await layer.close()
            except:
                pass

    result = benchmark(lambda: run_async(run_benchmark()))
    assert result == 10000


@pytest.mark.benchmark(group="group-broadcast")
@pytest.mark.timeout(300)  # 5 minutes timeout for benchmarks
def test_benchmark_group_broadcast_sqlite(benchmark):
    """
    Benchmark SQLite: Broadcast 100 messages to 1,000 channels via groups.
    Tests group operation performance under load.
    """
    from channels_lite.layers.core import SqliteChannelLayer

    async def run_benchmark():
        layer = SqliteChannelLayer(database="default", capacity=200000, auto_trim=False)

        try:
            # Create 1,000 named channels and add to a group
            channels = []
            for i in range(1000):
                channel = f"benchmark-channel-{i}"
                await layer.group_add("benchmark-group", channel)
                channels.append(channel)

            # Send 100 group messages (will be delivered to all 1,000 channels)
            for i in range(100):
                await layer.group_send(
                    "benchmark-group", {"type": "test.message", "num": i}
                )

            # Receive all messages from all channels
            total_received = 0
            for channel in channels:
                for i in range(100):
                    msg = await layer.receive(channel)
                    assert msg["type"] == "test.message"
                    total_received += 1

            return total_received
        finally:
            try:
                await layer.flush()
            except:
                pass
            try:
                await layer.close()
            except:
                pass

    result = benchmark(lambda: run_async(run_benchmark()))
    assert result == 100000  # 1,000 channels × 100 messages


@pytest.mark.benchmark(group="group-broadcast")
@pytest.mark.timeout(300)  # 5 minutes timeout for benchmarks
def test_benchmark_group_broadcast_redis(benchmark):
    """
    Benchmark Redis: Broadcast 100 messages to 1,000 channels via groups.
    Tests group operation performance under load.
    """
    from channels_redis.core import RedisChannelLayer

    async def run_benchmark():
        layer = RedisChannelLayer(
            hosts=[("localhost", 6379)],
            capacity=200000,
        )

        try:
            # Create 1,000 named channels and add to a group
            channels = []
            for i in range(1000):
                channel = f"benchmark-channel-redis-{i}"
                await layer.group_add("benchmark-group-redis", channel)
                channels.append(channel)

            # Send 100 group messages (will be delivered to all 1,000 channels)
            for i in range(100):
                await layer.group_send(
                    "benchmark-group-redis", {"type": "test.message", "num": i}
                )

            # Receive all messages from all channels
            total_received = 0
            for channel in channels:
                for i in range(100):
                    msg = await layer.receive(channel)
                    assert msg["type"] == "test.message"
                    total_received += 1

            return total_received
        finally:
            try:
                await layer.flush()
            except:
                pass

    result = benchmark(lambda: run_async(run_benchmark()))
    assert result == 100000  # 1,000 channels × 100 messages


@pytest.mark.benchmark(group="group-broadcast")
@pytest.mark.timeout(300)  # 5 minutes timeout for benchmarks
def test_benchmark_group_broadcast_aiosqlite(benchmark):
    """
    Benchmark AioSQLite: Broadcast 100 messages to 1,000 channels via groups.
    Tests group operation performance under load using aiosqlite.
    """
    from django.conf import settings
    from channels_lite.layers.aio import AioSqliteChannelLayer

    async def run_benchmark():
        # Use the Django test database
        db_path = settings.DATABASES["default"]["NAME"]
        layer = AioSqliteChannelLayer(db_path=db_path, capacity=200000, auto_trim=False)

        try:
            # Create 1,000 named channels and add to a group
            channels = []
            for i in range(1000):
                channel = f"benchmark-channel-aio-{i}"
                await layer.group_add("benchmark-group-aiosqlite", channel)
                channels.append(channel)

            # Send 100 group messages (will be delivered to all 1,000 channels)
            for i in range(100):
                await layer.group_send(
                    "benchmark-group-aiosqlite", {"type": "test.message", "num": i}
                )

            # Receive all messages from all channels
            total_received = 0
            for channel in channels:
                for i in range(100):
                    msg = await layer.receive(channel)
                    assert msg["type"] == "test.message"
                    total_received += 1

            return total_received
        finally:
            try:
                await layer.flush()
            except:
                pass
            try:
                await layer.close()
            except:
                pass

    result = benchmark(lambda: run_async(run_benchmark()))
    assert result == 100000  # 1,000 channels × 100 messages
