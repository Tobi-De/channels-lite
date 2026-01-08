"""Pytest configuration for channels-lite tests."""

import os
import tempfile

import django
import pytest
from django.conf import settings

# Use a temporary file database that persists during the test session
TEST_DB = os.path.join(tempfile.gettempdir(), "channels_lite_test.db")


def pytest_configure():
    """Configure Django settings for tests."""
    if not settings.configured:
        # Remove old test database if it exists
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)

        settings.configure(
            DEBUG=True,
            DATABASES={
                "default": {
                    "ENGINE": "django.db.backends.sqlite3",
                    "NAME": TEST_DB,
                    "OPTIONS": {
                        "transaction_mode": "IMMEDIATE",
                        "timeout": 5,
                        # "init_command": """PRAGMA journal_mode=WAL;
                        #     PRAGMA synchronous=NORMAL;
                        #     PRAGMA temp_store=MEMORY;
                        #     PRAGMA mmap_size=134217728;
                        #     PRAGMA journal_size_limit=27103364;
                        #     PRAGMA cache_size=2000;""",
                    },
                }
            },
            INSTALLED_APPS=[
                "django.contrib.contenttypes",
                "django.contrib.auth",
                "channels",
                "channels_lite",
            ],
            USE_TZ=True,
            SECRET_KEY="test-secret-key",
            CHANNEL_LAYERS={
                "default": {
                    "BACKEND": "channels_lite.layers.core.SQLiteChannelLayer",
                    "OPTIONS": {
                        "database": "default",
                    },
                },
            },
        )
        django.setup()

        # Create tables using migrations
        from django.core.management import call_command

        call_command("migrate", "channels_lite", verbosity=0)


def pytest_unconfigure():
    """Clean up after tests."""
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
