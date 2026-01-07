"""
Management command to clean expired channel messages and optimize the database.

This command performs two maintenance tasks:
1. Removes expired messages and group memberships
2. Runs VACUUM to reclaim disk space and optimize the database

Usage:
    python manage.py clean_channels
    python manage.py clean_channels --no-vacuum  # Skip VACUUM step

This is useful for scheduled maintenance, especially when auto_trim is disabled.
"""

from channels_lite.layers.aio import AIOSQLiteChannelLayer
from channels_lite.layers.core import SQLiteChannelLayer

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.core.management.base import BaseCommand
from django.db import connections


class Command(BaseCommand):
    help = "Clean expired channel messages and optimize the database"

    def add_arguments(self, parser):
        parser.add_argument(
            "--no-vacuum",
            action="store_true",
            help="Skip the VACUUM operation (only clean expired messages)",
        )

    def handle(self, *args, **options):
        channel_layer = get_channel_layer()

        if channel_layer is None:
            self.stderr.write(
                self.style.ERROR("No channel layer configured in settings")
            )
            return

        if not isinstance(channel_layer, (SQLiteChannelLayer, AIOSQLiteChannelLayer)):
            self.stderr.write(
                self.style.ERROR(
                    f"This command only works with SQLiteChannelLayer or AIOSQLiteChannelLayer.\n"
                    f"Current layer: {type(channel_layer).__name__}"
                )
            )
            return

        self.stdout.write("Cleaning expired messages and group memberships...")
        try:
            async_to_sync(channel_layer.clean_expired)()
            self.stdout.write(self.style.SUCCESS("✓ Expired messages cleaned"))
        except Exception as e:
            self.stderr.write(
                self.style.ERROR(f"Failed to clean expired messages: {e}")
            )
            return

        if not options["no_vacuum"]:
            self.stdout.write("Running VACUUM to optimize database...")
            try:
                self._vacuum_database(channel_layer)
                self.stdout.write(self.style.SUCCESS("✓ Database optimized"))
            except Exception as e:
                self.stderr.write(self.style.ERROR(f"Failed to vacuum database: {e}"))
                return

        self.stdout.write(
            self.style.SUCCESS("\nChannel layer maintenance completed successfully!")
        )

    def _vacuum_database(self, channel_layer):
        if isinstance(channel_layer, SQLiteChannelLayer):
            # Django ORM layer - use Django's connection
            db_alias = channel_layer.database
            connection = connections[db_alias]
            with connection.cursor() as cursor:
                cursor.execute("VACUUM")
            self.stdout.write(f"  Database: {db_alias}")

        elif isinstance(channel_layer, AIOSQLiteChannelLayer):

            async def _async_vacuum():
                async with channel_layer.connection() as conn:
                    await conn.execute("VACUUM")
                    await conn.commit()

            async_to_sync(_async_vacuum)()
            self.stdout.write(f"  Database: {channel_layer.db_path}")
