from django.conf import settings
from channels.layers import get_channel_layer


class ChannelsRouter:
    """
    A router to control all database operations on models in the channels_lite application.
    """

    route_app_labels = {"channels_lite"}

    def __init__(self, *args, **kwargs):
        channel_layer = get_channel_layer()
        self.database = getattr(channel_layer, "database", "default")

    def db_for_read(self, model, **hints):
        """
        Attempts to read channels_lite models go to the channels database.
        """
        if model._meta.app_label in self.route_app_labels:  # noqa
            return self.database
        return None

    def db_for_write(self, model, **hints):
        """
        Attempts to write channels_lite models go to the channels database.
        """
        if model._meta.app_label in self.route_app_labels:  # noqa
            return self.database
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):  # noqa
        """
        Make sure the channels_lite app only appears in the channels database.
        """
        if app_label in self.route_app_labels:
            return db == self.database
        return None
