from django.conf import settings


class ChannelsRouter:
    """
    A router to control all database operations on models in the channels_db application.
    """

    route_app_labels = {"channels_db"}

    def __init__(self, *args, **kwargs):
        self.database = "pop"       

    def db_for_read(self, model, **hints):
        """
        Attempts to read django_tasks models go to the tasks database.
        """
        if model._meta.app_label in self.route_app_labels:  # noqa
            return self.get_db()
        return None

    def db_for_write(self, model, **hints):
        """
        Attempts to write django_tasks models go to the tasks database.
        """
        if model._meta.app_label in self.route_app_labels:  # noqa
            return self.get_db()
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):  # noqa
        """
        Make sure the django_tasks app only appears in the tasks database.
        """
        if app_label in self.route_app_labels:
            return db == self.get_db()
        return None
 
    @classmethod
    def get_db(cls):
        try:
            return settings.CHANNELS_LAYER["default"]["OPTIONS"]["database"]
        except (AttributeError, KeyError):
            return 
