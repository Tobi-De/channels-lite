from django.db import models


class Event(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()
    channel_name = models.CharField(max_length=100)
    data = models.JSONField(default=dict)
    delivered = models.BooleanField(default=False)

    class Meta:
        indexes = [
            models.Index(fields=["channel_name", "expires_at"]),
        ]

    def __str__(self):
        return f"{self.channel_name} - {self.id} - {self.expires_at}"


class GroupMembership(models.Model):
    group_name = models.CharField(db_index=True)
    channel_name = models.CharField()
    expires_at = models.DateTimeField()
    joined_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = [["group_name", "channel_name"]]
        indexes = [
            models.Index(fields=["group_name", "joined_at"]),
        ]

    def __str__(self):
        return f"{self.channel_name} - {self.group_name}"
