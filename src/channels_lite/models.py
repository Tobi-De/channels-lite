from django.db import models


class Event(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField(db_index=True)
    channel_name = models.CharField(max_length=100)
    data = models.BinaryField()
    delivered = models.BooleanField(default=False)

    class Meta:
        indexes = [
            # Optimized for: WHERE channel_name=? AND delivered=0 AND expires_at>=? ORDER BY expires_at
            models.Index(fields=["channel_name", "delivered", "expires_at"]),
        ]

    def __str__(self):
        return f"{self.channel_name} - {self.id} - {self.expires_at}"


class GroupMembership(models.Model):
    group_name = models.CharField(max_length=100)
    channel_name = models.CharField(max_length=100)
    expires_at = models.DateTimeField(db_index=True)
    joined_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = [["group_name", "channel_name"]]
        indexes = [
            # Optimized for: WHERE group_name=? AND expires_at>=?
            models.Index(fields=["group_name", "expires_at"]),
        ]

    def __str__(self):
        return f"{self.channel_name} - {self.group_name}"
