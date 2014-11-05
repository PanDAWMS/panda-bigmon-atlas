__author__ = 'sbel'

from django.db import models
from django.utils import timezone

class VomsUser(models.Model):
    id = models.AutoField(db_column='ID', primary_key=True)
    username = models.CharField(max_length=60, db_column='USERNAME', db_index=True)
    dn = models.CharField(max_length=255, db_column='DN', db_index=True)
    ca = models.CharField(max_length=255, db_column='CA')
    added_on = models.DateTimeField(auto_now_add=True, db_column='ADDED_ON')

    def save(self, *args, **kwargs):
        if not self.added_on:
            self.added_on = timezone.now()
        super(VomsUser, self).save(*args, **kwargs)


    class Meta:
        managed = True
        db_table = u'VOMS_USERS_MAP'
        app_label = 'auth'
        unique_together = (("username", "dn"),)
