__author__ = 'sbel'

from django.db import models
from django.utils import timezone

class VomsUser(models.Model):
    username = models.CharField(max_length=60, db_column='username', primary_key=True)
    dn = models.CharField(max_length=255, db_column='dn')
    ca = models.CharField(max_length=255, db_column='ca')
    added_on = models.DateTimeField(auto_now_add=True, db_column='added_on')

    def save(self, *args, **kwargs):
        if not self.added_on:
            self.added_on = timezone.now()
        super(VomsUser, self).save(*args, **kwargs)


    class Meta:
        managed = True
        db_table = u'"ATLAS_DEFT"."VOMS_USERS_MAP"'
        unique_together = (("username", "dn"),)
