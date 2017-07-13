from django.db import models
from django.db import connections

from atlas.prodtask.models import ProductionTask, prefetch_id


class PackageTest(models.Model):

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='ART_ID', primary_key=True)
    nightly_release = models.CharField(max_length=40, db_column='NIGHTLY_RELEASE_SHORT')
    project = models.CharField(max_length=40, db_column='PROJECT')
    platform = models.CharField(max_length=40, db_column='PLATFORM')
    nightly_tag = models.CharField(max_length=40, db_column='NIGHTLY_TAG')
    sequence_tag = models.CharField(max_length=20, db_column='SEQUENCE_TAG')
    package = models.CharField(max_length=60, db_column='PACKAGE')
    task = models.ForeignKey(ProductionTask, db_column='TASK_ID')

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('dev_db',u'T_ART_SEQ','T_ART','ART_ID')
        super(PackageTest, self).save(*args, **kwargs)


    class Meta:

        db_table = u"T_ART"

class TestsInTasks(models.Model):

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TEST_ID', primary_key=True)
    test_index = models.DecimalField(decimal_places=0, max_digits=12, db_column='TEST_INDEX')
    name = models.CharField(max_length=200, db_column='TEST_NAME')
    package_test = models.ForeignKey(PackageTest, db_column='ART_ID')


    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('dev_db',u'T_ART_TEST_ID_SEQ','T_ART_TEST_IN_TASKS','T_ART_TEST_IN_TASKS')
        super(TestsInTasks, self).save(*args, **kwargs)


    class Meta:
        db_table = u"T_ART_TEST_IN_TASKS"
