from django.db import models

class ProductionDatasetsExec(models.Model):
    name = models.CharField(max_length=200, db_column='NAME', primary_key=True)
    taskid = models.DecimalField(decimal_places=0, max_digits=10, db_column='TASK_ID', null=False, default=0)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=True)
    events =  models.DecimalField(decimal_places=0, max_digits=7, db_column='EVENTS', null=False, default=0)

    class Meta:
        app_label = "grisli"
        managed = False
        db_table = 'T_PRODUCTIONDATASETS_EXEC'

class TaskProdSys1(models.Model):
    taskid = models.DecimalField(decimal_places=0, max_digits=10, db_column='REQID', primary_key=True)
    total_events =  models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_EVENTS')
    task_name = models.CharField(max_length=130, db_column='TASKNAME')
    status = models.CharField(max_length=12, db_column='STATUS')

    class Meta:
        app_label = "grisli"
        managed = False
        db_table = 'T_TASK_REQUEST'


class TRequest(models.Model):
    request = models.CharField(max_length=200, db_column='REQUEST', null=True)
