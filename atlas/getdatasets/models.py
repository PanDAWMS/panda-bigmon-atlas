from django.db import models

class ProductionDatasetsExec(models.Model):
    name = models.CharField(max_length=200, db_column='NAME', primary_key=True)
    taskid = models.DecimalField(decimal_places=0, max_digits=10, db_column='TASK_ID', null=False, default=0)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=True)

    class Meta:
        app_label = "grisli"
        db_table = u"T_PRODUCTIONDATASETS_EXEC"

class TRequest(models.Model):
    request = models.CharField(max_length=200, db_column='REQUEST', null=True)
