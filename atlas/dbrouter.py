class ProdMonDBRouter(object):
    def db_for_read(self, model, **hints):
        if model._meta.app_label == 'auth':
            return 'default'
        if model._meta.app_label == 'prodtask':
            return 'deft'
        if model._meta.app_label == 'art':
            return 'deft'
        if model._meta.app_label == 'panda':
            return 'panda'
        if model._meta.app_label == "taskmon":
            return "deft_adcr"
        if model._meta.app_label == 'authtoken':
            return 'default'
        if model._meta.app_label == 'dev':
            return 'dev_db'
        if model._meta.app_label == 'panda_dev':
            return 'panda_dev'
        if model._meta.app_label == 'django_celery_results':
            return 'dev_db'
        if model._meta.app_label == 'django_celery_beat':
            return 'dev_db'
        if model._meta.app_label == 'django_cache':
            return 'dev_db'

        return None

    def db_for_write(self, model, **hints):
        if model._meta.app_label == 'auth':
            return 'default'
        if model._meta.app_label == 'panda_dev':
            return 'panda_dev'
        if model._meta.app_label == 'prodtask':
            return 'deft'
        if model._meta.app_label == 'art':
            return 'deft'
        if model._meta.app_label == 'dev':
            return 'dev_db_wr'
        if model._meta.app_label == 'authtoken':
            return 'default'
        if model._meta.app_label == 'panda':
            return 'panda_wr'
        if model._meta.app_label == 'django_celery_results':
            return 'dev_db_wr'
        if model._meta.app_label == 'django_celery_beat':
            return 'dev_db_wr'
        if model._meta.app_label == 'django_cache':
            return 'dev_db_wr'
        return None

    def allow_relation(self, obj1, obj2, **hints):
        if ((obj1._meta.app_label == 'prodtask') and (obj2._meta.app_label == 'dev')) or\
                ((obj1._meta.app_label == 'dev') and (obj2._meta.app_label == 'dev')) or \
                ((obj1._meta.app_label == 'auth') and (obj2._meta.app_label == 'authtoken') or
                ((obj1._meta.app_label == 'panda') and (obj2._meta.app_label == 'panda'))):
            return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        return None
