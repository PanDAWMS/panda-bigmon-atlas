class ProdMonDBRouter(object):
    def db_for_read(self, model, **hints):
        if model._meta.app_label == 'prodtask':
            return 'deft'
        if model._meta.app_label == 'panda':
            return 'panda'
        if model._meta.app_label == 'grisli':
            return 'grisli'
        if model._meta.app_label == "taskmon":
            return "deft_adcr"
        if model._meta.app_label == 'dev':
            return 'dev_db'
        if model._meta.app_label == 'panda_dev':
            return 'panda_dev'
        if model._meta.app_label == 'djcelery':
            return 'djcelery'
        return None

    def db_for_write(self, model, **hints):
        if model._meta.app_label == 'panda_dev':
            return 'panda_dev'
        if model._meta.app_label == 'prodtask':
            return 'deft'
        if model._meta.app_label == 'dev':
            return 'dev_db_wr'
        if model._meta.app_label == 'panda':
            return 'panda_wr'
        if model._meta.app_label == 'djcelery':
            return 'djcelery'
        return None

    def allow_relation(self, obj1, obj2, **hints):
        if ((obj1._meta.app_label == 'prodtask') and (obj2._meta.app_label == 'dev')) or\
                ((obj1._meta.app_label == 'dev') and (obj2._meta.app_label == 'dev')):
            return True
        return None

    def allow_migrate(self, db, model):
        return None
