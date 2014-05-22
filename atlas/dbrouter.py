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
        return None

    def db_for_write(self, model, **hints):
        if model._meta.app_label == 'prodtask':
            return 'deft'
        return None

    def allow_relation(self, obj1, obj2, **hints):
        return None

    def allow_migrate(self, db, model):
        return None
