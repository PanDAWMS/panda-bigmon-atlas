class ProdMonDBRouter(object):
    def db_for_read(self, model, **hints):
        if model._meta.app_label == 'prodtask':
            return 'deft'
        return None

    def db_for_write(self, model, **hints):
        if model._meta.app_label == 'prodtask':
            return 'deft'
        return None

    def allow_relation(self, obj1, obj2, **hints):
        return None

    def allow_migrate(self, db, model):
        return None
