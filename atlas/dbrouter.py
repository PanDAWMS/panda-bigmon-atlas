class ProdMonDBRouter(object):
    def db_for_read(self, model, **hints):
        if model._meta.app_label == 'task':
            return 'deft'
        if model._meta.app_label == 'jedi':
            return 'default'
        if model._meta.app_label == 'prodtask':
            return 'grisli'
        if model._meta.app_label == 'panda':
            return 'panda'
        return None

    def db_for_write(self, model, **hints):
        if model._meta.app_label == 'task':
            return 'deft'
        if model._meta.app_label == 'jedi':
            return 'default'
        if model._meta.app_label == 'prodtask':
            return 'grisli'
        if model._meta.app_label == 'panda':
            return 'panda'
        return None

    def allow_relation(self, obj1, obj2, **hints):
        return None

    def allow_migrate(self, db, model):
        return None
