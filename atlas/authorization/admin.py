"""Policy-backed Django admin boundaries; existing login/backends are unchanged."""

from django.contrib.admin import AdminSite, ModelAdmin

from .resources import can_access


def assigned_permissions(user):
    permissions = list(user.user_permissions.select_related('content_type'))
    for group in user.groups.all():
        permissions.extend(group.permissions.select_related('content_type'))
    return {f'{p.content_type.app_label}.{p.codename}' for p in permissions}


class PolicyAdminMixin:
    def get_inlines(self, request, obj=None):
        return tuple(policy_admin_class(inline) for inline in super().get_inlines(request, obj))

    def has_module_permission(self, request):
        return can_access(request.user, 'admin_module', 'view', groups=(),
            facts=lambda: {'assigned_permission': any(p.startswith(self.opts.app_label + '.')
                                                      for p in assigned_permissions(request.user))},
            object_id=self.opts.app_label, source='admin.module')

    def _policy_permission(self, request, action, obj=None):
        def facts():
            permissions = assigned_permissions(request.user)
            prefix = self.opts.app_label + '.'
            return {'assigned_permission': prefix + action + '_' + self.opts.model_name in permissions,
                    'assigned_change_permission': prefix + 'change_' + self.opts.model_name in permissions}
        return can_access(request.user, 'admin_model', action, groups=(), facts=facts,
                          object_id=self.opts.label_lower, source='admin.model')

    def has_view_permission(self, request, obj=None):
        return self._policy_permission(request, 'view', obj)

    def has_add_permission(self, request, obj=None):
        return self._policy_permission(request, 'add', obj)

    def has_change_permission(self, request, obj=None):
        return self._policy_permission(request, 'change', obj)

    def has_delete_permission(self, request, obj=None):
        return self._policy_permission(request, 'delete', obj)


class PolicyAdminSite(AdminSite):
    def has_permission(self, request):
        return can_access(request.user, 'admin_site', 'enter', groups=(),
                          facts={'staff': request.user.is_staff}, source='admin.site')

    def register(self, model_or_iterable, admin_class=None, **options):
        return super().register(model_or_iterable, policy_admin_class(admin_class or ModelAdmin), **options)


def policy_admin_class(base):
    if issubclass(base, PolicyAdminMixin):
        return base
    return type('Policy' + base.__name__, (PolicyAdminMixin, base), {'__module__': __name__})
