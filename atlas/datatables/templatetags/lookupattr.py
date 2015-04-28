# Django
from django import template
from django.template import Context
from django.template.loader import select_template


# Django-DataTables
from core.datatables.utils import lookupattr

register = template.Library()
register.filter('lookupattr', lookupattr)

class ParametrizedNode(template.Node):
    def __init__(self, params_name):
        self.params_name = params_name
    def render(self, context):
        t = select_template(['parametrized/parametrized.html'])
        return t.render(Context(context.get(self.params_name).context))

@register.tag(name="parametrized")
def parametrized(parser, token):
    try:
        # split_contents() knows not to split quoted strings.
        tag_name, params_name = token.split_contents()
    except ValueError:
        raise template.TemplateSyntaxError("%r tag requires a single argument" % token.contents.split()[0])
    return ParametrizedNode(params_name)
