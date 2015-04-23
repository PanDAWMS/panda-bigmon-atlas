# Django
from django.db.models import Q

__all__ = ['Parameter','Parametrized']

class Parameter(object):
    def __init__(self, id=0, label=0, name=0, model_field=0, get_Q=0):
        if id:
            self.id=id
        if label:
            self.label=label
        if model_field:
            self.model_field=model_field
        if get_Q:
            self.get_Q = get_Q
        if name:
            self.name = name

    def update(self, name):
        self.id=getattr(self,'id',name)
        self.label=getattr(self,'label',name)
        self.model_field=getattr(self,'model_field',name)
        self.name = getattr(self,'name',name)
        return self

    def get_Q(self, value):
       # value = values.get(self.name, 0)
        if value:
            if value != 'None':
                return Q( **{ self.model_field+'__icontains' : value } )
            else:
                return Q( **{ self.model_field+'__exact' : '' } )


class Parametrized(object):

    def __init__(self, data=None, name=''):
        self.parameters = self.get_param_list()
        self.context = { 'params': self.parameters }
        if self.__class__.__dict__.has_key('Meta'):
            mcls = self.__class__.__dict__['Meta']
            for k, v in mcls.__dict__.items():
                if k[0] != '_' :
                    setattr(self, k, v)
                    self.context[k] = v
        pass #self.columns = SortedDict(self.base_columns.items())#deepcopy(self.base_columns)

    def process_request(self, request, name='parametrized'):
        setattr(request, name, self)

    def process_response(self, request, response):
        return response

    def get_param_list(self):
        all = self.__class__.__dict__
        ret = []
        for n, v in all.items():
            if n[0]=='_' or n == 'Meta':
                continue
            ret.append( v.update(n) )
        return ret

    def parse_parameters(self, request):
        parameters = {}
        for param in self.parameters:
            value = request.GET.get(param.name, 0)
            if value:
                parameters[param] = value
        return  parameters

    def apply_filters(self, request, qs, *args,**kwargs):
        """
        Overload DataTables method for filtering by additional elements of the page
        :return: filtered queryset
        """

        for param,value in self.parse_parameters(request).items():
            qs = qs.filter(param.get_Q(value))

        return qs
