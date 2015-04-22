# Decorator
from decorator import decorator

__all__ = ['datatable','parametrized','parametrized_datatable']

def datatable(datatable_class, name='datatable'):
    def datatable_f(f, request, *args, **kwargs):
        datatable_instance = datatable_class(request.GET, name)
        response = datatable_instance.process_request(request, name)
        if response is not None:
            return response
        response = f(request=request, *args, **kwargs)
        return datatable_instance.process_response(request, response)
    return decorator(datatable_f)

def parametrized(parametrized_class, param_name='parametrized', qs_obj=0):
    def parametrized_f(f, request, *args, **kwargs):
        parametrized_instance = parametrized_class(request.GET, param_name)
        response = parametrized_instance.process_request(request, param_name)
        if response is not None:
            return response
        if qs_obj:
            qs_o = getattr(request, qs_obj, 0)
            if qs_o:
                qs = qs_o.get_queryset()
                qs = parametrized_instance.apply_filters(request, qs, *args,**kwargs)
                qs_o.update_queryset(qs)
        response = f(request=request, *args, **kwargs)
        return parametrized_instance.process_response(request, response)
    return decorator(parametrized_f)


def parametrized_datatable(datatable_class, parametrized_class, name='datatable', param_name='parametrized'):
    def datatable_f(f, request, *args, **kwargs):
        parametrized_instance = parametrized_class(request.GET, param_name)
        response = parametrized_instance.process_request(request, param_name)
        if response is not None:
            return response

        datatable_instance = datatable_class(request.GET, name)

        qs = datatable_instance.get_queryset()
        qs = parametrized_instance.apply_filters(request, qs, *args,**kwargs)

        datatable_instance.update_queryset(qs)

        response = datatable_instance.process_request(request, name)
        if response is not None:
            return response
        response = f(request=request, *args, **kwargs)
        return datatable_instance.process_response(request, response)
    return decorator(datatable_f)
