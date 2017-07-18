from django.conf.urls import patterns, include, url

urlpatterns = patterns('',

    url(r'^api/create_atr_task/$', 'atlas.art.views.create_atr_task', name='create_atr_task'),
    url(r'^api/get_new_tasks/$', 'atlas.art.views.get_new_tasks', name='get_new_tasks')

)


