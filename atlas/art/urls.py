from django.conf.urls import patterns, include, url

urlpatterns = patterns('',

    url(r'^api/create_atr_task/$', 'atlas.art.views.create_atr_task', name='create_atr_task')
)


