from django.conf.urls import  include, url
from atlas.prodjob import views

urlpatterns = [
    url(r'^$', views.request_jobs,       name='request_jobs'),
    #url(r'^jobs_action/$', 'atlas.prodjob.views.jobs_action', name='jobs_action'),
    url(r'^jobs_action/(?P<action>\w+)/$', views.jobs_action, name='jobs_action'),
    url(r'^get_jobs/$', views.get_jobs, name='get_jobs'),

]


