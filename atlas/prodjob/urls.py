from django.conf.urls import  url
from atlas.prodjob.views import *

app_name='prodjob'


urlpatterns = [
    url(r'^$',request_jobs,       name='request_jobs'),
    url(r'^jobs_action/(?P<action>\w+)/$', jobs_action, name='jobs_action'),
    url(r'^get_jobs/$', get_jobs, name='get_jobs'),

]


