from django.urls import  re_path
from atlas.prodjob.views import *

app_name='prodjob'


urlpatterns = [
    re_path(r'^$',request_jobs,       name='request_jobs'),
    re_path(r'^jobs_action/(?P<action>\w+)/$', jobs_action, name='jobs_action'),
    re_path(r'^get_jobs/$', get_jobs, name='get_jobs'),

]


