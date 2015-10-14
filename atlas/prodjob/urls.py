from django.conf.urls import patterns, include, url

urlpatterns = patterns('',
    url(r'^$', 'atlas.prodjob.views.request_jobs',       name='request_jobs'),
)


