from django.conf.urls import patterns, include, url
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
### #FIXME admin.autodiscover()

import views as todo_views
#import views as pandajob_views

urlpatterns = patterns('',
    ### PanDA jobs
    url(r'^/todoTaskDescription/(?P<taskid>\d+)/$', todo_views.todoTaskDescription, name='todoTaskDescription'),
)


