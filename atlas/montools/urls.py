from django.conf.urls import patterns, include, url
from django.conf import settings
from django.conf.urls.static import static
from django.views.generic import TemplateView

urlpatterns = patterns('',
    url(r'^/workinggroup/$', 'atlas.montools.views.groupSum',       name='Summary'),
    url(r'^test/$', 'atlas.montools.views.testPlot',       name='test'),
    url(r'^show/$', 'atlas.montools.views.showPlot',       name='show'),
)+ static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)


