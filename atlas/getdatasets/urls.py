from django.conf.urls import  include, url
from atlas.getdatasets import views

urlpatterns = [
    url(r'^$', views.request_data_form,       name='request_data_form'),
]


