from django.conf.urls import url
from rest_framework import routers
from . import views

router = routers.SimpleRouter()

urlpatterns = [
    url(r'^healthcheck/?$', views.HealthCheckView.as_view(), name='healthcheck'),
]