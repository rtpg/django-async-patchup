from django.urls import path
from . import views

urlpatterns = [
    path("", views.overview, name="biz_overview"),
    path("client/<int:pk>/", views.client_detail, name="biz_client"),
]
