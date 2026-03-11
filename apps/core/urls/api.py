from django.urls import path
from apps.core.views import (
    SearchTopicListAPIView,
    CrawledPhoneListAPIView
)

urlpatterns = [
    path("topics/", SearchTopicListAPIView.as_view(), name="api-topics"),
    path("phones/", CrawledPhoneListAPIView.as_view(), name="api-phones"),
]