from django.shortcuts import render

from rest_framework.generics import ListAPIView
from django.utils.dateparse import parse_datetime
from rest_framework.generics import ListAPIView
from rest_framework.filters import OrderingFilter

from apps.core.models import (
    SearchTopic, 
    CrawledPhone
)
from apps.core.serializers import (
    SearchTopicSerializer,
    CrawledPhoneSerializer,
)
from apps.core.pagination import StandardResultsSetPagination


class SearchTopicListAPIView(ListAPIView):
    """
    Возвращает список всех тем
    """
    queryset = SearchTopic.objects.all()
    serializer_class = SearchTopicSerializer


class CrawledPhoneListAPIView(ListAPIView):
    """
    Возвращает телефоны CrawledPhone по topic_id и дате since
    """
    serializer_class = CrawledPhoneSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [OrderingFilter]
    ordering = ['-created_at']

    def get_queryset(self):
        queryset = CrawledPhone.objects.select_related(
            'crawled_data__search_result__history__topic'
        )
        topic_id = self.request.query_params.get('topic_id')
        since = self.request.query_params.get('since')

        if topic_id:
            queryset = queryset.filter(
                crawled_data__search_result__history__topic_id=topic_id
            ).order_by('-created_at', '-id')

        if since:
            dt = parse_datetime(since)
            if dt:
                queryset = queryset.filter(created_at__gte=dt)

        return queryset