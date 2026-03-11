from rest_framework import serializers
from .models import SearchTopic, CrawledPhone


class SearchTopicSerializer(serializers.ModelSerializer):
    class Meta:
        model = SearchTopic
        fields = [
            "id",
            "name",
            "query_text",
            "region",
            "pages_count",
        ]


class CrawledPhoneSerializer(serializers.ModelSerializer):
    topic_id = serializers.IntegerField(source="crawled_data.search_result.history.topic_id")

    class Meta:
        model = CrawledPhone
        fields = [
            "id",
            "phone",
            "phone_raw",
            "topic_id",
            "created_at",
        ]