# dashboard/views.py
from django.views.generic import TemplateView, ListView, DetailView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db.models import Count, Q, Sum
from django.utils import timezone
from datetime import timedelta
from django.shortcuts import get_object_or_404
from apps.core.models import SearchTopic, SearchHistory, SearchResult, CrawledPhone, CrawledData


class IndexView(LoginRequiredMixin, TemplateView):
    """Дашборд с основными метриками"""
    template_name = 'dashboard/index.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Получаем параметры фильтрации из GET запроса
        topic_id = self.request.GET.get('topic')
        page_number = self.request.GET.get('page')
        
        # Базовые queryset
        topics = SearchTopic.objects.all()
        
        # Фильтруем телефоны по теме
        if topic_id and topic_id != 'all':
            context['selected_topic'] = int(topic_id)
        else:
            context['selected_topic'] = 'all'
        
        # Фильтруем SearchResult по странице
        if page_number and page_number != 'all':
            try:
                context['selected_page'] = int(page_number)
            except ValueError:
                context['selected_page'] = 'all'
        else:
            context['selected_page'] = 'all'
        
        # Подготавливаем данные для графика по дням
        dates = []
        for i in range(4, -1, -1):
            date = timezone.now().date() - timedelta(days=i)
            dates.append(date.strftime('%Y-%m-%d'))
        
        # Подготавливаем данные для каждой темы
        topics_data = []
        for topic in topics:
            daily_counts = []
            for i in range(4, -1, -1):
                date = timezone.now().date() - timedelta(days=i)
                # Применяем фильтр по теме
                if topic_id and topic_id != 'all' and int(topic_id) != topic.id:
                    daily_counts.append(0)
                else:
                    count = CrawledPhone.objects.filter(
                        topic=topic,
                        created_at__date=date
                    ).count()
                    daily_counts.append(count)
            
            topics_data.append({
                'id': topic.id,
                'name': topic.name,
                'query_text': topic.query_text,
                'daily_counts': daily_counts,
                'total_phones': CrawledPhone.objects.filter(topic=topic).count(),
                'phones_last_5_days': sum(daily_counts)
            })
        
        context['dates'] = dates
        context['topics_data'] = topics_data
        
        # Общая статистика (с учетом фильтра по теме)
        if topic_id and topic_id != 'all':
            filtered_topics = [t for t in topics_data if t['id'] == int(topic_id)]
        else:
            filtered_topics = topics_data
        
        total_phones_sum = sum([t['total_phones'] for t in filtered_topics])
        new_phones_last_5_days = sum([t['phones_last_5_days'] for t in filtered_topics])
        
        context['total_phones_sum'] = total_phones_sum
        context['new_phones_last_5_days'] = new_phones_last_5_days
        
        # Статистика по страницам поисковой выдачи
        page_stats = {}
        for page in range(1, 6):
            
            search_results = SearchResult.objects.filter(
                page=page,
                crawled_data__isnull=False
            )
            
            if topic_id and topic_id != 'all':
                phone_count = CrawledPhone.objects.filter(
                    crawled_data__search_result__in=search_results,
                    topic_id=topic_id
                ).count()
            else:
                phone_count = CrawledPhone.objects.filter(
                    crawled_data__search_result__in=search_results
                ).count()
            
            page_stats[page] = phone_count
        
        context['page_stats'] = page_stats
        
        # Подготавливаем данные для графика по страницам (уже готовые, без фильтров)
        if context['selected_page'] != 'all':
            # Если выбрана конкретная страница
            context['page_chart_labels'] = [f'Страница {context["selected_page"]}']
            context['page_chart_data'] = [page_stats.get(context['selected_page'], 0)]
        else:
            # Если выбраны все страницы
            context['page_chart_labels'] = ['Страница 1', 'Страница 2', 'Страница 3', 'Страница 4', 'Страница 5']
            context['page_chart_data'] = [
                page_stats.get(1, 0),
                page_stats.get(2, 0),
                page_stats.get(3, 0),
                page_stats.get(4, 0),
                page_stats.get(5, 0)
            ]
        
        # Список страниц для фильтра
        context['pages_list'] = [1, 2, 3, 4, 5]
        context['topics'] = topics
        
        return context


class TopicListView(LoginRequiredMixin, ListView):
    """Список всех тем с готовыми данными"""
    model = SearchTopic
    template_name = 'dashboard/topic_list.html'
    context_object_name = 'topics'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Подготавливаем данные для каждой темы
        topics_with_stats = []
        for topic in context['topics']:
            topics_with_stats.append({
                'id': topic.id,
                'name': topic.name,
                'query_text': topic.query_text,
                'region': topic.region,
                'pages_count': topic.pages_count,
                'created_at': topic.created_at,
                'total_phones': CrawledPhone.objects.filter(topic=topic).count(),
                'total_queries': SearchHistory.objects.filter(topic=topic).count(),
                'last_updated': topic.updated_at
            })
        
        context['topics_with_stats'] = topics_with_stats
        return context


class TopicDetailView(LoginRequiredMixin, DetailView):
    """Детальная информация по теме"""
    model = SearchTopic
    template_name = 'dashboard/topic_detail.html'
    context_object_name = 'topic'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        topic = self.get_object()
        
        # Получаем параметр days из GET запроса (по умолчанию 5)
        days = self.request.GET.get('days', '5')
        try:
            days = int(days)
            if days < 1:
                days = 1
            if days > 30:
                days = 30
        except ValueError:
            days = 5

        # Получаем параметр filter_type: 'new' (только новые) или 'all' (включая обновлённые)
        filter_type = self.request.GET.get('filter_type', 'new')
        
        context['selected_days'] = days
        context['selected_filter_type'] = filter_type
        
        # История поисковых запросов
        search_histories = SearchHistory.objects.filter(topic=topic).order_by('-search_datetime')
        
        # Подготавливаем детальную информацию по каждому запросу
        search_details = []
        for history in search_histories[:20]:  # последние 20 запросов
            total_urls = history.search_results.count()
            processed_urls = history.search_results.filter(processed=True).count()
            phones_found = CrawledPhone.objects.filter(
                crawled_data__search_result__history=history,
                topic=topic
            ).count()
            
            search_details.append({
                'id': history.id,
                'search_datetime': history.search_datetime,
                'results_count': history.results_count,
                'status': history.status,
                'error_message': history.error_message,
                'total_urls': total_urls,
                'processed_urls': processed_urls,
                'phones_found': phones_found,
                'efficiency': (phones_found / total_urls * 100) if total_urls > 0 else 0
            })
        
        context['search_details'] = search_details
        
        # Статистика
        context['total_phones'] = CrawledPhone.objects.filter(topic=topic).count()
        
        # Новые телефоны за последние 30 дней
        thirty_days_ago = timezone.now() - timedelta(days=30)
        context['new_phones_30_days'] = CrawledPhone.objects.filter(
            topic=topic,
            created_at__gte=thirty_days_ago
        ).count()
        
        # Новые телефоны за выбранное количество дней
        days_ago = timezone.now() - timedelta(days=days)
        context['new_phones_selected_days'] = CrawledPhone.objects.filter(
            topic=topic,
            created_at__gte=days_ago
        ).count()
        
        # Новые телефоны за последние 5 дней (для быстрого доступа)
        five_days_ago = timezone.now() - timedelta(days=5)
        context['new_phones_5_days'] = CrawledPhone.objects.filter(
            topic=topic,
            created_at__gte=five_days_ago
        ).count()
        
        # Статистика по страницам для этой темы (все время)
        page_stats_all = {}
        for page in range(1, 6):
            
            histories = SearchHistory.objects.filter(topic=topic)
            
            search_results = SearchResult.objects.filter(
                history__in=histories,
                page=page,
                crawled_data__isnull=False
            ).distinct()
            
            phone_count = CrawledPhone.objects.filter(
                crawled_data__search_result__in=search_results,
                topic=topic
            ).count()
            
            page_stats_all[page] = phone_count
        
        context['page_stats_all'] = page_stats_all
        
        # Статистика по страницам за выбранное количество дней
        page_stats_selected = {}
        days_ago = timezone.now() - timedelta(days=days)
        
        for page in range(1, 6):
            
            histories = SearchHistory.objects.filter(topic=topic)
            
            search_results = SearchResult.objects.filter(
                history__in=histories,
                page=page,
                crawled_data__isnull=False
            ).distinct()
            
            # Фильтруем телефоны по дате создания
            phone_count = CrawledPhone.objects.filter(
                crawled_data__search_result__in=search_results,
                topic=topic,
                created_at__gte=days_ago
            ).count()
            
            page_stats_selected[page] = phone_count
        
        context['page_stats_selected'] = page_stats_selected
        
        # Списки телефонов
        # Все телефоны по теме
        all_phones = CrawledPhone.objects.filter(topic=topic).order_by('-created_at')
        context['all_phones'] = all_phones
        
        # Телефоны за выбранное количество дней
        if filter_type == 'new':
            # Только новые (созданные за период)
            recent_phones = CrawledPhone.objects.filter(
                topic=topic,
                created_at__gte=days_ago
            ).order_by('-created_at')
            context['recent_phones_label'] = f'Новые телефоны за последние {days} дней'
        else:
            # Все телефоны, обновлённые за период (включая перепроверенные)
            recent_phones = CrawledPhone.objects.filter(
                topic=topic,
                updated_at__gte=days_ago
            ).order_by('-updated_at')
            context['recent_phones_label'] = f'Телефоны (включая обновлённые) за последние {days} дней'
        
        context['recent_phones'] = recent_phones
        
        # Список дней для выбора (1-30)
        context['days_range'] = range(1, 8)
        
        return context