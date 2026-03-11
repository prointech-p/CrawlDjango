from django.contrib import admin
from django.http import HttpResponseRedirect
from django.utils.html import format_html, format_html_join
from django.urls import reverse
from django.db.models import Count, Q
from .models import (
    SearchTopic, SearchHistory, SearchResult, UrlExclusion,
    CrawledData, CrawledPhone, CrawledEmail, CrawledAddress
)

admin.site.site_header = 'Парсер Яндекса | Администрирование'
admin.site.site_title = 'Парсер Яндекса'
admin.site.index_title = 'Панель управления парсером'


class SearchHistoryInline(admin.TabularInline):
    """Инлайн для истории поиска в теме"""
    model = SearchHistory
    fields = ['search_datetime', 'results_count', 'status', 'view_results_link']
    readonly_fields = ['search_datetime', 'results_count', 'status', 'view_results_link']
    extra = 0
    can_delete = False
    max_num = 10

    def view_results_link(self, obj):
        if obj.pk:
            app_label = SearchResult._meta.app_label
            model_name = SearchResult._meta.model_name

            url = reverse(
                f'admin:{app_label}_{model_name}_changelist'
            ) + f'?history__id__exact={obj.pk}'

            return format_html(
                '<a class="button" href="{}">Просмотреть результаты ({})</a>',
                url,
                obj.results_count
            )

        return "-"

    view_results_link.short_description = 'Результаты'
    view_results_link.allow_tags = True


class CrawledDataInline(admin.StackedInline):
    """Инлайн для данных краулинга в результатах поиска"""
    model = CrawledData
    fields = [
        'organization_name', 
        'http_status', 
        'has_phones', 
        # 'has_emails', 
        # 'has_addresses', 
        'view_details_link'
    ]
    readonly_fields = [
        'organization_name', 
        'http_status', 
        'has_phones', 
        # 'has_emails', 
        # 'has_addresses', 
        'view_details_link'
    ]
    extra = 0
    max_num = 1
    can_delete = False
    
    def has_phones(self, obj):
        count = obj.phones.count() if obj.pk else 0
        return format_html('<span style="color: {};">{}</span>',
                         'green' if count > 0 else 'gray', f'📞 {count} тел.')
    has_phones.short_description = 'Телефоны'
    
    # def has_emails(self, obj):
    #     count = obj.emails.count() if obj.pk else 0
    #     return format_html('<span style="color: {};">{}</span>',
    #                      'green' if count > 0 else 'gray', f'✉️ {count} email')
    # has_emails.short_description = 'Email'
    
    def view_details_link(self, obj):
        if obj.pk:
            url = reverse('admin:app_crawleddata_change', args=[obj.pk])
            return format_html('<a class="button" href="{}">Подробнее</a>', url)
        return "-"
    view_details_link.short_description = 'Действия'


class CrawledPhoneInline(admin.TabularInline):
    """Инлайн для телефонов в данных краулинга"""
    model = CrawledPhone
    fields = ['phone', 'phone_raw', 'created_at']
    readonly_fields = ['created_at']
    extra = 0
    ordering = ['-created_at']


class CrawledEmailInline(admin.TabularInline):
    """Инлайн для email в данных краулинга"""
    model = CrawledEmail
    fields = ['email', 'created_at']
    readonly_fields = ['created_at']
    extra = 0
    ordering = ['-created_at']


@admin.register(SearchTopic)
class SearchTopicAdmin(admin.ModelAdmin):
    """Админка для тем поиска"""
    list_display = [
        'id', 'name', 'region', 'pages_count', 
        'total_searches', 'total_results', 'created_at_short'
    ]
    list_display_links = ['id', 'name']
    list_filter = ['region', 'created_at']
    search_fields = ['name', 'query_text', 'region']
    readonly_fields = ['created_at', 'updated_at', 'total_searches', 'total_results']
    inlines = [SearchHistoryInline]
    
    fieldsets = (
        ('Основная информация', {
            'fields': ('name', 'query_text', 'region', 'pages_count')
        }),
        ('Статистика', {
            'fields': ('total_searches', 'total_results'),
            'classes': ('collapse',)
        }),
        ('Системная информация', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def created_at_short(self, obj):
        return obj.created_at.strftime('%d.%m.%Y %H:%M')
    created_at_short.short_description = 'Создана'
    created_at_short.admin_order_field = 'created_at'
    
    def total_searches(self, obj):
        app_label = obj._meta.app_label
        model_name = 'searchhistory'
        count = obj.search_histories.count()
        url = reverse(f'admin:{app_label}_{model_name}_changelist') + f'?topic__id__exact={obj.id}'
        return format_html('<a href="{}">{}</a>', url, count)
    total_searches.short_description = 'Всего поисков'
    
    def total_results(self, obj):
        # Оптимизированный подсчет через annotate
        result = obj.search_histories.aggregate(
            total=Count('search_results')
        )['total']
        return result or 0
    total_results.short_description = 'Всего результатов'
    
    def get_queryset(self, request):
        return super().get_queryset(request).prefetch_related('search_histories')


@admin.register(SearchHistory)
class SearchHistoryAdmin(admin.ModelAdmin):
    """Админка для истории поиска"""
    list_display = [
        'id', 'topic_link', 'search_datetime_short', 
        'results_count', 'colored_status', 'duration'
    ]
    list_display_links = ['id']
    list_filter = ['status', 'created_at', 'search_datetime']
    search_fields = ['topic__name', 'error_message']
    readonly_fields = ['created_at', 'search_datetime']
    list_select_related = ['topic']
    
    fieldsets = (
        ('Основная информация', {
            'fields': ('topic', 'search_datetime', 'results_count', 'status')
        }),
        ('Ошибки', {
            'fields': ('error_message',),
            'classes': ('collapse',),
            'description': 'Информация об ошибках (если есть)'
        }),
        ('Системная информация', {
            'fields': ('created_at',),
            'classes': ('collapse',)
        }),
    )
    
    def topic_link(self, obj):
        app_label = obj.topic._meta.app_label
        model_name = obj.topic._meta.model_name
        url = reverse(
            f'admin:{app_label}_{model_name}_change',
            args=[obj.topic_id]
        )
        return format_html('<a href="{}">{}</a>', url, obj.topic.name)
    topic_link.short_description = 'Тема'
    topic_link.admin_order_field = 'topic__name'
    
    def search_datetime_short(self, obj):
        return obj.search_datetime.strftime('%d.%m.%Y %H:%M:%S')
    search_datetime_short.short_description = 'Время поиска'
    search_datetime_short.admin_order_field = 'search_datetime'
    
    def colored_status(self, obj):
        colors = {
            'success': 'green',
            'error': 'red',
        }
        status_display = dict(SearchHistory.Status.choices).get(obj.status, obj.status)
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            colors.get(obj.status, 'black'),
            status_display
        )
    colored_status.short_description = 'Статус'
    colored_status.admin_order_field = 'status'
    
    def duration(self, obj):
        if obj.search_datetime and obj.created_at:
            delta = obj.created_at - obj.search_datetime
            seconds = delta.total_seconds()
            if seconds < 60:
                return f'{seconds:.1f} сек'
            else:
                return f'{seconds/60:.1f} мин'
        return '-'
    duration.short_description = 'Длительность'
    
    actions = ['view_results', 'rerun_search']
    
    def view_results(self, request, queryset):
        if queryset.count() == 1:
            history = queryset.first()
            app_label = SearchResult._meta.app_label
            model_name = SearchResult._meta.model_name
            url = reverse(
                f'admin:{app_label}_{model_name}_changelist'
            ) + f'?history__id__exact={history.id}'
            return HttpResponseRedirect(url)
        else:
            self.message_user(request, 'Выберите только одну запись для просмотра результатов')
    view_results.short_description = '👁️ Просмотреть результаты'
    

@admin.register(SearchResult)
class SearchResultAdmin(admin.ModelAdmin):
    """Админка для результатов поиска"""
    list_display = [
        'id', 
        'history_link', 
        'domain', 
        'title_preview', 
        'position', 
        # 'status_icons', 
        'created_at_short'
    ]
    list_display_links = ['id']
    list_filter = [
        'processed', 
        'skipped', 
        'created_at', 
        'history__topic__name', 
        'domain'
    ]
    search_fields = ['url', 'title', 'snippet', 'domain']
    readonly_fields = ['created_at', 'crawled_status', 'view_crawled_link']
    list_select_related = ['history__topic']
    inlines = [CrawledDataInline]
    
    fieldsets = (
        ('Основная информация', {
            'fields': ('history', 'url', 'domain', 'title', 'snippet')
        }),
        ('Позиция и обработка', {
            'fields': ('position', 'processed', 'processed_at', 'skipped', 'skip_reason')
        }),
        ('Статус краулинга', {
            'fields': ('crawled_status', 'view_crawled_link'),
            'classes': ('collapse',)
        }),
        ('Системная информация', {
            'fields': ('created_at',),
            'classes': ('collapse',)
        }),
    )
    
    def history_link(self, obj):
        # url = reverse('admin:app_searchhistory_change', args=[obj.history_id])
        url = ''
        topic_name = obj.history.topic.name if hasattr(obj.history, 'topic') else 'N/A'
        return format_html('<a href="{}">{} #{}</a>', url, topic_name, obj.history_id)
    history_link.short_description = 'История поиска'
    history_link.admin_order_field = 'history'
    
    def title_preview(self, obj):
        if obj.title:
            return obj.title[:70] + '...' if len(obj.title) > 70 else obj.title
        return '-'
    title_preview.short_description = 'Заголовок'
    
    def created_at_short(self, obj):
        return obj.created_at.strftime('%d.%m.%Y %H:%M')
    created_at_short.short_description = 'Создан'
    created_at_short.admin_order_field = 'created_at'
    
    def status_icons(self, obj):
        icons = []
        if obj.processed:
            icons.append('✅ Обработан')
        elif obj.skipped:
            icons.append('⏭️ Пропущен')
        else:
            icons.append('⏳ Ожидает')
        
        if obj.crawled_data:
            icons.append('📥 Скраулено')
        
        return format_html('<br>'.join(icons))
    status_icons.short_description = 'Статус'
    
    def crawled_status(self, obj):
        if hasattr(obj, 'crawled_data'):
            crawled = obj.crawled_data
            status = []
            if crawled.http_status:
                status.append(f'HTTP: {crawled.http_status}')
            if crawled.organization_name:
                status.append(f'Орг: {crawled.organization_name}')
            if crawled.error_message:
                status.append(f'Ошибка: {crawled.error_message[:50]}')
            
            return format_html('<br>'.join(status)) if status else 'Нет данных'
        return 'Не краулилось'
    crawled_status.short_description = 'Статус краулинга'
    
    def view_crawled_link(self, obj):
        if hasattr(obj, 'crawled_data'):
            url = reverse('admin:app_crawleddata_change', args=[obj.crawled_data.id])
            return format_html('<a class="button" href="{}">Просмотреть данные краулинга</a>', url)
        return format_html('<span style="color: gray;">Нет данных краулинга</span>')
    view_crawled_link.short_description = 'Данные краулинга'
    
    actions = ['mark_processed', 'mark_skipped', 'run_crawling']
    
    def mark_processed(self, request, queryset):
        updated = queryset.update(processed=True, skipped=False)
        self.message_user(request, f'Отмечено {updated} результатов как обработанные')
    mark_processed.short_description = '✅ Отметить как обработанные'
    
    def mark_skipped(self, request, queryset):
        updated = queryset.update(skipped=True, processed=False, skip_reason='Отмечено вручную')
        self.message_user(request, f'Отмечено {updated} результатов как пропущенные')
    mark_skipped.short_description = '⏭️ Отметить как пропущенные'
    
    def run_crawling(self, request, queryset):
        count = queryset.filter(processed=False, skipped=False).count()
        self.message_user(request, f'Запущен краулинг для {count} результатов')
    run_crawling.short_description = '🕷️ Запустить краулинг'
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related(
            'history__topic'
        ).prefetch_related('crawled_data')


@admin.register(UrlExclusion)
class UrlExclusionAdmin(admin.ModelAdmin):
    """Админка для исключений URL"""
    list_display = ['id', 'url_pattern', 'description_short', 'is_active', 'created_at_short']
    list_display_links = ['id', 'url_pattern']
    list_filter = ['is_active', 'created_at']
    search_fields = ['url_pattern', 'description']
    readonly_fields = ['created_at']
    list_editable = ['is_active']
    
    fieldsets = (
        (None, {
            'fields': ('url_pattern', 'description', 'is_active')
        }),
        ('Системная информация', {
            'fields': ('created_at',),
            'classes': ('collapse',)
        }),
    )
    
    def description_short(self, obj):
        if obj.description:
            return obj.description[:50] + '...' if len(obj.description) > 50 else obj.description
        return '-'
    description_short.short_description = 'Описание'
    
    def created_at_short(self, obj):
        return obj.created_at.strftime('%d.%m.%Y %H:%M')
    created_at_short.short_description = 'Создано'
    
    actions = ['activate', 'deactivate']
    
    def activate(self, request, queryset):
        updated = queryset.update(is_active=True)
        self.message_user(request, f'Активировано {updated} исключений')
    activate.short_description = '✅ Активировать'
    
    def deactivate(self, request, queryset):
        updated = queryset.update(is_active=False)
        self.message_user(request, f'Деактивировано {updated} исключений')
    deactivate.short_description = '❌ Деактивировать'


@admin.register(CrawledData)
class CrawledDataAdmin(admin.ModelAdmin):
    """Админка для данных краулинга"""
    list_display = [
        'id', 
        'search_result_link', 
        'organization_name', 
        'http_status', 
        'data_summary', 
        'created_at_short'
    ]
    list_display_links = ['id', 'search_result_link']
    list_filter = ['http_status', 'created_at', 'updated_at']
    search_fields = ['url', 'organization_name', 'error_message']
    readonly_fields = [
        'created_at', 
        'updated_at', 
        'phones_count', 
        'emails_count', 
        # 'addresses_count'
    ]
    
    fieldsets = (
        ('Основная информация', {
            'fields': ('search_result', 'url', 'organization_name')
        }),
        ('Техническая информация', {
            'fields': ('http_status', 'raw_html'),
            'classes': ('collapse',)
        }),
        ('Ошибки', {
            'fields': ('error_message',),
            'classes': ('collapse',)
        }),
        ('Статистика', {
            'fields': (
                'phones_count', 
                'emails_count', 
                # 'addresses_count'
                ),
        }),
        ('Системная информация', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    # inlines = [CrawledPhoneInline, CrawledEmailInline, CrawledAddressInline]
    inlines = [CrawledPhoneInline]
    
    def search_result_link(self, obj):
        app_label = obj.search_result._meta.app_label
        model_name = obj.search_result._meta.model_name
        url = reverse(
            f'admin:{app_label}_{model_name}_change',
            args=[obj.search_result_id]
        )
        return format_html('<a href="{}">Результат #{} →</a>', url, obj.search_result_id)
    search_result_link.short_description = 'Результат поиска'
    
    def created_at_short(self, obj):
        return obj.created_at.strftime('%d.%m.%Y %H:%M')
    created_at_short.short_description = 'Создано'
    created_at_short.admin_order_field = 'created_at'
    
    def data_summary(self, obj):
        phones = obj.phones.count()
        emails = obj.emails.count()
        addresses = obj.addresses.count()
        
        icons = []
        if phones:
            icons.append(f'📞 {phones}')
        # if emails:
        #     icons.append(f'✉️ {emails}')
        # if addresses:
        #     icons.append(f'📍 {addresses}')

        if not icons:
            return '❌ Нет данных'
        
        return format_html_join('<br>', '{}', ((icon,) for icon in icons))
    data_summary.short_description = 'Найденные данные'
    
    def phones_count(self, obj):
        count = obj.phones.count()
        url = reverse('admin:app_crawledphone_changelist') + f'?crawled_data__id__exact={obj.id}'
        return format_html('<a href="{}">{}</a>', url, count)
    phones_count.short_description = 'Телефоны'
    
    def emails_count(self, obj):
        count = obj.emails.count()
        url = reverse('admin:app_crawledemail_changelist') + f'?crawled_data__id__exact={obj.id}'
        return format_html('<a href="{}">{}</a>', url, count)
    emails_count.short_description = 'Email'
    
    # def addresses_count(self, obj):
    #     count = obj.addresses.count()
    #     url = reverse('admin:app_crawledaddress_changelist') + f'?crawled_data__id__exact={obj.id}'
    #     return format_html('<a href="{}">{}</a>', url, count)
    # addresses_count.short_description = 'Адреса'
    
    actions = ['recrawl', 'export_data']
    
    def recrawl(self, request, queryset):
        for crawled in queryset:
            # Здесь можно вызвать задачу для повторного краулинга
            self.message_user(request, f'Запущен повторный краулинг для {crawled.url[:50]}...')
    recrawl.short_description = '🔄 Перекраулить'
    
    def export_data(self, request, queryset):
        # Здесь можно реализовать экспорт в CSV/Excel
        self.message_user(request, f'Экспортировано {queryset.count()} записей')
    export_data.short_description = '📥 Экспортировать данные'
    
    def get_queryset(self, request):
        return super().get_queryset(request).prefetch_related(
            'phones', 'emails', 'addresses'
        )


@admin.register(CrawledPhone)
class CrawledPhoneAdmin(admin.ModelAdmin):
    """Админка для найденных телефонов"""
    list_display = [
        'id', 
        'phone', 
        'crawled_link', 
        'phone_raw_short', 
        'created_at_short'
    ]
    list_display_links = ['id', 'phone']
    list_filter = ['created_at']
    search_fields = ['phone', 'phone_raw']
    readonly_fields = ['created_at']
    
    fieldsets = (
        (None, {
            'fields': ('crawled_data', 'phone', 'phone_raw', 'context')
        }),
        ('Системная информация', {
            'fields': ('created_at',),
            'classes': ('collapse',)
        }),
    )
    
    def crawled_link(self, obj):
        app_label = obj.crawled_data._meta.app_label
        model_name = obj.crawled_data._meta.model_name
        url = reverse(f'admin:{app_label}_{model_name}_change', args=[obj.crawled_data_id])
        return format_html('<a href="{}">Данные #{} →</a>', url, obj.crawled_data_id)
    crawled_link.short_description = 'Источник'
    
    def phone_raw_short(self, obj):
        if obj.phone_raw:
            return obj.phone_raw[:50] + '...' if len(obj.phone_raw) > 50 else obj.phone_raw
        return '-'
    phone_raw_short.short_description = 'Исходный текст'
    
    def created_at_short(self, obj):
        return obj.created_at.strftime('%d.%m.%Y %H:%M')
    created_at_short.short_description = 'Найден'


@admin.register(CrawledEmail)
class CrawledEmailAdmin(admin.ModelAdmin):
    """Админка для найденных email"""
    list_display = ['id', 'email', 'crawled_link', 'context_short', 'created_at_short']
    list_display_links = ['id', 'email']
    list_filter = ['created_at']
    search_fields = ['email', 'context']
    readonly_fields = ['created_at']
    
    def crawled_link(self, obj):
        app_label = obj.crawled_data._meta.app_label
        model_name = obj.crawled_data._meta.model_name
        url = reverse(f'admin:{app_label}_{model_name}_change', args=[obj.crawled_data_id])
        return format_html('<a href="{}">Данные #{} →</a>', url, obj.crawled_data_id)
    crawled_link.short_description = 'Источник'
    
    def context_short(self, obj):
        if obj.context:
            return obj.context[:70] + '...' if len(obj.context) > 70 else obj.context
        return '-'
    context_short.short_description = 'Контекст'
    
    def created_at_short(self, obj):
        return obj.created_at.strftime('%d.%m.%Y %H:%M')
    created_at_short.short_description = 'Найден'


# @admin.register(CrawledAddress)
# class CrawledAddressAdmin(admin.ModelAdmin):
#     """Админка для найденных адресов"""
#     list_display = ['id', 'address_short', 'crawled_link', 'has_cleaned', 'created_at_short']
#     list_display_links = ['id', 'address_short']
#     list_filter = ['created_at']
#     search_fields = ['address', 'address_cleaned']
#     readonly_fields = ['created_at']
    
#     fieldsets = (
#         (None, {
#             'fields': ('crawled_data', 'address', 'address_cleaned', 'context')
#         }),
#         ('Системная информация', {
#             'fields': ('created_at',),
#             'classes': ('collapse',)
#         }),
#     )
    
#     def address_short(self, obj):
#         if obj.address:
#             return obj.address[:70] + '...' if len(obj.address) > 70 else obj.address
#         return '-'
#     address_short.short_description = 'Адрес'
    
#     def crawled_link(self, obj):
#         url = reverse('admin:app_crawleddata_change', args=[obj.crawled_data_id])
#         return format_html('<a href="{}">Данные #{} →</a>', url, obj.crawled_data_id)
#     crawled_link.short_description = 'Источник'
    
#     def has_cleaned(self, obj):
#         return obj.address_cleaned is not None and obj.address_cleaned != ''
#     has_cleaned.boolean = True
#     has_cleaned.short_description = 'Очищен'
    
#     def created_at_short(self, obj):
#         return obj.created_at.strftime('%d.%m.%Y %H:%M')
#     created_at_short.short_description = 'Найден'