from django.db import models
from django.utils import timezone


class SearchTopic(models.Model):
    """
    Модель тем поиска - содержит настройки для различных поисковых сценариев.
    
    Атрибуты:
        name: Название темы (например, "Стоматологии Москвы")
        query_text: Текст поискового запроса
        region: Регион для поиска (например, "Москва", "77")
        pages_count: Количество страниц выдачи для парсинга
        created_at: Дата и время создания записи
        updated_at: Дата и время последнего обновления
    """
    name = models.CharField(
        max_length=255,
        verbose_name="Название темы",
        help_text="Название темы"
    )
    query_text = models.TextField(
        verbose_name="Текст поискового запроса",
        help_text="Текст поискового запроса"
    )
    region = models.CharField(
        max_length=100,
        blank=True,
        null=True,
        verbose_name="Регион поиска",
        help_text="Регион поиска"
    )
    pages_count = models.IntegerField(
        default=1,
        verbose_name="Количество страниц выдачи",
        help_text="Количество страниц выдачи"
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Дата создания",
        help_text="Дата создания"
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        verbose_name="Дата последнего обновления",
        help_text="Дата последнего обновления"
    )
    
    class Meta:
        db_table = 'search_topics'
        verbose_name = 'Тема поиска'
        verbose_name_plural = 'Темы поиска'
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.name}"


class SearchHistory(models.Model):
    """
    Модель истории выполнения поисковых запросов.
    Фиксирует каждый запуск парсинга по теме.
    
    Атрибуты:
        topic: ID темы поиска (внешний ключ)
        search_datetime: Дата и время выполнения запроса
        results_count: Количество полученных результатов в выдаче
        page_size: Размер страницы поисковой выдачи (количество результатов на странице)
        status: Статус выполнения ('success' или 'error')
        error_message: Текст ошибки (если статус 'error')
        created_at: Дата создания записи
    """
    class Status(models.TextChoices):
        SUCCESS = 'success', 'Успешно'
        ERROR = 'error', 'Ошибка'
    
    topic = models.ForeignKey(
        SearchTopic,
        on_delete=models.CASCADE,
        related_name='search_histories',
        verbose_name="Тема поиска",
        help_text="ID темы поиска"
    )
    search_datetime = models.DateTimeField(
        default=timezone.now,
        verbose_name="Дата и время запроса",
        help_text="Дата и время запроса"
    )
    results_count = models.IntegerField(
        default=0,
        verbose_name="Количество полученных результатов",
        help_text="Количество полученных результатов"
    )
    page_size = models.IntegerField(
        default=50,
        verbose_name="Размер страницы выдачи",
        help_text="Количество результатов на одной странице поисковой выдачи"
    )
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        verbose_name="Статус",
        help_text="Статус: success/error"
    )
    error_message = models.TextField(
        blank=True,
        null=True,
        verbose_name="Сообщение об ошибке",
        help_text="Сообщение об ошибке"
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Дата создания записи",
        help_text="Дата создания записи"
    )
    
    class Meta:
        db_table = 'search_history'
        verbose_name = 'История поиска'
        verbose_name_plural = 'Истории поиска'
        ordering = ['-search_datetime']
        indexes = [
            models.Index(fields=['topic', '-search_datetime']),
        ]
    
    def __str__(self):
        return f"Поиск #{self.id} ({self.topic.name}) - {self.status}"


class UrlExclusion(models.Model):
    """
    Модель исключений URL.
    Содержит подстроки URL, которые следует игнорировать при обработке.
    """
    url_pattern = models.CharField(
        max_length=500,
        unique=True,
        verbose_name="Подстрока URL для исключения",
        help_text="Подстрока URL для исключения (например, 'yandex.ru')"
    )
    description = models.CharField(
        max_length=500,
        blank=True,
        null=True,
        verbose_name="Описание причины исключения",
        help_text="Описание причины исключения"
    )
    is_active = models.BooleanField(
        default=True,
        verbose_name="Активно ли исключение",
        help_text="Активно ли исключение"
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Дата создания",
        help_text="Дата создания"
    )
    
    class Meta:
        db_table = 'url_exclusions'
        verbose_name = 'Исключение URL'
        verbose_name_plural = 'Исключения URL'
        ordering = ['url_pattern']
    
    def __str__(self):
        return self.url_pattern


class SearchResult(models.Model):
    """Модель результатов поисковой выдачи."""
    history = models.ForeignKey(
        SearchHistory,
        on_delete=models.CASCADE,
        related_name='search_results',
        verbose_name="Запись истории поиска",
        help_text="ID записи истории поиска"
    )
    title = models.CharField(
        max_length=500,
        blank=True,
        null=True,
        verbose_name="Заголовок результата",
        help_text="Заголовок результата"
    )
    url = models.TextField(
        verbose_name="URL страницы",
        help_text="URL страницы"
    )
    domain = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        verbose_name="Домен",
        help_text="Домен"
    )
    snippet = models.TextField(
        blank=True,
        null=True,
        verbose_name="Сниппет из выдачи",
        help_text="Сниппет из выдачи"
    )
    position = models.IntegerField(
        blank=True,
        null=True,
        verbose_name="Позиция в выдаче",
        help_text="Позиция в выдаче"
    )
    page = models.IntegerField(
        blank=True,
        null=True,
        verbose_name="Номер страницы выдачи",
        help_text="Номер страницы поисковой выдачи (1, 2, 3...)"
    )
    processed = models.BooleanField(
        default=False,
        verbose_name="Флаг обработки (crawling)",
        help_text="Флаг обработки (crawling)"
    )
    processed_at = models.DateTimeField(
        blank=True,
        null=True,
        verbose_name="Дата и время crawling-а",
        help_text="Дата и время crawling-а"
    )
    skipped = models.BooleanField(
        default=False,
        verbose_name="Был пропущен (исключение)",
        help_text="Был пропущен (исключение)"
    )
    skip_reason = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        verbose_name="Причина пропуска",
        help_text="Причина пропуска"
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Дата создания записи",
        help_text="Дата создания записи"
    )
    
    class Meta:
        db_table = 'search_results'
        verbose_name = 'Результат поиска'
        verbose_name_plural = 'Результаты поиска'
        ordering = ['history', 'position']
        indexes = [
            models.Index(fields=['history', 'position']),
            models.Index(fields=['url']),
        ]
    
    def __str__(self):
        return f"Результат #{self.id} ({self.url[:50]}...)"


class CrawledData(models.Model):
    """
    Модель данных, полученных в результате crawling (парсинга) страниц.
    Содержит сырые данные и связи с детальными таблицами.
    """
    search_result = models.OneToOneField(
        SearchResult,
        on_delete=models.CASCADE,
        related_name='crawled_data',
        verbose_name="Результат поиска",
        help_text="ID результата поиска"
    )
    url = models.TextField(
        verbose_name="URL страницы",
        help_text="URL страницы (дублируется для удобства)"
    )
    organization_name = models.CharField(
        max_length=500,
        blank=True,
        null=True,
        verbose_name="Название организации",
        help_text="Название организации"
    )
    raw_html = models.TextField(
        blank=True,
        null=True,
        verbose_name="Сырой HTML страницы",
        help_text="Сырой HTML страницы"
    )
    http_status = models.IntegerField(
        blank=True,
        null=True,
        verbose_name="HTTP статус код",
        help_text="HTTP статус код"
    )
    error_message = models.TextField(
        blank=True,
        null=True,
        verbose_name="Ошибка при загрузке",
        help_text="Ошибка при загрузке"
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Дата создания",
        help_text="Дата создания"
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        verbose_name="Дата последнего обновления",
        help_text="Дата последнего обновления"
    )
    
    class Meta:
        db_table = 'crawled_data'
        verbose_name = 'Собранные данные'
        verbose_name_plural = 'Собранные данные'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['search_result']),
        ]
    
    def __str__(self):
        return f"Данные #{self.id} ({self.url[:50]}...)"

class CrawledPhone(models.Model):
    """
    Модель найденных телефонных номеров.
    Каждая запись - отдельный телефон, связанный с crawled_data и темой поиска.
    """
    crawled_data = models.ForeignKey(
        CrawledData,
        on_delete=models.CASCADE,
        related_name='phones',
        verbose_name="Запись краулинга",
        help_text="ID записи краулинга"
    )
    topic = models.ForeignKey(
        SearchTopic,
        on_delete=models.CASCADE,
        related_name='crawled_phones',
        verbose_name="Тема поиска",
        help_text="Тема поиска, в рамках которой найден телефон",
        null=True,  # временно разрешаем NULL для существующих данных
        blank=True
    )
    phone = models.CharField(
        max_length=20,
        verbose_name="Номер телефона",
        help_text="Номер телефона в формате +7XXXXXXXXXX"
    )
    phone_raw = models.CharField(
        max_length=100,
        blank=True,
        null=True,
        verbose_name="Исходный текст с телефоном",
        help_text="Исходный текст с телефоном"
    )
    context = models.TextField(
        blank=True,
        null=True,
        verbose_name="Контекст вокруг телефона",
        help_text="Контекст вокруг телефона"
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Дата создания",
        help_text="Дата создания"
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        verbose_name="Дата последнего обновления",
        help_text="Дата последнего обновления записи"
    )
    
    class Meta:
        db_table = 'crawled_phones'
        verbose_name = 'Найденный телефон'
        verbose_name_plural = 'Найденные телефоны'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['phone']),
            models.Index(fields=['topic', 'phone']),  # индекс для быстрого поиска дублей
            models.Index(fields=['updated_at']),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['topic', 'phone'],
                name='unique_phone_per_topic'
            )
        ]
    
    def __str__(self):
        return self.phone


class CrawledEmail(models.Model):
    """
    Модель найденных email адресов.
    """
    crawled_data = models.ForeignKey(
        CrawledData,
        on_delete=models.CASCADE,
        related_name='emails',
        verbose_name="Запись краулинга",
        help_text="ID записи краулинга"
    )
    topic = models.ForeignKey(
        SearchTopic,
        on_delete=models.CASCADE,
        related_name='crawled_emails',
        verbose_name="Тема поиска",
        help_text="Тема поиска, в рамках которой найден email",
        null=True,  # временно разрешаем NULL для существующих данных
        blank=True
    )
    email = models.EmailField(
        max_length=255,
        verbose_name="Email адрес",
        help_text="Email адрес"
    )
    context = models.TextField(
        blank=True,
        null=True,
        verbose_name="Контекст вокруг email",
        help_text="Контекст вокруг email"
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Дата создания",
        help_text="Дата создания"
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        verbose_name="Дата последнего обновления",
        help_text="Дата последнего обновления записи"
    )
    
    class Meta:
        db_table = 'crawled_emails'
        verbose_name = 'Найденный email'
        verbose_name_plural = 'Найденные email'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['email']),
            models.Index(fields=['topic', 'email']),  # индекс для быстрого поиска дублей
            models.Index(fields=['updated_at']),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['topic', 'email'],
                name='unique_email_per_topic'
            )
        ]
    
    def __str__(self):
        return self.email


class CrawledAddress(models.Model):
    """
    Модель найденных адресов.
    """
    crawled_data = models.ForeignKey(
        CrawledData,
        on_delete=models.CASCADE,
        related_name='addresses',
        verbose_name="Запись краулинга",
        help_text="ID записи краулинга"
    )
    address = models.TextField(
        verbose_name="Найденный адрес",
        help_text="Найденный адрес"
    )
    address_cleaned = models.TextField(
        blank=True,
        null=True,
        verbose_name="Очищенная версия адреса",
        help_text="Очищенная версия адреса"
    )
    context = models.TextField(
        blank=True,
        null=True,
        verbose_name="Контекст вокруг адреса",
        help_text="Контекст вокруг адреса"
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Дата создания",
        help_text="Дата создания"
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        verbose_name="Дата последнего обновления",
        help_text="Дата последнего обновления записи"
    )
    
    class Meta:
        db_table = 'crawled_addresses'
        verbose_name = 'Найденный адрес'
        verbose_name_plural = 'Найденные адреса'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['updated_at']),
        ]
    
    def __str__(self):
        return self.address[:50] + "..." if len(self.address) > 50 else self.address