"""
Модуль для комплексной обработки темы поиска.
Объединяет поиск, сохранение результатов и краулинг.
Адаптировано для Django ORM.
"""


from datetime import datetime

from decouple import config

from django.utils import timezone
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import urlparse

import logging

# Импортируем Django модели
from apps.core.models import (
    SearchTopic, SearchHistory, SearchResult, CrawledData,
    CrawledPhone, CrawledPhoneHistory,
    CrawledEmail, CrawledAddress, UrlExclusion
)

# Импортируем внешние модули 
from .yandex_search_api import search_by_topic
from .crawler import fetch_page_with_retry
from .extractor import extract_all_from_html, extract_organization_name

# Настройка логирования
logger = logging.getLogger(__name__)


# Константы из .env
DATA_RETENTION_DAYS = config('DATA_RETENTION_DAYS', default=7, cast=int)


def cleanup_old_data(days: int = None) -> Dict[str, int]:
    """
    Удаляет старые данные из таблиц CrawledData и связанных с ними записей.
    
    Удаляются записи, у которых updated_at старше указанного количества дней.
    Каскадно удаляются связанные записи:
    - CrawledPhone (через crawled_data)
    - CrawledEmail (через crawled_data)
    - CrawledAddress (через crawled_data)
    
    Аргументы:
        days: Количество дней хранения (по умолчанию из .env)
    
    Возвращает:
        Словарь с количеством удалённых записей
    """
    if days is None:
        days = DATA_RETENTION_DAYS
    
    cutoff_date = timezone.now() - timezone.timedelta(days=days)
    
    logger.info(f"Очистка данных старше {days} дней (до {cutoff_date})")
    print(f"Очистка данных старше {days} дней (до {cutoff_date})")
    
    # Сначала получаем ID старых CrawledData для статистики
    old_crawled_data = CrawledData.objects.filter(created_at__lt=cutoff_date)
    old_crawled_ids = list(old_crawled_data.values_list('id', flat=True))
    
    # Подсчитываем количество связанных записей до удаления
    phones_count = CrawledPhone.objects.filter(created_at__lt=cutoff_date).count()
    emails_count = CrawledEmail.objects.filter(created_at__lt=cutoff_date).count()
    addresses_count = CrawledAddress.objects.filter(created_at__lt=cutoff_date).count()
    crawled_count = len(old_crawled_ids)
    
    logger.info(f"Найдено для удаления: {crawled_count} CrawledData, "
                f"{phones_count} телефонов, {emails_count} email, {addresses_count} адресов")

    # Удаляем связанные записи (они удалятся каскадно, но для статистики делаем явно)
    phones_deleted = CrawledPhone.objects.filter(created_at__lt=cutoff_date).delete()[0]
    emails_deleted = CrawledEmail.objects.filter(created_at__lt=cutoff_date).delete()[0]
    addresses_deleted = CrawledAddress.objects.filter(created_at__lt=cutoff_date).delete()[0]
    
    # Удаляем CrawledData
    crawled_deleted = old_crawled_data.delete()[0]
    
    result = {
        'crawled_data_deleted': crawled_deleted,
        'phones_deleted': phones_deleted,
        'emails_deleted': emails_deleted,
        'addresses_deleted': addresses_deleted,
        'total_deleted': crawled_deleted + phones_deleted + emails_deleted + addresses_deleted,
        'cutoff_date': cutoff_date,
        'retention_days': days
    }
    print(f"Очистка завершена: удалено {result['total_deleted']} записей "
                f"(CrawledData: {crawled_deleted}, телефоны: {phones_deleted}, "
                f"email: {emails_deleted}, адреса: {addresses_deleted})")
    logger.info(f"Очистка завершена: удалено {result['total_deleted']} записей "
                f"(CrawledData: {crawled_deleted}, телефоны: {phones_deleted}, "
                f"email: {emails_deleted}, адреса: {addresses_deleted})")
    
    return result


def update_or_create_phone(topic, crawled_data, phone, phone_raw=None, context=None) -> Tuple[bool, CrawledPhone]:
    """
    Создаёт или обновляет телефон, обновляя страницы выдачи и записывая историю.
    
    При создании: first_seen_page и last_seen_page = текущая страница
    При обновлении: обновляется last_seen_page, first_seen_page остаётся неизменной
    Также добавляет запись в CrawledPhoneHistory, если её ещё нет для этой даты
    
    Аргументы:
        topic: тема поиска
        crawled_data: объект CrawledData
        phone: номер телефона
        phone_raw: исходный текст (опционально)
        context: контекст (опционально)
    
    Возвращает:
        (is_new, phone_object)
    """
    page_number = None
    position = None
    search_datetime = None
    
    if crawled_data and crawled_data.search_result:
        page_number = crawled_data.search_result.page
        position = crawled_data.search_result.position
        search_datetime = crawled_data.search_result.history.search_datetime
    
    # Получаем дату без времени для поиска в истории
    search_date = None
    if search_datetime:
        search_date = search_datetime.date()
    
    # Пытаемся найти существующий телефон
    try:
        phone_obj = CrawledPhone.objects.get(topic=topic, phone=phone)
        created = False
        
        # Обновляем существующую запись
        phone_obj.crawled_data = crawled_data
        phone_obj.phone_raw = phone_raw or phone
        phone_obj.context = context
        phone_obj.last_seen_page = page_number
        # first_seen_page НЕ трогаем
        phone_obj.save(update_fields=['crawled_data', 'phone_raw', 'context', 'last_seen_page', 'updated_at'])
        
    except CrawledPhone.DoesNotExist:
        # Создаём новую запись
        phone_obj = CrawledPhone.objects.create(
            topic=topic,
            crawled_data=crawled_data,
            phone=phone,
            phone_raw=phone_raw or phone,
            context=context,
            first_seen_page=page_number,
            last_seen_page=page_number
        )
        created = True
    
    # Сохраняем историю появления телефона в выдаче
    if search_date and page_number is not None:
        try:
            # Проверяем, есть ли уже запись за эту дату
            history_obj = CrawledPhoneHistory.objects.get(
                topic=topic,
                phone=phone,
                search_date=search_date,
            )
        except CrawledPhoneHistory.DoesNotExist:
            history_obj = CrawledPhoneHistory.objects.create(
                topic=topic,
                phone=phone,
                search_date=search_date,
                page=page_number,
                position=position
            )
    
    return created, phone_obj



def update_or_create_email(topic, crawled_data, email, context=None) -> Tuple[bool, CrawledEmail]:
    """
    Создаёт или обновляет email, обновляя updated_at при наличии.
    
    Возвращает:
        (is_new, email_object) - is_new=True если создан новый, False если обновлён существующий
    """
    email_obj, created = CrawledEmail.objects.update_or_create(
        topic=topic,
        email=email,
        defaults={
            'crawled_data': crawled_data,
            'context': context,
            'updated_at': timezone.now()
        }
    )
    return created, email_obj


def update_or_create_address(topic, crawled_data, address, address_cleaned=None, context=None) -> Tuple[bool, CrawledAddress]:
    """
    Создаёт или обновляет адрес, обновляя updated_at при наличии.
    Для адресов нет уникального ограничения по теме, поэтому используем get_or_create
    """
    # Для адресов используем get_or_create, так как нет уникального ключа
    address_obj, created = CrawledAddress.objects.get_or_create(
        crawled_data=crawled_data,
        address=address,
        defaults={
            'address_cleaned': address_cleaned or address,
            'context': context
        }
    )
    
    if not created:
        # Если запись существует, обновляем updated_at и другие поля
        address_obj.address_cleaned = address_cleaned or address
        address_obj.context = context
        address_obj.save(update_fields=['address_cleaned', 'context', 'updated_at'])
    
    return created, address_obj


def is_url_excluded(url: str, exclusions: List[str]) -> Tuple[bool, Optional[str]]:
    """
    Проверяет, исключён ли URL на основе списка паттернов.
    
    Аргументы:
        url: URL для проверки
        exclusions: Список паттернов для исключения
    
    Возвращает:
        (True, pattern) если URL исключён, (False, None) если нет
    """
    url_lower = url.lower()
    
    for pattern in exclusions:
        pattern_lower = pattern.lower()
        # Проверяем вхождение паттерна в URL
        if pattern_lower in url_lower:
            return True, pattern
        
        # Также проверяем домен без www
        if '://' in url_lower:
            domain_part = url_lower.split('://', 1)[1].split('/', 1)[0]
            if domain_part.startswith('www.'):
                domain_part = domain_part[4:]
            if pattern_lower == domain_part or pattern_lower in domain_part:
                return True, pattern
    
    return False, None


def load_active_exclusions() -> List[str]:
    """
    Загружает все активные паттерны исключений из БД.
    Использует Django ORM.
    
    Возвращает:
        Список паттернов для исключения
    """
    exclusions = UrlExclusion.objects.filter(is_active=True).values_list('url_pattern', flat=True)
    return list(exclusions)


# @transaction.atomic
# def process_topic111(topic_id: int, 
#                   crawl: bool = True, 
#                   max_results: Optional[int] = None,
#                   skip_excluded: bool = True,
#                   filter_in_query: bool = True) -> Dict[str, Any]:
#     """
#     КОМПЛЕКСНАЯ ФУНКЦИЯ: обрабатывает тему поиска от начала до конца.
    
#     Что делает:
#     1. Берёт тему из БД по ID
#     2. Загружает исключения и модифицирует поисковый запрос (добавляет -site:domain)
#     3. Выполняет поисковый запрос через API Яндекса
#     4. Сохраняет результаты поиска в БД
#     5. Выполняет краулинг каждого URL (если crawl=True)
#     6. Извлекает контактные данные и сохраняет в соответствующие таблицы
    
#     Аргументы:
#         topic_id: ID темы в таблице SearchTopic
#         crawl: Флаг - выполнять ли краулинг после поиска
#         max_results: Максимальное количество результатов для обработки (None = все)
#         skip_excluded: Пропускать ли URL из списка исключений
#         filter_in_query: Добавлять ли исключения прямо в поисковый запрос
    
#     Возвращает:
#         Словарь с результатами обработки
#     """
    
#     result = {
#         "topic_id": topic_id,
#         "topic_name": None,
#         "original_query": None,
#         "modified_query": None,
#         "search_results_count": 0,
#         "skipped_count": 0,
#         "crawled_count": 0,
#         "errors": [],
#         "history_id": None
#     }
    
#     try:
#         # 1. Получаем тему из БД через Django ORM
#         try:
#             topic = SearchTopic.objects.get(id=topic_id)
#         except SearchTopic.DoesNotExist:
#             raise ValueError(f"Тема с ID {topic_id} не найдена")
        
#         result["topic_name"] = topic.name
#         result["original_query"] = topic.query_text
        
#         logger.info(f"Обработка темы: {topic.name} (ID: {topic_id})")
#         logger.info(f"Исходный запрос: '{topic.query_text}', страниц: {topic.pages_count}")

#         # Загружаем исключения
#         exclusions = []
#         exclusion_domains = []
        
#         if skip_excluded or filter_in_query:
#             exclusions = load_active_exclusions()
#             if exclusions:
#                 logger.info(f"Загружено исключений: {len(exclusions)}")
                
#                 # Извлекаем домены из паттернов для поискового запроса
#                 for pattern in exclusions:
#                     # Очищаем паттерн от протоколов и www
#                     domain = pattern.lower()
#                     domain = domain.replace('http://', '')
#                     domain = domain.replace('https://', '')
#                     domain = domain.replace('www.', '')
#                     domain = domain.split('/')[0]  # Убираем пути
#                     domain = domain.strip()
                    
#                     if domain:
#                         exclusion_domains.append(domain)
#                         logger.info(f"  - Добавлен домен в исключения: {domain}")
        
#         # 2. Формируем поисковый запрос с исключениями
#         search_query = topic.query_text
        
#         if filter_in_query and exclusion_domains:
#             # Добавляем оператор -site: для каждого исключённого домена
#             exclusion_terms = [f"-site:{domain}" for domain in exclusion_domains]
#             # search_query = f"{topic.query_text} {' '.join(exclusion_terms)}"
#             search_query = f"{topic.query_text}"
#             logger.info(f"Модифицированный запрос: '{search_query}'")
        
#         result["modified_query"] = search_query

#         # 3. Выполняем поиск через API
#         try:
#             search_results = search_by_topic(search_query, pages=topic.pages_count)
#             logger.info(f"Получено результатов поиска: {len(search_results)}")
#         except Exception as e:
#             error_msg = f"Ошибка при поиске: {str(e)}"
#             logger.error(error_msg)
#             result["errors"].append(error_msg)
            
#             # Сохраняем неудачную попытку в историю
#             history = SearchHistory.objects.create(
#                 topic=topic,
#                 results_count=0,
#                 status=SearchHistory.Status.ERROR,
#                 error_message=error_msg
#             )
#             result["history_id"] = history.id
#             return result

#         # 4. Сохраняем результаты поиска в БД
#         if search_results:
#             page_size = search_results[0]['page_size']

#         history = SearchHistory.objects.create(
#             topic=topic,
#             results_count=len(search_results),
#             page_size=page_size,
#             status=SearchHistory.Status.SUCCESS
#         )
        
#         saved_results = []
#         skipped_count = 0

#         for idx, res in enumerate(search_results):
#             # Ограничиваем количество результатов, если нужно
#             if max_results and len(saved_results) >= max_results:
#                 logger.info(f"Достигнут лимит max_results={max_results}, останавливаемся")
#                 break
            
#             url = res.get('url', '')
            
#             # Дополнительная проверка исключений (на случай, если API не отфильтровал)
#             excluded, pattern = False, None
#             if skip_excluded:
#                 excluded, pattern = is_url_excluded(url, exclusions)
            
#             search_result = SearchResult.objects.create(
#                 history=history,
#                 title=res.get('title'),
#                 url=url,
#                 domain=res.get('domain'),
#                 snippet=res.get('snippet'),
#                 position=res.get('position'),
#                 page=res.get('page'),
#                 processed=False,
#                 skipped=excluded,
#                 skip_reason=f"Исключение: {pattern}" if excluded else None
#             )
            
#             if not excluded:
#                 saved_results.append(search_result)
#             else:
#                 skipped_count += 1
#                 logger.info(f"Пропущен исключённый URL (дублирующая проверка): {url} (паттерн: {pattern})")

#         # Обновляем количество результатов в истории (на случай если max_results ограничил)
#         if max_results and len(saved_results) < len(search_results):
#             history.results_count = len(saved_results) + skipped_count
#             history.save(update_fields=['results_count'])

#         result["search_results_count"] = len(saved_results)
#         result["skipped_count"] = skipped_count
#         result["history_id"] = history.id
#         logger.info(f"Сохранено результатов для обработки: {len(saved_results)}")
#         logger.info(f"Пропущено по исключениям (дублирующая проверка): {skipped_count}")
        
#         # 5. Если нужно - выполняем краулинг
#         if crawl and saved_results:
#             crawled = crawl_search_results(saved_results)
#             result["crawled_count"] = len(crawled)
#             logger.info(f"Успешно обработано краулингом: {len(crawled)}")
        
#         return result
        
#     except Exception as e:
#         error_msg = f"Неожиданная ошибка: {str(e)}"
#         logger.error(error_msg, exc_info=True)
#         result["errors"].append(error_msg)
#         return result


@transaction.atomic
def process_topic(topic_id: int, 
                  crawl: bool = True, 
                  max_results: Optional[int] = None,
                  skip_excluded: bool = True,
                  filter_in_query: bool = True,
                  cleanup_before: bool = True) -> Dict[str, Any]:
    """
    КОМПЛЕКСНАЯ ФУНКЦИЯ: обрабатывает тему поиска от начала до конца.
    
    Что делает:
    1. Берёт тему из БД по ID
    2. Загружает исключения и модифицирует поисковый запрос (добавляет -site:domain)
    3. Выполняет поисковый запрос через API Яндекса
    4. Сохраняет результаты поиска в БД
    5. Выполняет краулинг каждого URL (если crawl=True)
    6. Извлекает контактные данные и сохраняет в соответствующие таблицы
    
    Аргументы:
        topic_id: ID темы в таблице SearchTopic
        crawl: Флаг - выполнять ли краулинг после поиска
        max_results: Максимальное количество результатов для обработки (None = все)
        skip_excluded: Пропускать ли URL из списка исключений
        filter_in_query: Добавлять ли исключения прямо в поисковый запрос
    
    Возвращает:
        Словарь с результатами обработки
    """
    
    result = {
        "topic_id": topic_id,
        "topic_name": None,
        "original_query": None,
        "modified_query": None,
        "search_results_count": 0,
        "skipped_count": 0,
        "crawled_count": 0,
        "errors": [],
        "history_id": None,
        "cleanup_result": None  # Добавляем информацию об очистке
    }
    
    try:
        # 0. Очистка старых данных (если нужно)
        if cleanup_before:
            logger.info("Выполняется очистка старых данных перед обработкой...")
            print("Выполняется очистка старых данных перед обработкой...")
            cleanup_result = cleanup_old_data()
            result["cleanup_result"] = cleanup_result
            logger.info(f"Очистка завершена: удалено {cleanup_result['total_deleted']} записей")
            print(f"Очистка завершена: удалено {cleanup_result['total_deleted']} записей")
        
        # 1. Получаем тему из БД через Django ORM
        try:
            topic = SearchTopic.objects.get(id=topic_id)
        except SearchTopic.DoesNotExist:
            raise ValueError(f"Тема с ID {topic_id} не найдена")
        
        # ... остальной код process_topic остается без изменений ...
        result["topic_name"] = topic.name
        result["original_query"] = topic.query_text
        
        logger.info(f"Обработка темы: {topic.name} (ID: {topic_id})")
        logger.info(f"Исходный запрос: '{topic.query_text}', страниц: {topic.pages_count}")

        # Загружаем исключения
        exclusions = []
        exclusion_domains = []
        
        if skip_excluded or filter_in_query:
            exclusions = load_active_exclusions()
            if exclusions:
                logger.info(f"Загружено исключений: {len(exclusions)}")
                
                for pattern in exclusions:
                    domain = pattern.lower()
                    domain = domain.replace('http://', '')
                    domain = domain.replace('https://', '')
                    domain = domain.replace('www.', '')
                    domain = domain.split('/')[0]
                    domain = domain.strip()
                    
                    if domain:
                        exclusion_domains.append(domain)
                        logger.info(f"  - Добавлен домен в исключения: {domain}")
        
        # 2. Формируем поисковый запрос с исключениями
        search_query = topic.query_text
        
        if filter_in_query and exclusion_domains:
            exclusion_terms = [f"-site:{domain}" for domain in exclusion_domains]
            search_query = f"{topic.query_text}"
            logger.info(f"Модифицированный запрос: '{search_query}'")
        
        result["modified_query"] = search_query

        # 3. Выполняем поиск через API
        try:
            search_results = search_by_topic(search_query, pages=topic.pages_count)
            logger.info(f"Получено результатов поиска: {len(search_results)}")
        except Exception as e:
            error_msg = f"Ошибка при поиске: {str(e)}"
            logger.error(error_msg)
            result["errors"].append(error_msg)
            
            history = SearchHistory.objects.create(
                topic=topic,
                results_count=0,
                status=SearchHistory.Status.ERROR,
                error_message=error_msg
            )
            result["history_id"] = history.id
            return result

        # 4. Сохраняем результаты поиска в БД
        if search_results:
            page_size = search_results[0]['page_size']

        history = SearchHistory.objects.create(
            topic=topic,
            results_count=len(search_results),
            page_size=page_size,
            status=SearchHistory.Status.SUCCESS
        )
        
        saved_results = []
        skipped_count = 0

        for idx, res in enumerate(search_results):
            if max_results and len(saved_results) >= max_results:
                logger.info(f"Достигнут лимит max_results={max_results}, останавливаемся")
                break
            
            url = res.get('url', '')
            
            excluded, pattern = False, None
            if skip_excluded:
                excluded, pattern = is_url_excluded(url, exclusions)
            
            search_result = SearchResult.objects.create(
                history=history,
                title=res.get('title'),
                url=url,
                domain=res.get('domain'),
                snippet=res.get('snippet'),
                position=res.get('position'),
                page=res.get('page'),
                processed=False,
                skipped=excluded,
                skip_reason=f"Исключение: {pattern}" if excluded else None
            )
            
            if not excluded:
                saved_results.append(search_result)
            else:
                skipped_count += 1
                logger.info(f"Пропущен исключённый URL: {url} (паттерн: {pattern})")

        if max_results and len(saved_results) < len(search_results):
            history.results_count = len(saved_results) + skipped_count
            history.save(update_fields=['results_count'])

        result["search_results_count"] = len(saved_results)
        result["skipped_count"] = skipped_count
        result["history_id"] = history.id
        logger.info(f"Сохранено результатов для обработки: {len(saved_results)}")
        logger.info(f"Пропущено по исключениям: {skipped_count}")
        
        # 5. Если нужно - выполняем краулинг
        if crawl and saved_results:
            crawled = crawl_search_results(saved_results)
            result["crawled_count"] = len(crawled)
            logger.info(f"Успешно обработано краулингом: {len(crawled)}")
        
        return result
        
    except Exception as e:
        error_msg = f"Неожиданная ошибка: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result["errors"].append(error_msg)
        return result


# @transaction.atomic
# def crawl_search_results111(search_results: List[SearchResult]) -> List[int]:
#     """
#     Выполняет краулинг для списка результатов поиска.
#     Сохраняет данные в CrawledData и связанные таблицы.
    
#     Аргументы:
#         search_results: Список объектов SearchResult для обработки
    
#     Возвращает:
#         Список ID созданных объектов CrawledData
#     """
#     crawled_ids = []
    
#     for search_result in search_results:
#         logger.info(f"Краулинг: {search_result.url}")
        
#         try:
#             # Проверяем, не краулили ли уже этот URL
#             if hasattr(search_result, 'crawled_data'):
#                 logger.info(f"  URL уже был обработан, пропускаем")
#                 continue
            
#             # Загружаем страницу
#             fetch_result = fetch_page_with_retry(search_result.url)
            
#             # Создаём запись в CrawledData
#             crawled = CrawledData.objects.create(
#                 search_result=search_result,
#                 url=search_result.url,
#                 http_status=fetch_result.get("status_code"),
#                 error_message=fetch_result.get("error")
#             )
            
#             # Если успешно загрузили - сохраняем HTML и извлекаем данные
#             if fetch_result.get("success"):
#                 # Сохраняем HTML (ограничиваем размер для производительности)
#                 crawled.raw_html = fetch_result.get("html")[:100000] if fetch_result.get("html") else None
                
#                 if fetch_result.get("html"):
#                     # Извлекаем все контактные данные
#                     extracted = extract_all_from_html(fetch_result["html"])
                    
#                     # Пробуем найти название организации
#                     try:
#                         crawled.organization_name = extract_organization_name(fetch_result["html"])
#                     except Exception as e:
#                         logger.warning(f"  Не удалось извлечь название организации: {e}")
                    
#                     crawled.save(update_fields=['raw_html', 'organization_name'])

#                     # При создании телефонов нужно будет передавать topic
#                     topic = search_result.history.topic
                    
#                     # Сохраняем телефоны
#                     phones_created = 0
#                     for phone in extracted.get("phones", []):
#                          # Проверяем, что телефон не пустой и не дубль
#                         if phone and not CrawledPhone.objects.filter(topic=topic, phone=phone).exists(): 
#                             print(phone)
#                             CrawledPhone.objects.create(
#                                 crawled_data=crawled,
#                                 topic=topic,
#                                 phone=phone,
#                                 phone_raw=phone
#                             )
#                             phones_created += 1
                    
#                     # Сохраняем email'ы
#                     emails_created = 0
#                     for email in extracted.get("emails", []):
#                         if email and not CrawledEmail.objects.filter(topic=topic, email=email).exists():
#                             CrawledEmail.objects.create(
#                                 crawled_data=crawled,
#                                 topic=topic,
#                                 email=email
#                             )
#                             emails_created += 1
                    
#                     # Сохраняем адреса
#                     addresses_created = 0
#                     for address in extracted.get("addresses", []):
#                         if address:
#                             CrawledAddress.objects.create(
#                                 crawled_data=crawled,
#                                 address=address,
#                                 address_cleaned=address  # Пока используем тот же адрес
#                             )
#                             addresses_created += 1
                    
#                     # Помечаем результат как обработанный
#                     search_result.processed = True
#                     search_result.processed_at = timezone.now()
#                     search_result.save(update_fields=['processed', 'processed_at'])
                    
#                     logger.info(f"  Найдено: {phones_created} телефонов, "
#                                f"{emails_created} email, {addresses_created} адресов")
#                 else:
#                     logger.warning(f"  HTML пустой, пропускаем извлечение данных")
#             else:
#                 # Если ошибка загрузки - просто сохраняем информацию об ошибке
#                 logger.warning(f"  Ошибка загрузки: {fetch_result.get('error')}")
            
#             crawled_ids.append(crawled.id)
            
#         except Exception as e:
#             logger.error(f"  Ошибка при краулинге {search_result.url}: {str(e)}", exc_info=True)
#             # Продолжаем со следующим URL
    
#     return crawled_ids


@transaction.atomic
def crawl_search_results(search_results: List[SearchResult]) -> List[int]:
    """
    Выполняет краулинг для списка результатов поиска.
    Сохраняет данные в CrawledData и связанные таблицы.
    
    Аргументы:
        search_results: Список объектов SearchResult для обработки
    
    Возвращает:
        Список ID созданных объектов CrawledData
    """
    crawled_ids = []
    
    for search_result in search_results:
        logger.info(f"Краулинг: {search_result.url}")
        
        try:
            # Проверяем, не краулили ли уже этот URL
            if hasattr(search_result, 'crawled_data'):
                logger.info(f"  URL уже был обработан, пропускаем")
                continue
            
            # Загружаем страницу
            fetch_result = fetch_page_with_retry(url=search_result.url, max_retries=0)

            # Создаём запись в CrawledData
            crawled = CrawledData.objects.create(
                search_result=search_result,
                url=search_result.url,
                http_status=fetch_result.get("status_code"),
                error_message=fetch_result.get("error")
            )
            
            # Если успешно загрузили - сохраняем HTML и извлекаем данные
            if fetch_result.get("success"):
                crawled.raw_html = fetch_result.get("html")[:100000] if fetch_result.get("html") else None
                
                if fetch_result.get("html"):
                    extracted = extract_all_from_html(fetch_result["html"])
                    
                    try:
                        crawled.organization_name = extract_organization_name(fetch_result["html"])
                    except Exception as e:
                        logger.warning(f"  Не удалось извлечь название организации: {e}")
                    
                    crawled.save(update_fields=['raw_html', 'organization_name'])

                    topic = search_result.history.topic
                    
                    # Сохраняем телефоны с обновлением updated_at
                    phones_created = 0
                    phones_updated = 0
                    for phone in extracted.get("phones", []):
                        if phone:
                            is_new, _ = update_or_create_phone(
                                topic=topic,
                                crawled_data=crawled,
                                phone=phone,
                                phone_raw=phone
                            )
                            if is_new:
                                phones_created += 1
                            else:
                                phones_updated += 1
                    
                    # Сохраняем email'ы с обновлением updated_at
                    emails_created = 0
                    emails_updated = 0
                    for email in extracted.get("emails", []):
                        if email:
                            is_new, _ = update_or_create_email(
                                topic=topic,
                                crawled_data=crawled,
                                email=email
                            )
                            if is_new:
                                emails_created += 1
                            else:
                                emails_updated += 1
                    
                    # Сохраняем адреса
                    addresses_created = 0
                    for address in extracted.get("addresses", []):
                        if address:
                            is_new, _ = update_or_create_address(
                                topic=topic,
                                crawled_data=crawled,
                                address=address,
                                address_cleaned=address
                            )
                            if is_new:
                                addresses_created += 1
                    
                    # Помечаем результат как обработанный
                    search_result.processed = True
                    search_result.processed_at = timezone.now()
                    search_result.save(update_fields=['processed', 'processed_at'])
                    
                    logger.info(f"  Найдено: {phones_created} новых телефонов, "
                               f"{phones_updated} обновлено; "
                               f"{emails_created} новых email, {emails_updated} обновлено; "
                               f"{addresses_created} адресов")
                else:
                    logger.warning(f"  HTML пустой, пропускаем извлечение данных")
            else:
                logger.warning(f"  Ошибка загрузки: {fetch_result.get('error')}")
            
            crawled_ids.append(crawled.id)
            
        except Exception as e:
            logger.error(f"  Ошибка при краулинге {search_result.url}: {str(e)}", exc_info=True)
    
    return crawled_ids


def get_topic_statistics(topic_id: int) -> Dict[str, Any]:
    """
    Получает статистику по теме для отчётов.
    
    Аргументы:
        topic_id: ID темы
    
    Возвращает:
        Словарь со статистикой
    """
    try:
        topic = SearchTopic.objects.get(id=topic_id)
        
        # Агрегация данных через Django ORM
        total_searches = topic.search_histories.count()
        successful_searches = topic.search_histories.filter(status=SearchHistory.Status.SUCCESS).count()
        failed_searches = topic.search_histories.filter(status=SearchHistory.Status.ERROR).count()
        
        # Общее количество результатов
        total_results = SearchResult.objects.filter(history__topic=topic).count()
        processed_results = SearchResult.objects.filter(
            history__topic=topic, 
            processed=True
        ).count()
        
        # Количество найденных контактов
        total_phones = CrawledPhone.objects.filter(
            crawled_data__search_result__history__topic=topic
        ).count()
        
        total_emails = CrawledEmail.objects.filter(
            crawled_data__search_result__history__topic=topic
        ).count()
        
        total_addresses = CrawledAddress.objects.filter(
            crawled_data__search_result__history__topic=topic
        ).count()
        
        # Последний поиск
        last_search = topic.search_histories.order_by('-search_datetime').first()
        
        return {
            'topic': topic,
            'total_searches': total_searches,
            'successful_searches': successful_searches,
            'failed_searches': failed_searches,
            'total_results': total_results,
            'processed_results': processed_results,
            'processing_percentage': (processed_results / total_results * 100) if total_results > 0 else 0,
            'total_phones': total_phones,
            'total_emails': total_emails,
            'total_addresses': total_addresses,
            'last_search_date': last_search.search_datetime if last_search else None,
            'last_search_status': last_search.status if last_search else None,
        }
    except SearchTopic.DoesNotExist:
        return {'error': f'Тема с ID {topic_id} не найдена'}
