"""
Модуль для комплексной обработки темы поиска.
Объединяет поиск, сохранение результатов и краулинг.
Адаптировано для Django ORM.
"""

from django.utils import timezone
from django.db import transaction
from django.db.models import Q
from typing import Optional, List, Dict, Any, Tuple
import logging
from urllib.parse import urlparse

# Импортируем Django модели
from apps.core.models import (
    SearchTopic, SearchHistory, SearchResult, CrawledData,
    CrawledPhone, CrawledEmail, CrawledAddress, UrlExclusion
)

# Импортируем внешние модули 
from .yandex_search_api import search_by_topic
from .crawler import fetch_page_with_retry
from .extractor import extract_all_from_html, extract_organization_name

# Настройка логирования
logger = logging.getLogger(__name__)


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


@transaction.atomic
def process_topic(topic_id: int, 
                  crawl: bool = True, 
                  max_results: Optional[int] = None,
                  skip_excluded: bool = True,
                  filter_in_query: bool = True) -> Dict[str, Any]:
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
        "history_id": None
    }
    
    try:
        # 1. Получаем тему из БД через Django ORM
        try:
            topic = SearchTopic.objects.get(id=topic_id)
        except SearchTopic.DoesNotExist:
            raise ValueError(f"Тема с ID {topic_id} не найдена")
        
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
                
                # Извлекаем домены из паттернов для поискового запроса
                for pattern in exclusions:
                    # Очищаем паттерн от протоколов и www
                    domain = pattern.lower()
                    domain = domain.replace('http://', '')
                    domain = domain.replace('https://', '')
                    domain = domain.replace('www.', '')
                    domain = domain.split('/')[0]  # Убираем пути
                    domain = domain.strip()
                    
                    if domain:
                        exclusion_domains.append(domain)
                        logger.info(f"  - Добавлен домен в исключения: {domain}")
        
        # 2. Формируем поисковый запрос с исключениями
        search_query = topic.query_text
        
        if filter_in_query and exclusion_domains:
            # Добавляем оператор -site: для каждого исключённого домена
            exclusion_terms = [f"-site:{domain}" for domain in exclusion_domains]
            # search_query = f"{topic.query_text} {' '.join(exclusion_terms)}"
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
            
            # Сохраняем неудачную попытку в историю
            history = SearchHistory.objects.create(
                topic=topic,
                results_count=0,
                status=SearchHistory.Status.ERROR,
                error_message=error_msg
            )
            result["history_id"] = history.id
            return result

        # 4. Сохраняем результаты поиска в БД
        history = SearchHistory.objects.create(
            topic=topic,
            results_count=len(search_results),
            status=SearchHistory.Status.SUCCESS
        )
        
        saved_results = []
        skipped_count = 0

        for idx, res in enumerate(search_results):
            # Ограничиваем количество результатов, если нужно
            if max_results and len(saved_results) >= max_results:
                logger.info(f"Достигнут лимит max_results={max_results}, останавливаемся")
                break
            
            url = res.get('url', '')
            
            # Дополнительная проверка исключений (на случай, если API не отфильтровал)
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
                processed=False,
                skipped=excluded,
                skip_reason=f"Исключение: {pattern}" if excluded else None
            )
            
            if not excluded:
                saved_results.append(search_result)
            else:
                skipped_count += 1
                logger.info(f"Пропущен исключённый URL (дублирующая проверка): {url} (паттерн: {pattern})")

        # Обновляем количество результатов в истории (на случай если max_results ограничил)
        if max_results and len(saved_results) < len(search_results):
            history.results_count = len(saved_results) + skipped_count
            history.save(update_fields=['results_count'])

        result["search_results_count"] = len(saved_results)
        result["skipped_count"] = skipped_count
        result["history_id"] = history.id
        logger.info(f"Сохранено результатов для обработки: {len(saved_results)}")
        logger.info(f"Пропущено по исключениям (дублирующая проверка): {skipped_count}")
        
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
            fetch_result = fetch_page_with_retry(search_result.url)
            
            # Создаём запись в CrawledData
            crawled = CrawledData.objects.create(
                search_result=search_result,
                url=search_result.url,
                http_status=fetch_result.get("status_code"),
                error_message=fetch_result.get("error")
            )
            
            # Если успешно загрузили - сохраняем HTML и извлекаем данные
            if fetch_result.get("success"):
                # Сохраняем HTML (ограничиваем размер для производительности)
                crawled.raw_html = fetch_result.get("html")[:100000] if fetch_result.get("html") else None
                
                if fetch_result.get("html"):
                    # Извлекаем все контактные данные
                    extracted = extract_all_from_html(fetch_result["html"])
                    
                    # Пробуем найти название организации
                    try:
                        crawled.organization_name = extract_organization_name(fetch_result["html"])
                    except Exception as e:
                        logger.warning(f"  Не удалось извлечь название организации: {e}")
                    
                    crawled.save(update_fields=['raw_html', 'organization_name'])

                    # При создании телефонов нужно будет передавать topic
                    topic = search_result.history.topic
                    print('=============================')
                    print(search_result)
                    print(search_result.history)
                    print(search_result.history.topic)
                    
                    # Сохраняем телефоны
                    phones_created = 0
                    for phone in extracted.get("phones", []):
                         # Проверяем, что телефон не пустой и не дубль
                        if phone and not CrawledPhone.objects.filter(topic=topic, phone=phone).exists(): 
                            CrawledPhone.objects.create(
                                crawled_data=crawled,
                                topic=topic,
                                phone=phone,
                                phone_raw=phone
                            )
                            phones_created += 1
                    
                    # Сохраняем email'ы
                    emails_created = 0
                    for email in extracted.get("emails", []):
                        if email and not CrawledEmail.objects.filter(topic=topic, email=email).exists():
                            CrawledEmail.objects.create(
                                crawled_data=crawled,
                                topic=topic,
                                email=email
                            )
                            emails_created += 1
                    
                    # Сохраняем адреса
                    addresses_created = 0
                    for address in extracted.get("addresses", []):
                        if address:
                            CrawledAddress.objects.create(
                                crawled_data=crawled,
                                address=address,
                                address_cleaned=address  # Пока используем тот же адрес
                            )
                            addresses_created += 1
                    
                    # Помечаем результат как обработанный
                    search_result.processed = True
                    search_result.processed_at = timezone.now()
                    search_result.save(update_fields=['processed', 'processed_at'])
                    
                    logger.info(f"  Найдено: {phones_created} телефонов, "
                               f"{emails_created} email, {addresses_created} адресов")
                else:
                    logger.warning(f"  HTML пустой, пропускаем извлечение данных")
            else:
                # Если ошибка загрузки - просто сохраняем информацию об ошибке
                logger.warning(f"  Ошибка загрузки: {fetch_result.get('error')}")
            
            crawled_ids.append(crawled.id)
            
        except Exception as e:
            logger.error(f"  Ошибка при краулинге {search_result.url}: {str(e)}", exc_info=True)
            # Продолжаем со следующим URL
    
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


def cleanup_old_data(days: int = 30) -> Dict[str, int]:
    """
    Удаляет старые данные (для задач очистки).
    
    Аргументы:
        days: Удалять данные старше указанного количества дней
    
    Возвращает:
        Словарь с количеством удалённых записей
    """
    cutoff_date = timezone.now() - timezone.timedelta(days=days)
    
    # Удаляем старые истории (каскадно удалит связанные результаты и crawled_data)
    old_histories = SearchHistory.objects.filter(search_datetime__lt=cutoff_date)
    histories_count = old_histories.count()
    
    # Считаем связанные записи до удаления
    results_count = SearchResult.objects.filter(history__in=old_histories).count()
    
    # Удаляем
    old_histories.delete()
    
    return {
        'histories_deleted': histories_count,
        'results_deleted': results_count,
        'crawled_data_deleted': 'cascade',  # Каскадно через БД
    }