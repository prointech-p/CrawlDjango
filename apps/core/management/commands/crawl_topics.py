# -*- coding: utf-8 -*-

"""
Management command для периодического запуска краулинга всех активных тем.
Можно запускать через cron для автоматической обработки.
"""

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from django.db import transaction
from django.db.models import Q, Count
from django.conf import settings
from datetime import timedelta
import logging
import time
import sys
from typing import Dict, Any, List, Optional

# Импортируем модели и функции обработки
from apps.core.models import SearchTopic, SearchHistory, SearchResult
from service.processor import process_topic, crawl_search_results, cleanup_old_data

# Настройка логирования
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Запускает краулинг для всех активных тем поиска'
    
    def add_arguments(self, parser):
        """
        Добавляет аргументы командной строки для гибкой настройки.
        """
        parser.add_argument(
            '--topic-id',
            type=int,
            help='ID конкретной темы для обработки (если нужно обработать только одну)'
        )
        
        parser.add_argument(
            '--no-crawl',
            action='store_true',
            help='Только поиск, без краулинга'
        )
        
        parser.add_argument(
            '--max-results',
            type=int,
            default=None,
            help='Максимальное количество результатов на тему'
        )
        
        parser.add_argument(
            '--limit',
            type=int,
            default=None,
            help='Максимальное количество тем для обработки за один запуск'
        )
        
        parser.add_argument(
            '--delay',
            type=float,
            default=2.0,
            help='Задержка между темами в секундах (для соблюдения лимитов API)'
        )
        
        parser.add_argument(
            '--skip-older-than',
            type=int,
            default=None,
            help='Пропускать темы, которые обрабатывались менее N часов назад'
        )
        
        parser.add_argument(
            '--only-without-results',
            action='store_true',
            help='Обрабатывать только темы, у которых нет результатов за последние 24 часа'
        )
        
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод'
        )
        
        parser.add_argument(
            '--email-report',
            action='store_true',
            help='Отправить отчёт на email после завершения'
        )

        parser.add_argument(
            '--skip-cleanup',
            action='store_true',
            help='Пропустить очистку старых данных перед обработкой'
        )

        parser.add_argument(
            '--cleanup-only',
            action='store_true',
            help='Только очистка старых данных, без обработки тем'
        )
            
    def handle(self, *args, **options):
        """
        Основной метод обработки команды.
        """
        start_time = time.time()
        
        # Настройка уровня логирования
        if options['verbose']:
            logging.getLogger().setLevel(logging.DEBUG)
            self.stdout.write("Режим подробного логирования включен")

        # Если указан cleanup-only - только очистка
        if options['cleanup_only']:
            self.stdout.write(self.style.WARNING('🧹 Режим только очистки...'))
            cleanup_result = cleanup_old_data()
            self.stdout.write(self.style.SUCCESS(
                f"✅ Очистка завершена: удалено {cleanup_result['total_deleted']} записей "
                f"(телефоны: {cleanup_result['phones_deleted']}, "
                f"email: {cleanup_result['emails_deleted']}, "
                f"адреса: {cleanup_result['addresses_deleted']})"
            ))
            return
        
        self.stdout.write(self.style.SUCCESS('🚀 Запуск краулинга тем...'))
        
        try:
            # Получаем темы для обработки
            topics = self.get_topics_to_process(options)
            
            if not topics:
                self.stdout.write(self.style.WARNING('Нет тем для обработки'))
                return
            
            total_topics = len(topics)
            self.stdout.write(f"Найдено тем для обработки: {total_topics}")
            
            # Счетчики для статистики
            stats = {
                'total_topics': total_topics,
                'processed_topics': 0,
                'failed_topics': 0,
                'skipped_topics': 0,
                'total_results': 0,
                'total_crawled': 0,
                'errors': [],
                'topic_results': []
            }
            
            # Обрабатываем каждую тему
            for idx, topic in enumerate(topics, 1):
                try:
                    self.stdout.write(f"\n[{idx}/{total_topics}] Обработка темы: {topic.name} (ID: {topic.id})")
                    
                    # Обрабатываем тему
                    result = self.process_single_topic(topic, options)
                    
                    # Собираем статистику
                    stats['processed_topics'] += 1
                    stats['total_results'] += result.get('search_results_count', 0)
                    stats['total_crawled'] += result.get('crawled_count', 0)
                    stats['topic_results'].append({
                        'topic_id': topic.id,
                        'topic_name': topic.name,
                        'results': result.get('search_results_count', 0),
                        'crawled': result.get('crawled_count', 0),
                        'skipped': result.get('skipped_count', 0),
                        'history_id': result.get('history_id'),
                        'errors': result.get('errors', [])
                    })
                    
                    # Выводим результат
                    self.print_topic_result(result, options)
                    
                    # Задержка между темами
                    if idx < total_topics and options['delay'] > 0:
                        self.stdout.write(f"Ожидание {options['delay']} сек...")
                        time.sleep(options['delay'])
                    
                except Exception as e:
                    stats['failed_topics'] += 1
                    error_msg = f"Ошибка при обработке темы {topic.name} (ID: {topic.id}): {str(e)}"
                    stats['errors'].append(error_msg)
                    logger.error(error_msg, exc_info=True)
                    self.stdout.write(self.style.ERROR(f"❌ {error_msg}"))
            
            # Выводим итоговую статистику
            self.print_final_stats(stats, start_time, options)
            
            # Отправляем email отчёт если нужно
            if options['email_report']:
                self.send_email_report(stats, start_time)
            
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('\n⚠️ Прервано пользователем'))
            sys.exit(1)
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}", exc_info=True)
            raise CommandError(f"Ошибка выполнения команды: {e}")
    
    def get_topics_to_process(self, options: Dict[str, Any]):
        """
        Получает список тем для обработки на основе параметров.
        """
        # Если указан конкретный ID
        if options['topic_id']:
            try:
                return [SearchTopic.objects.get(id=options['topic_id'])]
            except SearchTopic.DoesNotExist:
                raise CommandError(f"Тема с ID {options['topic_id']} не найдена")
        
        # Базовый запрос - все темы (можно добавить поле is_active если нужно)
        queryset = SearchTopic.objects.all()
        
        # Фильтр по свежести обработки
        if options['skip_older_than']:
            hours = options['skip_older_than']
            cutoff_time = timezone.now() - timedelta(hours=hours)
            
            # Ищем темы, которые не обрабатывались указанное время
            recently_processed = SearchHistory.objects.filter(
                search_datetime__gte=cutoff_time,
                status=SearchHistory.Status.SUCCESS
            ).values_list('topic_id', flat=True).distinct()
            
            queryset = queryset.exclude(id__in=recently_processed)
            self.stdout.write(f"Исключены темы, обработанные менее {hours} часов назад")
        
        # Фильтр по отсутствию результатов
        if options['only_without_results']:
            yesterday = timezone.now() - timedelta(days=1)
            
            # Темы без успешных результатов за последние 24 часа
            topics_with_results = SearchHistory.objects.filter(
                search_datetime__gte=yesterday,
                status=SearchHistory.Status.SUCCESS,
                results_count__gt=0
            ).values_list('topic_id', flat=True).distinct()
            
            queryset = queryset.exclude(id__in=topics_with_results)
            self.stdout.write("Оставлены только темы без результатов за последние 24 часа")
        
        # Ограничение количества
        if options['limit']:
            queryset = queryset[:options['limit']]
        
        return list(queryset)
    
    def process_single_topic(self, topic: SearchTopic, options: Dict[str, Any]) -> Dict[str, Any]:
        """
        Обрабатывает одну тему.
        """
        self.stdout.write(f"  Запрос: '{topic.query_text}'")
        self.stdout.write(f"  Страниц: {topic.pages_count}")
        
        # Запускаем обработку
        result = process_topic(
            topic_id=topic.id,
            crawl=not options['no_crawl'],
            max_results=options['max_results']
        )
        
        return result
    
    def print_topic_result(self, result: Dict[str, Any], options: Dict[str, Any]):
        """
        Выводит результат обработки одной темы.
        """
        if result['errors']:
            self.stdout.write(self.style.ERROR(f"  ❌ Ошибки: {', '.join(result['errors'])}"))
        
        self.stdout.write(f"  📊 Результаты поиска: {result['search_results_count']}")
        
        if result['skipped_count'] > 0:
            self.stdout.write(f"  ⏭️  Пропущено (исключения): {result['skipped_count']}")
        
        if not options['no_crawl']:
            if result['crawled_count'] > 0:
                self.stdout.write(self.style.SUCCESS(f"  ✅ Обработано краулингом: {result['crawled_count']}"))
            else:
                self.stdout.write("  ⏳ Нет новых данных для краулинга")
        
        if options['verbose'] and result.get('history_id'):
            self.stdout.write(f"  🆔 ID истории: {result['history_id']}")
    
    def print_final_stats(self, stats: Dict[str, Any], start_time: float, options: Dict[str, Any]):
        """
        Выводит итоговую статистику.
        """
        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)
        
        self.stdout.write("\n" + "="*60)
        self.stdout.write(self.style.SUCCESS("📊 ИТОГОВАЯ СТАТИСТИКА"))
        self.stdout.write("="*60)
        
        self.stdout.write(f"⏱️  Время выполнения: {minutes} мин {seconds} сек")
        self.stdout.write(f"📋 Всего тем: {stats['total_topics']}")
        self.stdout.write(f"✅ Обработано успешно: {stats['processed_topics']}")
        
        if stats['failed_topics'] > 0:
            self.stdout.write(self.style.ERROR(f"❌ Ошибок: {stats['failed_topics']}"))
        
        if stats['skipped_topics'] > 0:
            self.stdout.write(f"⏭️  Пропущено: {stats['skipped_topics']}")
        
        self.stdout.write(f"🔍 Всего результатов поиска: {stats['total_results']}")
        
        if not options['no_crawl']:
            self.stdout.write(self.style.SUCCESS(f"🕷️  Всего обработано краулингом: {stats['total_crawled']}"))
        
        if stats['errors']:
            self.stdout.write("\n" + self.style.ERROR("❌ ОШИБКИ:"))
            for error in stats['errors']:
                self.stdout.write(f"  • {error}")
        
        # Детали по каждой теме (в подробном режиме)
        if options['verbose'] and stats['topic_results']:
            self.stdout.write("\n" + self.style.SUCCESS("📋 ДЕТАЛИ ПО ТЕМАМ:"))
            for topic in stats['topic_results']:
                status = "✅" if not topic['errors'] else "❌"
                self.stdout.write(f"  {status} {topic['topic_name']}: "
                                 f"{topic['results']} результатов, "
                                 f"{topic['crawled']} обработано")
        
        self.stdout.write("="*60)
    
    def send_email_report(self, stats: Dict[str, Any], start_time: float):
        """
        Отправляет отчёт на email.
        """
        try:
            from django.core.mail import send_mail
            
            elapsed_time = time.time() - start_time
            minutes = int(elapsed_time // 60)
            
            subject = f"Отчёт о краулинге: {stats['processed_topics']} тем обработано"
            
            message = f"""
Отчёт о выполнении краулинга
================================

Время выполнения: {minutes} минут
Всего тем: {stats['total_topics']}
Успешно: {stats['processed_topics']}
Ошибок: {stats['failed_topics']}

Результаты поиска: {stats['total_results']}
Обработано краулингом: {stats['total_crawled']}

Детали по темам:
{chr(10).join([f"• {t['topic_name']}: {t['results']} результатов, {t['crawled']} обработано" for t in stats['topic_results']])}

Ошибки:
{chr(10).join(stats['errors']) if stats['errors'] else 'Ошибок нет'}
"""
            
            send_mail(
                subject,
                message,
                settings.DEFAULT_FROM_EMAIL,
                [admin[1] for admin in settings.ADMINS],
                fail_silently=True,
            )
            
            self.stdout.write(self.style.SUCCESS("📧 Отчёт отправлен на email"))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Ошибка отправки email: {e}"))