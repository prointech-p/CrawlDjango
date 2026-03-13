"""
Модуль для работы с Yandex Search API.
Получение поисковой выдачи в формате XML через API Cloud.
"""

import base64
import os
import requests
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional
from decouple import config
from pprint import pprint


# Получение ключей API из переменных окружения
YANDEX_API_KEY = config('YANDEX_API_KEY', default='WRONG_YANDEX_API_KEY')
YANDEX_FOLDER_ID = config('YANDEX_CATALOG_ID', default='WRONG_YANDEX_CATALOG_ID')


def fetch_search_page(query: str, page: int = 0) -> List[Dict[str, str]]:
    """
    Получение страницы поисковой выдачи Яндекса через API Cloud.
    
    Аргументы:
        query: Поисковый запрос
        page: Номер страницы (0 - первая страница, 1 - вторая и т.д.)
    
    Возвращает:
        Список словарей с результатами поиска (title, url, domain, snippet)
    
    Примечание:
        API возвращает данные в base64-encoded XML, который затем декодируется и парсится.
        Одна страница содержит до 10 результатов.
    """
    # Формируем URL для запроса к Yandex Search API
    url = "https://searchapi.api.cloud.yandex.net/v2/web/search"
    
    # Формируем тело запроса согласно документации API
    # offset = page * 10 - для пагинации (на странице 10 результатов)
    query_text = query

    payload = {
        "query": {
            "searchType": "SEARCH_TYPE_RU",         # Поиск по русскоязычным сайтам
            "queryText": query_text,                # сюда можно добавить исключения
            "page": str(page),                      # Смещение для пагинации
            "familyMode": "FAMILY_MODE_MODERATE",   # по желанию
            "fixTypoMode": "FIX_TYPO_MODE_ON"
        },
        "groupSpec": {
            "groupMode": "GROUP_MODE_FLAT",
            "groupsOnPage": "50",               # до 100
            "docsInGroup": "1"
        },
        "maxPassages": "3",                     # более длинные сниппеты
        "folderId": YANDEX_FOLDER_ID,           # ID каталога в Yandex Cloud
        "responseFormat": "FORMAT_XML"          # явно, хотя и default
    }
    
    # payload = {
    #     "query": {
    #         "searchType": "SEARCH_TYPE_RU",  # Поиск по русскоязычным сайтам
    #         "queryText": query               # Текст поискового запроса
    #     },
    #     "folderId": YANDEX_FOLDER_ID,        # ID каталога в Yandex Cloud
    #     "page": {
    #         "offset": page * 10               # Смещение для пагинации
    #     }
    # }
    
    # Для отладки выводим отправляемые данные
    print(f"Отправка запроса для страницы {page + 1}:")
    pprint(payload)
    
    # Заголовки запроса (авторизация через API-ключ)
    headers = {
        "Authorization": f"Api-Key {YANDEX_API_KEY}"
    }
    
    try:
        # Выполняем POST-запрос к API
        r = requests.post(url, json=payload, headers=headers)
        r.raise_for_status()  # Проверяем на ошибки HTTP
        
        # Получаем ответ в формате JSON
        data = r.json()
        
        # Проверяем наличие rawData в ответе
        if "rawData" not in data:
            print(f"Ошибка: в ответе отсутствует rawData. Полный ответ:")
            pprint(data)
            return []
        
        # API возвращает XML в base64-кодировке
        raw_data = data["rawData"]
        xml_bytes = base64.b64decode(raw_data)  # Декодируем base64
        xml_string = xml_bytes.decode("utf-8")   # Преобразуем в строку UTF-8
        
        # Парсим XML и возвращаем результаты
        return parse_xml(xml_string)
        
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при выполнении запроса к API: {e}")
        return []
    except KeyError as e:
        print(f"Ошибка при обработке ответа API: отсутствует ключ {e}")
        return []
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")
        return []


def parse_xml(xml_string: str) -> List[Dict[str, str]]:
    """
    Парсинг XML-ответа от Yandex Search API.
    
    Аргументы:
        xml_string: XML строка с результатами поиска
    
    Возвращает:
        Список словарей, каждый словарь содержит:
        - title: заголовок результата
        - url: URL страницы
        - domain: домен
        - snippet: сниппет (текстовое описание)
    """
    results = []
    
    try:
        # Парсим XML строку
        root = ET.fromstring(xml_string)
        
        # Ищем все группы результатов (каждая группа содержит один результат)
        # group - это элемент в XML, содержащий информацию о найденной странице
        for group in root.findall(".//group"):
            
            # В каждой группе ищем элемент doc с данными
            doc = group.find("doc")
            if doc is None:
                continue  # Пропускаем группу без doc
            
            # Извлекаем заголовок (если есть)
            title = doc.findtext("title", "") or ""
            
            # Извлекаем URL (обязательный элемент)
            url = doc.findtext("url", "") or ""
            
            # Извлекаем домен (если есть)
            domain = doc.findtext("domain", "") or ""
            
            # Обработка сниппета (краткого описания)
            snippet = ""
            passages = doc.find("passages")
            
            if passages is not None:
                # Собираем все текстовые фрагменты из passages
                texts = []
                for p in passages.findall("passage"):
                    if p.text:
                        texts.append(p.text)
                
                # Объединяем фрагменты в один текст
                snippet = " ".join(texts)
            
            # Добавляем результат в список
            results.append({
                "title": title.strip(),
                "url": url.strip(),
                "domain": domain.strip(),
                "snippet": snippet.strip()
            })
            
    except ET.ParseError as e:
        print(f"Ошибка парсинга XML: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка при парсинге XML: {e}")
    
    return results


def search_by_topic(query: str, pages: int = 1) -> List[Dict[str, Any]]:
    """
    Выполнение поиска по теме с получением нескольких страниц.
    
    Аргументы:
        query: Поисковый запрос
        pages: Количество страниц для получения (по 10 результатов на странице)
    
    Возвращает:
        Список всех результатов со страницы, страницы и позиции
    """
    all_results = []
    
    for page in range(pages):
        print(f"\n--- Получение страницы {page + 1} ---")
        
        # Получаем результаты для текущей страницы
        page_results = fetch_search_page(query, page)
        
        # Добавляем информацию о позиции (1-10, 11-20 и т.д.)
        for idx, result in enumerate(page_results):
            result['position'] = page * 10 + idx + 1
            result['page'] = page + 1
            all_results.append(result)
        
        print(f"Получено результатов на странице {page + 1}: {len(page_results)}")
    
    return all_results


# Пример использования
if __name__ == "__main__":
    # Тестовый запрос
    test_query = "механизированная штукатурка Москва"
    
    print(f"Выполнение поиска по запросу: '{test_query}'")
    results = search_by_topic(test_query, pages=2)  # Получаем 2 страницы
    
    print(f"\nВсего получено результатов: {len(results)}")
    
    # Выводим первые 5 результатов для примера
    for i, result in enumerate(results[:5]):
        print(f"\nРезультат {i+1}:")
        print(f"  Заголовок: {result['title'][:100]}...")
        print(f"  URL: {result['url']}")
        print(f"  Позиция: {result['position']}")