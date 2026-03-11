"""
Модуль для загрузки HTML-страниц (crawling).
"""

import requests
from typing import Optional, Dict, Any
from urllib.parse import urlparse
import time


def fetch_page(url: str, timeout: int = 10, headers: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Загружает HTML-страницу по URL.
    
    Аргументы:
        url: URL страницы
        timeout: Таймаут запроса в секундах
        headers: Заголовки запроса (если None, используются стандартные)
    
    Возвращает:
        Словарь с результатами:
        - success: bool
        - html: str (если успешно)
        - status_code: int
        - error: str (если ошибка)
        - url: str (исходный URL)
    """
    result = {
        "url": url,
        "success": False,
        "html": None,
        "status_code": None,
        "error": None
    }
    
    # Стандартные заголовки, чтобы имитировать браузер
    if headers is None:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        }
    
    try:
        response = requests.get(url, timeout=timeout, headers=headers)
        result["status_code"] = response.status_code
        
        if response.status_code == 200:
            # Проверяем кодировку
            if response.encoding:
                response.encoding = response.apparent_encoding or response.encoding
            
            result["html"] = response.text
            result["success"] = True
        else:
            result["error"] = f"HTTP {response.status_code}"
            
    except requests.Timeout:
        result["error"] = "Timeout"
    except requests.ConnectionError:
        result["error"] = "Connection error"
    except Exception as e:
        result["error"] = str(e)
    
    return result


def fetch_page_with_retry(url: str, max_retries: int = 2, delay: float = 1.0) -> Dict[str, Any]:
    """
    Загружает страницу с повторными попытками при ошибке.
    
    Аргументы:
        url: URL страницы
        max_retries: Максимальное количество попыток
        delay: Задержка между попытками в секундах
    
    Возвращает:
        Результат fetch_page
    """
    for attempt in range(max_retries + 1):
        result = fetch_page(url)
        
        if result["success"]:
            return result
        
        if attempt < max_retries:
            time.sleep(delay * (attempt + 1))  # Увеличиваем задержку
    
    return result