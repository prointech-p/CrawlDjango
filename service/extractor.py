"""
Модуль для извлечения контактных данных из HTML-страниц.
Содержит функции для парсинга телефонов, email-адресов и физических адресов.
Использует BeautifulSoup для работы с HTML и регулярные выражения для поиска шаблонов.
"""

import re
from bs4 import BeautifulSoup
from typing import List, Set, Optional

# Регулярное выражение для поиска кандидатов в телефонные номера
# Ищет: +7 или 8, затем минимум 10 символов из цифр, дефисов, пробелов, скобок
# Примеры: +7 (495) 123-45-67, 89161234567, +7-916-123-45-67
PHONE_CANDIDATE_REGEX = re.compile(r"(\+7|8)[\d\-\s\(\)]{10,}")

# Регулярное выражение для поиска email-адресов
# Соответствует стандартному формату email: local-part@domain
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
)

# Ключевые слова для идентификации адресов в тексте
# Используются для фильтрации фрагментов, которые могут содержать адрес
ADDRESS_KEYWORDS = [
    "адрес",      # прямое указание на адрес
    "г.",         # город (сокращенно)
    "город",      # город (полностью)
    "ул",         # улица (сокращенно)
    "улица",      # улица (полностью)
    "шоссе",      # шоссе
    "проспект",   # проспект
    "пр-т",       # проспект (сокращенно)
    "переулок",   # переулок
    "наб",        # набережная (сокращенно)
    "набережная"  # набережная (полностью)
]

# Максимальная длина строки адреса для фильтрации
# Адреса обычно не бывают очень длинными, это помогает отсеять шум
MAX_ADDRESS_LENGTH = 120


def extract_addresses(html: str) -> List[str]:
    """
    Извлекает возможные адреса из HTML-кода страницы.
    
    Алгоритм работы:
    1. Парсит HTML с помощью BeautifulSoup
    2. Ищет текстовые блоки в тегах div, p, span, li
    3. Разбивает блоки на части по разделителям
    4. Проверяет каждую часть на наличие ключевых слов адреса
    5. Очищает найденные адреса и возвращает список уникальных значений
    
    Аргументы:
        html: Строка с HTML-кодом страницы
        
    Возвращает:
        Список уникальных строк, которые могут быть адресами
    """
    # Парсим HTML
    soup = BeautifulSoup(html, "html.parser")
    
    # Множество для хранения уникальных адресов
    addresses = set()
    
    # Ищем все теги, которые могут содержать текст с адресом
    for tag in soup.find_all(["div", "p", "span", "li"]):
        
        # Получаем текст тега, заменяя переносы строк пробелами
        text = tag.get_text(" ", strip=True)
        
        # Разбиваем текст на части по разделителям
        # Это помогает отделить адрес от другого контента
        parts = re.split(r"[|•\n]", text)
        
        for part in parts:
            part = part.strip()
            
            # Пропускаем слишком длинные фрагменты
            if len(part) > MAX_ADDRESS_LENGTH:
                continue
            
            # Проверяем наличие ключевых слов в нижнем регистре
            lower = part.lower()
            if any(k in lower for k in ADDRESS_KEYWORDS):
                
                # Очищаем найденный адрес от лишних символов
                cleaned = clean_address(part)
                
                if cleaned:
                    addresses.add(cleaned)
    
    return list(addresses)


def clean_address(text: str) -> str:
    """
    Очищает и нормализует строку адреса.
    
    Выполняет:
    1. Заменяет множественные пробелы на одиночные
    2. Удаляет слово "адрес" в начале строки
    
    Аргументы:
        text: Исходная строка с адресом
        
    Возвращает:
        Очищенную строку
    """
    # Заменяем множественные пробелы на одиночные
    text = re.sub(r"\s+", " ", text)
    
    # Удаляем слово "адрес:" или "адрес-" в начале строки
    text = re.sub(r"^адрес[:\-]?\s*", "", text, flags=re.IGNORECASE)
    
    return text.strip()


def extract_text(html: str) -> str:
    """
    Извлекает чистый текст из HTML, удаляя скрипты и стили.
    
    Аргументы:
        html: Строка с HTML-кодом
        
    Возвращает:
        Текст страницы без HTML-тегов
    """
    soup = BeautifulSoup(html, "html.parser")
    
    # Удаляем все скрипты и стили (они не содержат полезного текста)
    for tag in soup(["script", "style"]):
        tag.extract()
    
    # Получаем текст из оставшихся тегов
    return soup.get_text(" ")


def normalize_phone(phone: str) -> Optional[str]:
    """
    Нормализует телефонный номер в международный формат.
    
    Правила нормализации:
    1. Удаляет все нецифровые символы
    2. Если номер начинается с 8, заменяет на 7 (код России)
    3. Проверяет длину (должен быть 11 цифр, начинаться с 7)
    
    Аргументы:
        phone: Сырая строка с телефоном
        
    Возвращает:
        Нормализованный номер в формате +7XXXXXXXXXX или None, если номер некорректный
    """
    # Оставляем только цифры
    digits = re.sub(r"\D", "", phone)
    
    # Заменяем 8 в начале на 7 (международный формат)
    if digits.startswith("8"):
        digits = "7" + digits[1:]
    
    # Проверяем корректность: 11 цифр, начинается с 7
    if len(digits) == 11 and digits.startswith("7"):
        return "+" + digits
    
    return None


def extract_phones(text: str) -> List[str]:
    """
    Извлекает телефонные номера из текста.
    
    Аргументы:
        text: Текст для поиска телефонов
        
    Возвращает:
        Список уникальных нормализованных телефонных номеров
    """
    phones = set()
    
    # Ищем все совпадения с шаблоном телефона
    for match in PHONE_CANDIDATE_REGEX.finditer(text):
        
        raw_phone = match.group()
        
        # Пробуем нормализовать найденный номер
        normalized = normalize_phone(raw_phone)
        
        if normalized:
            phones.add(normalized)
    
    return list(phones)


def extract_emails(text: str) -> List[str]:
    """
    Извлекает email-адреса из текста.
    
    Аргументы:
        text: Текст для поиска email'ов
        
    Возвращает:
        Список уникальных email-адресов
    """
    # Находим все email'ы по регулярному выражению и возвращаем уникальные
    return list(set(EMAIL_REGEX.findall(text)))


def extract_organization_name(html: str) -> Optional[str]:
    """Пытается извлечь название организации из HTML."""
    from bs4 import BeautifulSoup
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        
        title_tag = soup.find("title")
        if title_tag and title_tag.text:
            title = title_tag.text.strip()
            if len(title) < 200:
                return title
        
        h1_tag = soup.find("h1")
        if h1_tag and h1_tag.text:
            h1 = h1_tag.text.strip()
            if len(h1) < 200:
                return h1
                
    except Exception:
        pass
    
    return None


def load_html(file_path: str) -> str:
    """
    Загружает HTML-код из файла.
    
    Аргументы:
        file_path: Путь к HTML-файлу
        
    Возвращает:
        Содержимое файла в виде строки
    """
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()


def extract_all_from_html(html: str) -> dict:
    """
    Удобная функция для извлечения всех данных сразу.
    
    Аргументы:
        html: HTML-код страницы
        
    Возвращает:
        Словарь с телефонами, email'ами и адресами
    """
    # Извлекаем текст для поиска телефонов и email'ов
    text = extract_text(html)
    
    return {
        "phones": extract_phones(text),
        "emails": extract_emails(text),
        "addresses": extract_addresses(html)
    }


if __name__ == "__main__":
    """
    Пример использования модуля.
    Загружает HTML из файла и выводит найденные контакты.
    """
    
    FILE = "page_content.html"
    
    print("Загружаем HTML:", FILE)
    
    try:
        html = load_html(FILE)
        
        print("\nРазмер HTML:", len(html), "байт")
        
        # Используем удобную функцию для извлечения всего сразу
        data = extract_all_from_html(html)
        
        print("\n----- ТЕЛЕФОНЫ -----")
        for p in data["phones"]:
            print(p)
        print(f"Найдено телефонов: {len(data['phones'])}")
        
        print("\n----- EMAIL'Ы -----")
        for e in data["emails"]:
            print(e)
        print(f"Найдено email: {len(data['emails'])}")
        
        print("\n----- АДРЕСА -----")
        for addr in data["addresses"]:
            print(addr)
        print(f"Найдено адресов: {len(data['addresses'])}")
        
    except FileNotFoundError:
        print(f"Ошибка: файл {FILE} не найден")
    except Exception as e:
        print(f"Ошибка при обработке: {e}")