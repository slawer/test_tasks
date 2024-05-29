
import os
import time
import json
from datetime import datetime, timezone
from typing import List, Tuple

import pytest
import requests
from pydantic import BaseModel, ValidationError

from pandas import DataFrame

# Задание 1
# имеется текстовый файл f.csv, по формату похожий на .csv с разделителем |
"""
lastname|name|patronymic|date_of_birth|id
Фамилия1|Имя1|Отчество1 |21.11.1998   |312040348-3048
Фамилия2|Имя2|Отчество2 |11.01.1972   |457865234-3431
...
"""
# 1. Реализовать сбор уникальных записей
# 2. Случается, что под одинаковым id присутствуют разные данные - собрать такие записи


def test_1_1():
    # если требуются уникальные записи (строки) из файла
    file_name = 'f.csv'
    strings = set()
    with open(file_name, 'r', encoding='utf8') as f:
        f.readline()
        for st in f.readlines():
            strings.add(st.strip())

    print(strings)


def test_1_2():
    # если требуются уникальные пользователи
    records = set()
    file_name = 'f.csv'
    pattern = '|'
    with open(file_name, 'r', encoding='utf8') as f:
        f.readline()
        for st in f.readlines():
            fio, name, patronymic, dt, _ = st.split(pattern)
            records.add((fio.strip(), name.strip(), patronymic.strip(), dt.strip()))

    print(records)


# Задание 2
# в наличии список множеств. внутри множества целые числа
# посчитать
#  1. общее количество чисел
#  2. общую сумму чисел
#  3. посчитать среднее значение
#  4. собрать все числа из множеств в один кортеж
# m = [{11, 3, 5}, {2, 17, 87, 32}, {4, 44}, {24, 11, 9, 7, 8}]
# *написать решения в одну строку

def test_2():
    m = [{11, 3, 5}, {2, 17, 87, 32}, {4, 44}, {24, 11, 9, 7, 8}]

    print(*[[len(t), sum(t), sum(t)/len(t), t] for t in [tuple(e for s in m for e in s)]][0])


# Задание 3
# имеется список списков
# a = [[1,2,3], [4,5,6]]
# сделать список словарей
# b = [{'k1': 1, 'k2': 2, 'k3': 3}, {'k1': 4, 'k2': 5, 'k3': 6}]
# *написать решение в одну строку


def test_3():
    a = [[1, 2, 3], [4, 5, 6]]

    print([{f'k{i+1}': v for i, v in enumerate(l)} for l in a])


# Задание 4
# Имеется папка с файлами
# Реализовать удаление файлов старше N дней

def test_4():
    N = 1
    dr = 'test_dir'

    delta = 60 * 60 * 24 * N
    cnt = 0
    for fn in os.listdir(dr):
        pth = os.path.join(dr, fn)
        if time.time() - os.stat(pth).st_ctime > delta:
            os.remove(pth)
            cnt += 1
    print(cnt)


# Задание 5*
'''
Имеется текстовый файл с набором русских слов(имена существительные, им.падеж)
Одна строка файла содержит одно слово.
Написать программу которая выводит список слов, каждый элемент списка которого - это новое слово,
которое состоит из двух сцепленных в одно, которые имеются в текстовом файле.
Порядок вывода слов НЕ имеет значения
Например, текстовый файл содержит слова: ласты, стык, стыковка, баласт, кабала, карась
Пользователь вводмт первое слово: ласты
Программа выводит:
ластык
ластыковка
Пользователь вводмт первое слово: кабала
Программа выводит:
кабаласты
кабаласт
Пользователь вводмт первое слово: стыковка
Программа выводит:
стыковкабала
стыковкарась
'''


def test_5():

    fn = 'слова.txt'
    words = set()
    with open(fn, encoding='utf8') as f:
        for word in f.readlines():
            words.add(word.strip())

    word = input('введите слово:')

    if word not in words:
        print('Введите слово из списка')
        return

    sp = set()

    for n in range(2, len(words)-1):
        for w in words:
            if w.startswith(word[-n:]):
                sp.add(word[:-n]+w)

    for e in sp:
        print(e)


'''
# Задание 6*
Имеется банковское API возвращающее JSON
{
    "Columns": ["key1", "key2", "key3"],
    "Description": "Банковское API каких-то важных документов",
    "RowCount": 2,
    "Rows": [
        ["value1", "value2", "value3"],
        ["value4", "value5", "value6"]
    ]
}
Основной интерес представляют значения полей "Columns" и "Rows",
которые соответственно являются списком названий столбцов и значениями столбцов
Необходимо:
    1. Получить JSON из внешнего API
        ендпоинт: GET https://api.gazprombank.ru/very/important/docs?documents_date={"начало дня сегодня в виде таймстемп"}
        (!) ендпоинт выдуманный
    2. Валидировать входящий JSON используя модель pydantic
        (из ТЗ известно что поле "key1" имеет тип int, "key2"(datetime), "key3"(str))
    2. Представить данные "Columns" и "Rows" в виде плоского csv-подобного pandas.DataFrame
    3. В полученном DataFrame произвести переименование полей по след. маппингу
        "key1" -> "document_id", "key2" -> "document_dt", "key3" -> "document_name"
    3. Полученный DataFrame обогатить доп. столбцом:
        "load_dt" -> значение "сейчас"(датавремя)
*реализовать п.1 с использованием Apache Airflow HttpHook

#
Решение задач со * не обязательно, но как плюс

'''


def test_6():

    class TableSchema(BaseModel):
        Columns: List
        Description: str
        RowCount: int
        Rows: List[Tuple[int, datetime, str]]

    now = datetime.now()
    dt = datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=timezone.utc)

    answer = requests.get(f"https://slawer.pythonanywhere.com/docs?documents_date={int(dt.timestamp())}").json()

    try:
        data = TableSchema(**answer)
    except ValidationError as e:
        print(f'Ошибка валидации: {e}')
        return None

    df = DataFrame(data.Rows, columns=data.Columns)
    df = df.rename(columns={"key1": "document_id", "key2": "document_dt", "key3": "document_name"})
    df.insert(len(df.columns), "load_dt", [now] * (len(df.columns) - 1), True)

    print(df)
