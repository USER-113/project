# ------------------------------|      Импорт библиотек       |-----------------------------
import asyncio                                                  # Так как мы будем работать с Telethon, а Telethon работает асинхронно, то подключаем эту библиотеку
import psycopg2                                                 # Библиотека используется для подключения к БД
from psycopg2 import pool                                       # Пул соединений для БД
from telethon import TelegramClient                             # Импорт клиента Telegram
from telethon.tl.functions.messages import GetHistoryRequest    # Для чтения истории сообщений
from datetime import datetime, time, date                       # Работа с датами
import hashlib                                                  # Библиотека для формирования хэш-кода
import uuid                                                     # Библиотека для формирования хэш-кода
import os                                                       # Работа с переменными окружения, чтение данных БД из соединения Airflow
from urllib.parse import urlparse                               # Парсинг строки подключения
import sys                                                      # Необходима для чтения файла с данными для подключения к Telegram из директории /logics_files/
                                                                # Для удобства работы с кодом, все констаны зададим через переменные
                                                                # Структура с расположениями файлов должна выглядеть так:
                                                                # apache_airflow
                                                                # └── dags
                                                                #     ├── telegram_dag.py  => Файл, которые создают и запускают DAG-и
                                                                #     └── logics_files
                                                                #         │
                                                                #         ├── connect.py  => Файл, который создает файл authorization.session
                                                                #         ├── telegram_load.py   => Файл, с логикой обработки данных в рамках DAG-а
                                                                #         ├── con_data  => Файл, с данными подключения к БД
                                                                #         └── authorization.session  => Файл, с параметрами авторизации и подключения к telegram
# ------------------------------|   Подключение con_data.py    |-----------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from con_data import api_id, api_hash, phone
# ------------------------------|   Определяем переменные     |-----------------------------
lang = "ru"                                                     # Язык интерфейса клиента
system_lang = "ru-RU"                                           # Язык системы
# lang и system_lang добавлены т.к. без них случались сбои при создании файла session, не приходил код на телефон, на всякий добавлены и здесь. Это было найдено в интернете, как один их вариантов решения проблемы с получением кода.
authorization_file = os.path.join(current_dir, "authorization") # session файл лежит в logics_files и называется authorization
channel_name = "mod_russia"                                     # Адрес Telegram канала
date_param = datetime.today().date()                            # Автоматическое определение текущей даты
#Если потребуется задать период то указываем: datetime.combine(date(2026,3,1), time(0, 0, 0)), т.е. вместо date_param пишем в переменных начала и конца периода date(год,месяц,дата), месяц и дата без ведущих нулей.
start_date = datetime.combine(date_param, time(0, 0, 0))        # Начало периода
end_date = datetime.combine(date_param, time(23, 59, 59))       # Конец периода
# -----------Если нужен конкретный период ---------------------------------------------------
#start_date = datetime.combine(date(2026,3,27), time(0, 0, 0))  # Начало периода на конкретную дату
#end_date = datetime.combine(date(2026,3,27), time(23, 59, 59)) # Конец периода на конкретную дату
phrase = [                                                      # Фразы, по которым выбираем сообщения
    "дежурными средствами пво",
    "дежурные средства пво",
    "украинских беспилотных летательных аппар",
    "украинский беспилотный летательный аппар",
    "беспилотного летательного аппарата"
]
# Список исключающих фраз
exclude_phrase = [                                              # Фразы, по которым исключаем сообщения,не выбираем если есть любая из фраз
    "сводка министерства обороны российской федерации о ходе проведения специальной военной операци",
    "брифинг минобороны россии", "Главное за день"
]
batch_size = 300                                                # Количество сообщений за запрос
messages_batch = []                                             # Массив сообщений
insert_batch_size = 500                                         # Размер batch загрузки
# --------------------|Получение данных для подключения к БД из Airflow|--------------------      
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("<название вашего соединения в Apache Airflow, по котрому Airflow соединяется с БД>")               # Указываем наименование соединения с БД в Airflow.
db_host = conn.host
db_port = conn.port
db_name = conn.schema
db_user = conn.login
db_password = conn.password
# ------------------------------| Подключение к БД PostgreSQL|-----------------------------

#print("== BEGIN DEBUG SESSION ===")                             # DEBUG: проверяем, видит ли скрипт файл сессии, эту информацию будет видно в логах для графа в Airflow
#print("Текущий рабочий каталог:", os.getcwd())                  # Опциональный кусок, упрощает проверку работы и отладки
#print("Путь к файлу сессии:", authorization_file + ".session")
#print("Файлы в директории:", os.listdir(os.path.dirname(authorization_file)))
#print("Файл существует?", os.path.exists(authorization_file + ".session"))
#print("=== END DEBUG SESSION  ===")

db_pool = pool.SimpleConnectionPool(
    1,
    5,
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password
)
def get_connection():
    return db_pool.getconn()

def release_connection(conn):
    db_pool.putconn(conn)
# ------------------------------|    Подключение Telegram     |-----------------------------
client = TelegramClient(
    authorization_file,
    api_id,
    api_hash,
    lang_code=lang,
    system_lang_code=system_lang,
    proxy=("socks5", "<Указать хост>", <Ваш порт>)              # Если используется соединение для выхода в интернет, например ВПН. Если нет, то закомментирвоать строку.
)
# ------------------------------|  Сохранение в PostgreSQL    |-----------------------------
def save_batch_to_db(batch, load_dt):
    conn = get_connection()
    cur = conn.cursor()
    query = """
        insert into "STG".telegram_messages
        (msg_id, msg_text, msg_dt, load_dt)
        values (%s, %s, %s, %s)
        on conflict (msg_id, load_dt) do nothing
    """
    batch_with_load = [row + (load_dt,) for row in batch]
    cur.executemany(query, batch_with_load)
    conn.commit()
    cur.close()
    release_connection(conn)
# ------------------------------|        Сохранение лога      |-----------------------------
def save_log(status, load_from_dt, load_to_dt, msg_total, msg_save, work_time):
    conn = get_connection()
    cur = conn.cursor()
    load_dt = datetime.now()
    query = """
        insert into "STG".load_logs
        (status, load_from_dt, load_to_dt, msg_total, msg_save, work_time, load_dt)
        values (%s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(query,(status,str(load_from_dt),str(load_to_dt),str(msg_total),str(msg_save),str(work_time),load_dt))
    conn.commit()
    cur.close()
    release_connection(conn)
# ------------------------------|   Авторизация в Telegram    |-----------------------------
async def login_telegram():
    await client.connect()
    if not await client.is_user_authorized():
        raise Exception("Файл authorization.session отсутсвует или сессия сброшена, пересоздайте authorization.session.")
# ------------------------------|      Чтение сообщений       |-----------------------------
async def selection_of_messages(load_dt):
    start_time = datetime.now()
    channel = await client.get_entity(channel_name)
    offset_id = 0
    total_processed = 0
    saved_rows = 0
    max_iterations = 50000                                      # 50 000 повторений, т.к. за 2022 год было около 48 000 сообщений.
    iteration = 0
    while True:
        iteration += 1
        if iteration > max_iterations:
            raise Exception("Превышено максимальное количество итераций (возможен бесконечный цикл)")
        history = await client(GetHistoryRequest(
            peer=channel,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=batch_size,
            max_id=0,
            min_id=0,
            hash=0
        ))
        msgs = history.messages
        if not msgs:
            break
        for msg in msgs:
            total_processed += 1
            if not msg.message or msg.media:
                continue
            msg_date = msg.date.replace(tzinfo=None)
            if msg_date < start_date:
                break
            if msg_date > end_date:
                continue
            text = msg.message.lower()
            if any(p in text for p in phrase):
                if any(e in text for e in exclude_phrase):
                    continue
                msg_id = msg.id                                 # Используем id сообщения из Telegram, если текст менялся, то выберем самый последний вариант по сочетанию msg_id + load_dt
                row = (
                    str(msg_id),
                    msg.message,
                    msg_date.strftime("%Y-%m-%d %H:%M:%S")
                )
                messages_batch.append(row)
                saved_rows += 1
                if len(messages_batch) >= insert_batch_size:
                    save_batch_to_db(messages_batch, load_dt)
                    messages_batch.clear()
        offset_id = msgs[-1].id
        if msgs[-1].date.replace(tzinfo=None) < start_date:
            break
    if messages_batch:
        save_batch_to_db(messages_batch, load_dt)
        messages_batch.clear()
    end_time = datetime.now()
    work_time = round((end_time - start_time).total_seconds())
    return total_processed, saved_rows, work_time
# ------------------------------|   Logout Telegram клиента   |-----------------------------
async def logout_telegram():
    await client.disconnect()
# ------------------------------|Последовательность выполнения|-----------------------------
async def main():
    start_time = datetime.now()
    load_dt = datetime.now()
    try:
        await login_telegram()
        total_processed, saved_rows, work_time = await selection_of_messages(load_dt)
        await logout_telegram()
        save_log('success', start_date, end_date, total_processed, saved_rows, work_time)
    except Exception as e:
        end_time = datetime.now()
        work_time = round((end_time - start_time).total_seconds(), 2)
        save_log('error', start_date, end_date, str(e), 0, work_time)
        raise
# -------------------------|Запуск исполнения пайплайна задач в коде|------------------------
def run():
    asyncio.run(main())
if __name__ == "__main__":
    run()