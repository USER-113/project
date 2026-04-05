import asyncio
from telethon import TelegramClient
from con_data import api_id, api_hash, phone
# ------------------------------|  Настройки Telegram  |------------------------------
lang = "ru"                                                     # Язык интерфейса клиента, если его не ставить коды авторизации могут не приходить
system_lang = "ru-RU"                                           # Язык системы/устройства, если его не ставить коды авторизации могут не приходить
authorization_file = "authorization"                            # Название файла, который будет сгенерирован, чтобы не запрашивать каждый раз код и неводить его, благодаря этому файлу программа всегда будет авторизована
# ------------------------------|  Создание клиента  |------------------------------
client = TelegramClient(
    authorization_file,
    api_id,
    api_hash,
    lang_code=lang,
    system_lang_code=system_lang,
    proxy=("socks5", "<Указать хост>", <Ваш порт>)              # Если используется соединение для выхода в интернет, например ВПН. Если нет, то закомментирвоать строку.
)
# ------------------------------|  Авторизация  |------------------------------
async def login_telegram():
    await client.connect()

    if not await client.is_user_authorized():
        # отправляем код на телефон
        await client.send_code_request(phone)
        print(f"Код отправлен на телефон {phone}")
        code = input("Введите код: ")  # вводим код один раз
        await client.sign_in(phone=phone, code=code)

    print(f"Авторизация успешна. Файл сессии создан, имя файла - {authorization_file}.session")
    await client.disconnect()
# ------------------------------|  Запуск  |------------------------------
asyncio.run(login_telegram())