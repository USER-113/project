# =================================
# Парсинг БПЛА drone_parsing.py
# =================================
import re
import pandas as pd
#from logics_files import con_data
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, timedelta
import con_data
# =================================
# ЧТЕНИЕ ПАРАМЕТРОВ БД И ПОДКЛЮЧЕНИЕ
# =================================
db_uri = f"postgresql+psycopg2://{con_data.user}:{con_data.password}@{con_data.host}:{con_data.port}/{con_data.dbname}"
engine = create_engine(db_uri)
# =================================
# список субъектов для замены и унификации
# =================================
entity_dict = {
"entity_01":["адыг"], "entity_04":["республика алтай","республики алтай","республике алтай","республику алтай","республикой алтай"],
"entity_02":["башкортостан"], "entity_03":["бурят"], "entity_05":["дагестан"], "entity_06":["ингушет"],"entity_07":["кабардин","балкарск"], 
"entity_08":["калмык"], "entity_09":["карачаев","черкесс"], "entity_10":["карел"], "entity_11":["коми"], "entity_91":["крым"], "entity_12":["марий","эл"],
"entity_13":["мордов"], "entity_14":["саха"], "entity_15":["осетия","алан"], "entity_16":["татарст"], "entity_17":["тыв"], "entity_18":["удмурт"],
"entity_19":["хакас"], "entity_20":["чечен","чечн"], "entity_21":["чуваш"], "entity_22":["алтайский край","алтайском крае","алтайского края","алтайскому краю"],
"entity_75":["забайкальск"], "entity_41":["камчатск"], "entity_23":["краснодарск"], "entity_24":["красноярск"], "entity_59":["пермск"], "entity_25":["приморск"],
"entity_26":["ставропольск"], "entity_27":["хабаровск"], "entity_28":["амурск"], "entity_29":["архангельск"], "entity_30":["астрахан"], "entity_31":["белгород"],
"entity_32":["брянск"], "entity_33":["владимир"], "entity_34":["волгоград"], "entity_35":["вологодск"], "entity_36":["воронеж"], "entity_37":["ивановск"],
"entity_38":["иркутск"], "entity_39":["калининград"], "entity_40":["калужск"], "entity_42":["кемеровск"], "entity_43":["кировск"], "entity_44":["костром"],
"entity_45":["курган"], "entity_46":["курск"], "entity_47":["ленинград"], "entity_48":["липецк"], "entity_49":["магаданск"], "entity_50":["московск"],
"entity_51":["мурманск"], "entity_52":["нижегород"], "entity_53":["новгород"], "entity_54":["новосибирс"], "entity_55":["омск"], "entity_56":["оренбург"],
"entity_57":["орлов"], "entity_58":["пензен"], "entity_60":["псков"], "entity_61":["ростов"], "entity_62":["рязан"], "entity_63":["самар"], "entity_64":["сарат"],
"entity_65":["сахалин"], "entity_66":["свердлов"], "entity_67":["смоленс"], "entity_68":["тамбов"], "entity_69":["тверс"], "entity_70":["томск"], "entity_71":["тульск"],
"entity_72":["тюмен"], "entity_73":["ульяновск"], "entity_74":["челябинск"], "entity_76":["ярославс"], "entity_77":["москв"], "entity_78":["санкт-петер"],
"entity_92":["севастоп"], "entity_79":["еврейск"], "entity_83":["ненецк"], "entity_93":["донецк"], "entity_94":["луганск"], "entity_95":["херсонск"],
"entity_90":["запорожск"], "entity_86":["ханты","югр"], "entity_87":["чукотский"], "entity_89":["ямало"], "entity_99":["байконур"], "entity_tc1":["азовск"],
"entity_tc2":["черн"], "entity_tc3":["каспийск", "каспий"]
}
# =================================
# ФРАЗЫ ДЛЯ УДАЛЕНИЯ
# =================================
delete_word = [
    "дежурными", "средствами", "украинский","украинских","пресечены", "пресечена", "самолетного типа",
    "территорией", "территориями", "на территории", "подписаться на канал мы в мах", "max - территория своих", "попытки", "попытка", "попытку", 
    "киевского", "киевским", "области", "акватории", "акваториями", "акватория", "акваторией","областью", "область", "областей", "области", "моря", "морей",
    "регионом", "региона", "регион", "российской федерации.", "российской федерации", "по объектам", "#оперативно", "республиками", "республики", "режима", "режим", 
    "совершить", "совершил", "террористических", "террористическую", "террористическая", "террористические", "очередная","атаку с", "атаку","атака с",
    "атака", "атаки с", "атаки", "режима", "режим", "в ходе", "отражения", ". силами был ", "с применением", "применением", "совершить", "мы в мах", 
    "украинские", "оперативно", "полуостровом", "канал в max - только для своих!", "в max - источник мощных новостей.", "канал минобороны россии в max - только для своих", 
    "в max - только для своих", "курс на max", "минобороны россии", "минобороны", "россии", "очередные", "силами Черноморского флота",  
    "края", "пво", "над", "еще", "режимом", "были", "предприняты", "атак", "силами", "была", "был", "осуществить","полуострова", "обнаружен", "у побережья", 
    "побережье", "побережьями", "канал в max - только для своих", "!", "сегодня",
    "перехваченных и уничтоженных", "перехвачены и уничтожены", "перехваченному и уничтоженному", "перехваченном и уничтоженном", "перехвачено и уничтожено", "перехвачен и уничтожен",
    "уничтоженных и перехваченных", "уничтожены и перехвачены", "уничтоженному и перехваченному", "уничтоженном и перехваченном", "уничтожено и перехвачено", "уничтожен и перехвачен",
    "перехваченных", "перехвачены", "перехваченному", "перехваченном", "перехвачено", "перехвачен", "уничтоженных", "уничтожены", "уничтоженному", "уничтоженном", "уничтожено", "уничтожен",
    "сбиты", "сбито", "сбит", "ещё", "еще", "c-200", "с-200"
]
# =================================
# ЗАМЕНЫ для замены слов, фраз, символов 
# =================================
# 21:00:00 – 02:59:59 — ночь
# 03:00:00 – 08:59:59 — утро
# 09:00:00 – 14:59:59 — день
# 15:00:00 – 20:59:59 — вечер
change_word = {
    "бп": ["беспилотного","беспилотному","беспилотном","беспилотный","беспилотные","беспилотными","беспилотным","беспилотных"],
    "л": [" летательного "," летательному "," летательном "," летательный "," летательные "," летательными "," летательным ","летательных"],
    "а": ["аппаратами","аппаратам","аппаратах","аппарата","аппарате","аппаратом","аппаратов","аппарату","аппараты","аппарат"],
    "в":["в течение прошедшей ночи, в", "по состоянию на", "в течение прошедшей ночи в","в течении прошедшей ночи в"],
    "с":["в течение прошедшей ночи c", "в течение прошедшей ночи с"],
    " ": ["  "],
    "бэк": ["безэкипажный катер", "безэкипажных катера"],
    "в том числе 1,": ["в том числе один,"],
    "с 00:00": ["с полуночи"],
    "до 23:59": ["до полуночи"],
    "с 21:00 до 02:59 д": ["в течение прошедшей ночи д"],
    "с 21:00 до 02:59 п": ["в течение прошедшей ночи п","в течение прошедшей ночи, п", "в течении прошедшей ночи п"],
    "с 21:00 до 02:59 и": ["в течение прошедшей ночи и"],
    "с 21:00 до 02:59 0": ["в течение прошедшей ночи 0"],
    "с 21:00 до 02:59 1": ["в течение прошедшей ночи 1"],
    "с 21:00 до 02:59 2": ["в течение прошедшей ночи 2"],
    "с 21:00 до 02:59 3": ["в течение прошедшей ночи 3"],
    "с 21:00 до 02:59 4": ["в течение прошедшей ночи 4"],
    "с 21:00 до 02:59 5": ["в течение прошедшей ночи 5"],
    "с 21:00 до 02:59 6": ["в течение прошедшей ночи 6"],
    "с 21:00 до 02:59 7": ["в течение прошедшей ночи 7"],
    "с 21:00 до 02:59 8": ["в течение прошедшей ночи 8"],
    "с 21:00 до 02:59 9": ["в течение прошедшей ночи 9"],
    "с 21:00 до 02:59": ["в течение ночи", "сегодня ночью", "в ночь на", "в течение сегодняшней ночи","ночью", "в ночное время"],
    "с 03:00 до 08:59": ["в течение утра", "утром"],
    "с 00:00 до 08:59": ["в ночные и утренние часы"],
    "с 15:00 до 20:59": ["вечером"],
    "с 09:00 до 14:59": ["днем"],
    "": ["т.г.", "т.г.", "с.г."],
    "бпла": ["бп л а"],
    ". бпла": ["бпла . бпла"],
}
# =================================
# --- МЕСЯЦЫ справочник месяцев для определения дат
# =================================
month_map = {
    "января": 1, "январь": 1,
    "февраля": 2, "февраль": 2,
    "марта": 3, "март": 3,
    "апреля": 4, "апрель": 4,
    "мая": 5, "май": 5,
    "июня": 6, "июнь": 6,
    "июля": 7, "июль": 7, 
    "августа": 8, "август": 8,
    "сентября": 9, "сентябрь": 9, 
    "октября": 10, "октябрь": 10, 
    "ноября": 11, "ноябрь": 11, 
    "декабря": 12, "декабрь": 12
}
# =================================
# ЧИСЛА справочник для перевода слов в цифры
# =================================
num_map = {
    "одиннадцать": 11, "одиннадцатью":11,"одиннадцати":11,
    "двенадцать": 12, "двенадцатью":12,"двенадцати":12,
    "тринадцать": 13, "тринадцатью":13,"тринадцати":13,
    "четырнадцать": 14, "четырнадцатью":14,"четырнадцати":14,
    "пятнадцать": 15, "пятнадцатью":15,"пятнадцати":15,
    "шестнадцать": 16, "шестнадцатью":16,"шестнадцати":16,
    "семнадцать": 17, "семнадцатью":17,"семнадцати":17,
    "восемнадцать": 18, "восемнадцатью":18,"восемнадцати":18,
    "девятнадцать": 19, "девятнадцатью":19,"девятнадцати":19,
    "двадцать": 20, "двадцатью":20,"двадцати":20,
    "тридцать": 30,"тридцатью":30,"тридцати":30,
    "сорок": 40,"сорока":40,
    "пятьдесят": 50, "пятьюдесятью":50,"пятидесяти":50,
    "шестьдесят": 60, "шестьюдесятью":60,"шестидесяти":60,
    "семьдесят": 70,"семьюдесятью":70,"семидесяти":70,
    "восемьдесят": 80, "восемьюдесятью": 80, "восьмьюдесятью":80,"восемидесяти": 80,
    "девяносто": 90, "девяноста" :90,
    "сто": 100, "ста":100,
    "нуль": 0, "нуля": 0, 
    "один": 1, "одна": 1, "одного":1, "первый": 1, "одному": 1, "первый": 1,
    "два": 2, "две": 2, "двумя":2, "двух":2, "второй": 2, " оба " : 2,
    "три": 3, "тремя":3,"трех":3, "третий": 3,
    "четыре": 4, "четырьмя":4, "четырех":4, 
    "пять": 5, "пятью":5,"пяти":5,
    "шесть": 6, "шестью":6, "шести":6, 
    "семь": 7, "семью":7, "семи":7,
    "восемь": 8, "восемью":8, "восьми":8,
    "девять": 9, "девятью":9, "девяти":9,
    "десять": 10, "десятью":10, "десяти":10,
}
# =================================
# НОРМАЛИЗАЦИЯ
# =================================
def normalize_text(text):
    text = str(text).lower()
    text = re.sub(r"\bc\b", "с", text)
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"[🎖🔹▪▫◾¶⚡️❗💬#]", " ", text)
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()
# =================================
# ЧИСЛА СЛОВАМИ В ЦИФРЫ
# =================================
def words_to_number(text):
    words = text.split()
    result = []
    temp_num = 0  # Здесь копим сумму (100 + 20 + 1)
    has_num = False
    for w in words:
        clean_w = w.strip(".,!?;:").lower()
        if clean_w in num_map:
            val = num_map[clean_w]
            # Если это тысяча, умножаем накопленное на 1000
            if val >= 1000:
                temp_num = (temp_num if temp_num > 0 else 1) * val
            else:
                temp_num += val
            has_num = True
        else:
            if has_num:             # Если встретили обычное слово которое содержит в себе слово, обозначающее цефру, например "трактори(ста)", сначала «выгружаем» накопленное число
                result.append(str(temp_num))
                temp_num = 0
                has_num = False
            result.append(w)
    if has_num:                     # Добавляем последнее число, если текст закончился на нем
        result.append(str(temp_num))
    return " ".join(result)
# =================================
# УДАЛЕНИЕ ФРАЗ
# =================================
def remove_phrases(text):
    for phrase in delete_word:
        text = re.sub(rf"\b{phrase}\b", "", text)
    return text
# =================================
# ЗАМЕНЫ
# =================================
def replace_change_words(text):
    for target, variants in change_word.items():
        for variant in variants:
            text = text.replace(variant, target)
    return text
# =================================
# РЕГИОНЫ
# =================================
def replace_regions(text):
    for region, variants in entity_dict.items():
        for variant in variants:
            text = re.sub(rf"\b{variant}\w*\b", region.lower(), text)
    return text
# =================================
# ОСНОВНАЯ ОБРАБОТКА
# =================================
def process_text(text):
    text = normalize_text(text)         # нормализация
    text = replace_change_words(text)   # делаем замены
    text = words_to_number(text)        # заменяем текстовые описания чисел на цифры
    text = remove_phrases(text)         # удаление фраз и слов
    text = replace_regions(text)        # Перезапись регионов
    text = re.sub(r"\s+", " ", text)
    return text.strip()
# =================================
# ПАРСИНГ ВРЕМЕНИ
# =================================
def parse_time(t):
    if t is None:
        return None
    # --- строгая проверка формата ---
    if not re.fullmatch(r"\d{1,2}(:\d{2})?", t):
        return None
    parts = t.split(":")
    hour = int(parts[0])
    # --- фильтр часов ---
    if hour > 23:
        return None
    if len(parts) == 2:
        minute = int(parts[1])
    # --- фильтр минут ---
        if minute > 59:
            return None
        return datetime.strptime(t,"%H:%M").time()
    return datetime.strptime(t,"%H").time()
# =================================
# ИЗВЛЕЧЕНИЕ ДАТ (ОТДЕЛЬНЫЙ БЛОК)
# =================================
def extract_datetime_range(text, msg_dt):                       #функция определения диапазона дат и времени def <имя функции>
    # --- приводим msg_dt к datetime ---
    if isinstance(msg_dt, str):
        msg_dt = pd.to_datetime(msg_dt)
    # --- приводим msg_dt к МСК ---
    msg_dt_msk = msg_dt + timedelta(hours=3)
    msg_date = msg_dt_msk.date()
    msg_time = msg_dt_msk.time()
    # --- "с ЧЧ:ММ до ЧЧ:ММ ДД месяц" ---
    m = re.search(
        r"с\s*(\d{1,2}[:\.]\d{2})\s*до\s*(\d{1,2}[:\.]\d{2})\s*(\d{1,2})\s+"
        r"(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)",
        text
    )
    if m:
        from_t = m.group(1).replace(".", ":")
        to_t = m.group(2).replace(".", ":")
        day = int(m.group(3))
        month = month_map.get(m.group(4))
        year = msg_dt_msk.year  #год берём из msg_dt
        from_time = parse_time(from_t)
        to_time = parse_time(to_t)
        if from_time and to_time and month:
            from_dt = datetime(year, month, day, from_time.hour, from_time.minute)
            to_dt = datetime(year, month, day, to_time.hour, to_time.minute)
            if from_time > to_time:
                from_dt -= timedelta(days=1)
            return from_dt, to_dt
    # --- "ДД месяц (ГГГГ) с ЧЧ:ММ до ЧЧ:ММ" ---
    m = re.search(
        r"(\d{1,2})\s+"
        r"(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)"
        r"(?:\s+(\d{4}))?\s+с\s*(\d{1,2}[:\.]\d{2})\s*до\s*(\d{1,2}[:\.]\d{2})",
        text
    )
    if m:
        day = int(m.group(1))
        month = month_map.get(m.group(2))
        year = int(m.group(3)) if m.group(3) else msg_dt_msk.year  # если года нет — берём из msg_dt
        from_t = m.group(4).replace(".", ":")
        to_t = m.group(5).replace(".", ":")
        from_time = parse_time(from_t)
        to_time = parse_time(to_t)
        if from_time and to_time and month:
            from_dt = datetime(year, month, day, from_time.hour, from_time.minute)
            to_dt = datetime(year, month, day, to_time.hour, to_time.minute)
            if from_time > to_time:
                from_dt -= timedelta(days=1)
            return from_dt, to_dt
    # --- "ДД месяц (ГГГГ) в ЧЧ:ММ мск" ---
    m = re.search(
        r"(\d{1,2})\s+"
        r"(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)"
        r"(?:\s+(\d{4}))?\s+в\s*(\d{1,2})[:\.](\d{2})\s*мск",
        text
    )
    if m:
        day = int(m.group(1))
        month = month_map.get(m.group(2))
        year = int(m.group(3)) if m.group(3) else msg_dt_msk.year
        hour = int(m.group(4))
        minute = int(m.group(5))
        if hour <= 23 and minute <= 59 and month:
            dt = datetime(year, month, day, hour, minute)
            return dt, dt
    # --- "около/сегодня около / в районе HH.MM или HH часов" ---
    m = re.search(
        r"(?:сегодня\s+)?(?:около|в\s+районе)\s+(\d{1,2})(?:[:\.](\d{2}))?(?:\s*час[а-я]*)?",
        text
    )
    if m:
        hour = int(m.group(1))
        minute = int(m.group(2)) if m.group(2) else 0
        if hour <= 23 and minute <= 59:
            dt = datetime.combine(msg_date, datetime.min.time()) + timedelta(
                hours=hour,
                minutes=minute
            )
            # если время уже прошло сегодня → сегодня
            if msg_time > dt.time():
                return dt, dt
            else:
                return dt - timedelta(days=1), dt - timedelta(days=1)
    # --- "ДД месяц около ЧЧ.ММ" ---
    m = re.search(
        r"(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)"
        r"(?:\s+т\.г\.)?\s+около\s+(\d{1,2})(?:[:\.](\d{2}))?",
        text
    )
    if m:
        day = int(m.group(1))
        month_str = m.group(2)
        hour = int(m.group(3))
        minute = int(m.group(4)) if m.group(4) else 0
    # --- фильтр времени ---
        if hour <= 23 and minute <= 59:
            month = month_map.get(month_str)
            if month:
                year = msg_dt_msk.year
                dt = datetime(year, month, day, hour, minute)
                return dt, dt
    # --- "около HH.MM/в HH.MM (без "с" перед временем)" ---
    m = re.match(r"(около|в)\s*(\d{1,2})\.(\d{2})", text)
    if m:
        hour = m.group(2)
        minute = m.group(3)
        t = datetime.strptime(f"{hour}:{minute}", "%H:%M").time()
        if msg_time > t:
            dt = datetime.combine(msg_date, t)
        else:
            dt = datetime.combine(msg_date - timedelta(days=1), t)
        return dt, dt
    from_time = None
    to_time = None
    # --- с HH:MM до HH:MM ---
    m = re.search(r"с\s*(\d{1,2}[:\.]\d{2})\s*до\s*(\d{1,2}[:\.]\d{2})", text)
    if m:
        from_time = m.group(1).replace(".", ":")
        to_time = m.group(2).replace(".", ":")
    # --- с HH до HH ---
    if not from_time:
        m = re.search(r"с\s*(\d{1,2})\s*до\s*(\d{1,2})", text)
        if m:
            from_time = m.group(1)
            to_time = m.group(2)
    # --- только "с" ---
    if not from_time:
        m = re.search(r"с\s*(\d{1,2}[:\.]?\d{0,2})", text)
        if m:
            from_time = m.group(1).replace(".", ":")
    # --- только "до" ---
    if not to_time:
        m = re.search(r"до\s*(\d{1,2}[:\.]?\d{0,2})", text)
        if m:
            to_time = m.group(1).replace(".", ":")
    # --- преобразуем ---
    from_time = parse_time(from_time)
    to_time = parse_time(to_time)
    from_dt = None
    to_dt = None
# =================================
# ЛОГИКА ИНТЕРВАЛОВ
# =================================
    if from_time and to_time:
        to_dt = datetime.combine(msg_date, to_time)
        from_dt = datetime.combine(msg_date, from_time)
        if from_time > to_time:
            from_dt -= timedelta(days=1)
    elif from_time:
    # только начало → конец = msg_dt ---
        to_dt = msg_dt_msk
        from_dt = datetime.combine(msg_date, from_time)
        if from_time > to_dt.time():
            from_dt -= timedelta(days=1)
    elif to_time:
    # ближайшее прошлое время ---
        candidate = datetime.combine(msg_date, to_time)
        if candidate > msg_dt_msk:
            candidate -= timedelta(days=1)
        to_dt = candidate
    return from_dt, to_dt
# =================================
# функция парсинка сообщения на субъекты и количество бпла
# =================================
def extract_entity_phrases(text):
    """
    Возвращает список кортежей (count, entity_code)
    """
    results = []
    seen = set()                                                                     # По маскам некоторые выражения могут быть обработаны несколько раз, убираем будлирующиеся строки 
# --- новая маска: "по ZZ бпла - entity_XX и entity_YY , ..." ---
    pattern_po_list = [                                                              # маска для фразы "по 00 бпла" ":"/"- "entity_00, .... и entity_00 ,"
        r"(?:^|[,\s])по\s+(\d+)\sбпла\s*-\s*(entity_[A-Za-z0-9]+)\s+и\s+(entity_[A-Za-z0-9]+)",
        r"(?:^|[,\s])по\s+(\d+)\sбпла\s*-\s*((?:entity_[A-Za-z0-9]+\s*,\s*)+(?:entity_[A-Za-z0-9]+))",
        r"(?:^|[,\s])по\s+(\d+)\sбпла\s*-\s*((?:entity_[A-Za-z0-9]+(?:\s*(?:,|и|, и)\s*)?)+)"

    ]
    for pattern in pattern_po_list:
        matches = re.findall(pattern, text)
        for match in matches:
            count = int(match[0])
        # собираем все entity из оставшейся части
            tail = " ".join(match[1:])
            entities = re.findall(r"entity_[A-Za-z0-9]+", tail)
            for entity_full in entities:
                entity_code = entity_full.replace("entity_", "")
                key = (count, entity_code)
                if key not in seen:
                    seen.add(key)
                    results.append(key)             
# --- новая маска ---
    pattern_multi = r"(\d+)\s*бпла\s*:?\s*((?:entity_[A-Za-z0-9]+(?:\s*,\s*|\s+и\s+)?)+)"# маска для фразы "ZZ бпла entity_GG, entity_GG, ..."
    matches = re.findall(pattern_multi, text)
    for count_str, entities_block in matches:
        total = int(count_str)
        entities = re.findall(r"entity_[A-Za-z0-9]+", entities_block)
        n = len(entities)
        if n < 2:
            continue  # нам нужно минимум 2 entity
        base = total // n
        remainder = total % n
        for i, entity_full in enumerate(entities):
            entity_code = entity_full.replace("entity_", "")
            count_val = base + (1 if i < remainder else 0)
            key = (count_val, entity_code)
            if key not in seen:
                seen.add(key)
                results.append(key)
# --- новая маска ---
    patterns = [
        r"(\d+)\s*-\s*(entity_[A-Za-z0-9]+)",                                       # маска для фразы "00 - entity_00"
        r"(\d+)\s*(entity_[A-Za-z0-9]+)",                                           # маска для фразы "00 entity_00"
        r"(\d+)\s*бпла\s*-\s*(entity_[A-Za-z0-9]+)",                                # маска для фразы "00 бпла - entity_00"
        r"(\d+)\s*бпла\s*в\s*(entity_[A-Za-z0-9]+)",                                # маска для фразы "00 бпла в entity_00"
        r"(\d+)\s*бпла\s*(entity_[A-Za-z0-9]+)",                                    # маска для фразы "00 бпла entity_00"
        r"(entity_[A-Za-z0-9]+)\s*-\s*(\d+)",                                       # маска для фразы "entity_00 - 00"
        r"(entity_[A-Za-z0-9]+)\s+(\d+)\s*бпла",                                    # маска для фразы "entity_00 00 бпла"
        r"(entity_[A-Za-z0-9]+)\s+(\d+)\s*бэк",                                     # маска для фразы "entity_00 00 бэк"
        r"бпла\s*\.\s*(\d+)\s*бпла.*?(entity_[A-Za-z0-9]+)",                        # маска для фразы "бпла . 00 бпла <текст и символы> entity_00"
    ]
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for m in matches:
            if len(m) == 2:
                a, b = m
# --- определяем где число, где entity ---
                if a.startswith("entity_"):
                    entity = a
                    count = b
                else:
                    count = a
                    entity = b
# --- нормализация entity ---
                if entity.startswith("entity_"):
                    entity_code = entity.replace("entity_", "")
                    key = (int(count), entity_code)
                    if key not in seen:                                             # По маскам некоторые выражения могут быть обработаны несколько раз, убираем будлирующиеся строки
                        seen.add(key)
                        results.append(key)
# --- новая маска ---
    pattern_1 = r"(entity_[A-Za-z0-9]+)\s*\(\s*(\d+)\s*бпла\s*\)"                   # маска для фразы "entity_00 (00 бпла)"
    for m in re.findall(pattern_1, text):
        entity_full, count = m
        entity_code = entity_full.replace("entity_", "")
        key = (int(count), entity_code)
        if key not in seen:                                                         # По маскам некоторые выражения могут быть обработаны несколько раз, убираем будлирующиеся строки
            seen.add(key)
            results.append(key)
# --- новая маска ---
    pattern_2 = r"(?:бпла\s*\.\s*)?бпла.*?(entity_[A-Za-z0-9]+)"                    # маска для фразы "бпла . бпла entity_00 ." или " . бпла entity_00 .", или "бпла . бпла <текст и символы> entity_00."
    matches = re.findall(pattern_2, text)
    for entity_full in matches:
        entity_code = entity_full.replace("entity_", "")
        key = (1, entity_code)
        if key not in seen:                                                         # По маскам некоторые выражения могут быть обработаны несколько раз, убираем будлирующиеся строки
            seen.add(key)
            results.append(key)                                                     # count = 1, если явно не указано количество
 # --- новая маска ---   
    pattern_beck = r"бэк\s+в\s+(entity_[A-Za-z0-9]+)"                               # маска для фразы "бэк в entity_00"
    for entity_full in re.findall(pattern_beck, text):
        entity_code = entity_full.replace("entity_", "")
        key = (1, entity_code)
        if key not in seen:                                                         # По маскам некоторые выражения могут быть обработаны несколько раз, убираем будлирующиеся строки
            seen.add(key)
            results.append(key)                                                     # count = 1, если явно не указано количество
# --- новая маска ---
    pattern_msk_single = r"мск\s+бпла\s+(entity_[A-Za-z0-9]+)"                      # маска для фразы "мск бпла entity_00" без указания количества
    for entity_full in re.findall(pattern_msk_single, text):
        entity_code = entity_full.replace("entity_", "")
        key = (1, entity_code)
        if key not in seen:                                                         # По маскам некоторые выражения могут быть обработаны несколько раз, убираем будлирующиеся строки
            seen.add(key)
            results.append(key)                                                     # count = 1, если явно не указано количество
# --- новая маска ---
    pattern_msk_multi = r"мск\s+бпла\s+((?:entity_[A-Za-z0-9]+\s*(?:и\s*)?)+)"      # маска для фразы "мск бпла entity_00 и entity_00, и .... ."
    matches = re.findall(pattern_msk_multi, text)
    for match in matches:
        entities = re.findall(r"entity_[A-Za-z0-9]+", match)
        for entity_full in entities:
            entity_code = entity_full.replace("entity_", "")
            key = (1, entity_code)
            if key not in seen:                                                     # По маскам некоторые выражения могут быть обработаны несколько раз, убираем будлирующиеся строки
                seen.add(key)
                results.append(key)                                                 # count = 1, если явно не указано количество
# --- новая маска ---
    pattern_msk_count = r"мск\s+бпла\s+(entity_[A-Za-z0-9]+)\s*\.\s*(\d+)\s*бпла"   # маска для фразы "мск бпла entity_00. 00 бпла."
    for entity_full, count in re.findall(pattern_msk_count, text):
        entity_code = entity_full.replace("entity_", "")
        key = (int(count), entity_code)
        if key not in seen:                                                         # По маскам некоторые выражения могут быть обработаны несколько раз, убираем будлирующиеся строки
            seen.add(key)
            results.append(key)           
# -----------------------------
    return results
# =================================
# функция
# =================================
if __name__ == "__main__":
    # --- подключение --- 
    engine = create_engine(
        f"postgresql+psycopg2://{con_data.user}:{con_data.password}@{con_data.host}:{con_data.port}/{con_data.dbname}"
    )
    # ---  загрузка  ---
    query = text('''
	select ftm.msg_id as msg_id, tm.msg_text as msg_text, ftm.msg_dt as msg_dt, ftm.load_dt as load_dt from (select msg_id, msg_dt, max (load_dt) as load_dt from "STG".telegram_messages as tm1 group by msg_id, msg_dt) as ftm
	left join "STG".telegram_messages as tm on ftm.msg_id=tm.msg_id and ftm.msg_dt=tm.msg_dt and ftm.load_dt=tm.load_dt where tm.msg_text not like '%водка Министерства обороны Российской Федерации о ходе проведения специальной военной операци%'
	and tm.msg_text not like '%рифинг Минобороны России%' and tm.msg_text not like '%Главное за день%' and tm.msg_text not like '%с начала проведения специальной военной операции%';
    ''')
    df = pd.read_sql(query, engine)
    # --- обработка текста ---
    df["msg_sample"] = df["msg_text"].apply(process_text)
    # --- извлечение дат ---
    df[["from_dt", "to_dt"]] = df.apply(
        lambda row: pd.Series(
            extract_datetime_range(row["msg_sample"], row["msg_dt"])
        ),
        axis=1
    )
    # --- дозаполнение from_dt и to_dt
    df["from_dt"] = df["from_dt"].fillna(df["msg_dt"])
    df["to_dt"] = df["to_dt"].fillna(df["msg_dt"])
# =================================
# формируем таблицу с полученными данными
# =================================
    rows = []
    for _, row in df.iterrows():
        phrases = extract_entity_phrases(row["msg_sample"])
        from_dt = row["from_dt"] if pd.notna(row["from_dt"]) else row["msg_dt"]
        to_dt = row["to_dt"] if pd.notna(row["to_dt"]) else row["msg_dt"]
        if phrases:
            for i, (count, entity_code) in enumerate(phrases, start=1):
                rows.append({
                    "msg_id": row["msg_id"],
                    "row_num": i,
                    "count_uav": count,
                    "entity_code": entity_code,
                    "from_dt": from_dt,
                    "to_dt": to_dt,
                    "msg_dt": row["msg_dt"],
                    "load_dt": row["load_dt"],
                    "msg_text": row["msg_text"],
                    "msg_sample": row["msg_sample"],
                })
        else:
            rows.append({
                "msg_id": row["msg_id"],
                "row_num": None,
                "count_uav": None,
                "entity_code": None,
                "from_dt": from_dt,
                "to_dt": to_dt,
                "msg_dt": row["msg_dt"],
                "load_dt": row["load_dt"],
                "msg_text": row["msg_text"],
                "msg_sample": row["msg_sample"],
            })
    parsed_df = pd.DataFrame(rows)
    parsed_df = (
        parsed_df
        .sort_values(["msg_id", "entity_code", "row_num"])
        .groupby(["msg_id", "entity_code"], as_index=False)
        .first()
    )
# =================================
# загрузка в таблицу БД
# =================================
    parsed_df.to_sql(
        "parsed_messages",
        engine,
        schema="ODS",
        if_exists="append",
        index=False
    )
    print("Готово. Все данные загружены в ODS.parsed_messages.")