#!/usr/bin/env python3
"""
=============================================================================
analyze-json.py — Анализ происхождения файлов сборки
=============================================================================

ОПИСАНИЕ:
    Скрипт сравнивает хеши исходных файлов проекта (src.json) с данными
    трассировщика сборки (buildography) и распределяет файлы по категориям:
    какие исходники реально используются, какие избыточны, откуда пришли
    бинари в дистрибутив.

    Анализ выполняется за 4 прохода:
      Проход 1 — сравнение хешей исходников с buildography
      Проход 2 — анализ компилируемых файлов (.c, .cpp, .rs, .go и др.)
      Проход 3 — анализ интерпретируемых файлов (.py, .sh, .js и др.)
      Проход 4 — проверка происхождения бинарей в дистрибутиве

    Требуемые входные данные (готовятся отдельными скриптами):
      results/{project}/sources/{project}_src.json   — хеши исходников
      results/{project}/sources/{project}_bin.json   — хеши файлов дистрибутива
      results/{project}/ext/binaries_in_bin.txt      — список ELF/PE бинарей
      buildography/builds/{project}/*.json           — данные трассировщика
      lib/utilities.yaml                             — списки компиляторов и интерпретаторов

    ВАЖНО: если трассировщик не охватывает фазу компиляции (например,
    компиляция происходит внутри dpkg-buildpackage), бинари собранные
    из ваших исходников попадут в untraced_from_src — это нормально
    и не означает подозрительного происхождения.

ИСПОЛЬЗОВАНИЕ:
    python3 analyze-json.py [OPTIONS]

ОПЦИИ:
    -p, --single-project NAME   Обработать только один указанный проект.
                                Без этого флага обрабатываются все проекты
                                из buildography/builds/.

    -d, --by-disk               Включить Проход 5: разбить результаты
                                по дискам (pass5/src/ и pass5/bin/).
                                Полезно для проектов с несколькими дисками.

    -k, --keep                  Сохранять предыдущие результаты.
                                По умолчанию (без флага) папка try1
                                перезаписывается, а try2, try3, ... удаляются.
                                С флагом создаётся новая папка tryN.

    -h, --help                  Показать эту справку и выйти.

ПРИМЕРЫ:
    # Обработать все проекты (try1 перезаписывается)
    python3 analyze-json.py

    # Обработать один проект
    python3 analyze-json.py -p KTDL.00554-01
    python3 analyze-json.py --single-project KTDL.00554-01

    # Обработать один проект с разбивкой по дискам
    python3 analyze-json.py -p KTDL.00554-01 -d

    # Сохранить предыдущие результаты (создать try2, try3, ...)
    python3 analyze-json.py -p KTDL.00554-01 -k
    python3 analyze-json.py --single-project KTDL.00554-01 --keep

ПРИМЕЧАНИЕ:
    Перед запуском убедитесь что для каждого проекта выполнен
    analyze-ext.sh — он генерирует binaries_in_bin.txt необходимый
    для Прохода 4. Если файл отсутствует, скрипт предложит запустить
    analyze-ext.sh автоматически.

НАСТРОЙКА ПУТЕЙ:
    Все пути настраиваются в начале скрипта (раздел НАСТРАИВАЕМЫЕ ПУТИ):
      BASE_DIR         — корневая папка проекта
      BUILDOGRAPHY_DIR — папка с данными трассировщика
      RESULTS_DIR      — папка для результатов
      UTILITIES_FILE   — путь к utilities.yaml

=============================================================================
РЕЗУЛЬТИРУЮЩИЕ ФАЙЛЫ (results/{project}/izb/tryN/)
=============================================================================

Каждый запуск создаёт новую папку tryN (try1, try2, ...) чтобы не
перезаписывать предыдущие результаты. Файлы разбиты по проходам.

  --- Проход 1 (pass1/): сравнение хешей исходников ---
  Каждый исходный файл из src.json проверяется по хешу в buildography.

  {project}_direct.json / .txt
      Исходные файлы, чей хеш напрямую найден в buildography — файл
      использовался в сборке. Поля: path, hash

  {project}_parent.json / .txt
      Файлы, чей хеш не найден напрямую, но найден хеш родительского
      архива (.tar.gz и т.д.) — файл попал в сборку через архив.
      Поля: path, hash, parent_hash

  {project}_redundant-by-hash.json / .txt
      Файлы не найденные ни напрямую ни через архив — потенциально
      избыточные исходники. Поля: path, hash

  --- Проход 2 (pass2/): компилируемые языки ---
  Из redundant выделяются компилируемые файлы и проверяется были ли
  они на входе у компиляторов (gcc, g++, rustc и др. из utilities.yaml).

  {project}_compiled_not_copied_to_distr.json / .txt
      Компилируемые файлы (.c, .cpp, .rs, .go и др.) которые не попали
      в дистрибутив — компилировались, но результат не нужен.
      Поля: path, hash, source (direct|parent)

  --- Проход 3 (pass3/): интерпретируемые языки ---
  Анализируются .py, .sh, .js и другие интерпретируемые файлы.

  {project}_executed.json / .txt
      Файлы которые запускались интерпретатором в ходе сборки
      (python3 script.py, bash build.sh и т.д.).
      Поля: path, hash, commands (список команд запуска)

  {project}_compiled_used.json / .txt
      Интерпретируемые файлы результат компиляции которых (.pyc и т.д.)
      попал в дистрибутив. Поля: path, hash

  {project}_compiled_unused.json / .txt
      Интерпретируемые файлы которые компилировались, но результат
      не попал в дистрибутив — избыточные. Поля: path, hash

  {project}_copied.json / .txt
      Интерпретируемые файлы присутствующие в дистрибутиве напрямую
      (скопированы как есть). Поля: path, hash

  {project}_not_used.json / .txt
      Интерпретируемые файлы которые нигде не используются —
      не запускались, не компилировались, не скопированы в дистрибутив.
      Поля: path, hash

  --- Проход 4 (pass4/): происхождение бинарей дистрибутива ---
  Для каждого ELF/PE бинаря из binaries_in_bin.txt определяется
  откуда он взялся. Категории упорядочены от "чистых" к "подозрительным".

  {project}_compiled_from_src.json / .txt
      Файл появился в ходе сборки: его хеш является выходом прослеженной
      команды, и при этом отсутствует в src.json.

      ВНИМАНИЕ: категория НЕ означает, что файл собран из исходных текстов
      изделия. Это else-ветка Pass 4, то есть попадание в неё означает
      отсутствие возражений, а не доказательство. Сюда уходит всё, что
      сборка так или иначе произвела, включая переупакованные и подписанные
      сторонние пакеты. Измерение на NPUR.34018-01: из 124 уникальных файлов
      этой категории 53 произведены wheel pack, 27 — python -m build,
      22 — bsign, 6 — unzip, остальные — pip/dpkg/node/tar. Компиляторов
      среди производителей нет ни одного.

      Разбор по действительному происхождению лежит рядом, см. ниже
      origin_*. В summary{N}/ выносится только этот файл, единым списком.
      Поля: path, hash, container (если внутри пакета),
            operation, operation_cmd, origin, origin_detail, origin_chain

  {project}_origin_product_src.json / .txt
  {project}_origin_approved_source.json / .txt
  {project}_origin_download.json / .txt
  {project}_origin_unresolved.json / .txt
      Разбор предыдущей категории по ТЕРМИНАЛУ цепочки происхождения —
      по тому файлу, из содержимого которого получен данный.

      Две независимые оси:
        origin (категория)  — где цепочка заканчивается:
            product_src      терминал в src.json изделия
            approved_source  терминал на согласованном носителе (TRUSTED),
                             совпадение ПО ХЕШУ; в origin_detail указаны
                             носитель и архив-основание
            download         терминал на сетевой загрузке; в origin_detail
                             адрес источника из командной строки
            unresolved       терминал не опознан — проверка не смогла
                             подтвердить происхождение
        operation (атрибут) — чем файл произведён: compiled, built,
            repacked, signed, extracted, copied, transformed, downloaded,
            other. Переупаковка, подпись, распаковка и копирование
            содержимое не создают, поэтому для них происхождение ищется
            во входе команды.

      Порядок проверки: src.json -> TRUSTED -> загрузка -> не установлено.
      Поскольку TRUSTED проверяется раньше загрузки, в origin_download
      остаётся только то, что скачано И не найдено ни на одном
      согласованном носителе.

  {project}_binaries_from_src.json / .txt
      Хеш бинаря найден в src.json — бинарь хранился прямо в исходниках
      и скопирован в дистрибутив. Требует внимания (бинарь в репозитории).
      Поля: path, hash, container

  {project}_untraced_from_src.json / .txt
      Хеш в src.json, но трассировщик не видит как он попал в дистрибутив.
      Типично когда компиляция происходит внутри dpkg-buildpackage и
      трассировщик не охватывает эту фазу — тогда это ваши собственные
      скомпилированные бинари и подозрений нет.
      Поля: path, hash, container

  {project}_external_built.json / .txt
      Бинарь собран (трассировщик видит компиляцию), но зависимости
      не из src.json — скомпилирован из внешних исходников. Подозрительно.
      Поля: path, hash, container, external_deps, filtered_deps (опц.)
        external_deps  — подозрительные внешние зависимости:
                         path, hash, aliases (для versioned .so)
        filtered_deps  — допустимые зависимости (системные заголовки,
                         .so из /usr/lib и т.д.) с полем reason

  {project}_external_prebuilt.json / .txt
      Трассировщик видит бинарь как зависимость чьей-то команды, но
      сам он не собирался — пришёл готовым (apt, wget, pip). Подозрительно.
      Поля: path, hash, container

  {project}_external_package_content.json / .txt
      Файлы внутри внешних пакетов (.deb, pip whl, node_modules) источник
      которых известен трассировщику (apt download, pip install и т.д.).
      Не подозрительно — это штатные внешние зависимости.
      JSON: сгруппировано по пакетам: package_type, container, source,
            command (apt download / pip install / npm install), files.
      TXT: path<TAB>hash<TAB>package_type<TAB>container<TAB>command

  {project}_untraced_external.json / .txt
      Не в src.json, трассировщик не видит, не внутри известного пакета.
      Полностью неизвестное происхождение. Очень подозрительно.
      Поля: path, hash, container (если определён)

  {project}_system_binaries.json / .txt
      Бинарь в системном пути дистрибутива (/usr/lib/, /lib/ и т.д.) —
      системные библиотеки поставляемые дистрибутивом, не проектом.
      Поля: path, hash, container

=============================================================================
"""

import array
import bisect
import gc
import sqlite3
import sys
import os
import json as _json_stdlib  # всегда доступен — нужен для fallback при ошибках orjson

# Пробуем загрузить orjson из локальной папки lib/ рядом со скриптом.
# orjson в 3-5 раз быстрее стандартного json при парсинге больших файлов.
# Если не найден или не совместим — используем стандартный json.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LIB_DIR    = os.path.join(os.path.dirname(_SCRIPT_DIR), 'lib')
try:
    if os.path.isdir(_LIB_DIR):
        sys.path.insert(0, _LIB_DIR)
    import orjson as _orjson

    def _json_loads(data):
        """
        Парсит JSON строку/байты через orjson.
        orjson строгий к control character внутри строк (некоторые
        buildography файлы их содержат) — при ошибке парсинга делаем
        fallback на стандартный json с strict=False, который их пропускает.
        """
        if isinstance(data, str):
            data = data.encode('utf-8', errors='replace')
        try:
            return _orjson.loads(data)
        except _orjson.JSONDecodeError:
            text = data.decode('utf-8', errors='replace')
            return _json_stdlib.loads(text, strict=False)

    print("[INFO] JSON backend: orjson {} (fast mode, with stdlib fallback)".format(
        _orjson.__version__), flush=True)
    _ORJSON_AVAILABLE = True

except (ImportError, Exception) as _e:

    def _json_loads(data):
        """Парсит JSON строку через стандартный json."""
        if isinstance(data, bytes):
            data = data.decode('utf-8', errors='replace')
        return _json_stdlib.loads(data, strict=False)

    _ORJSON_AVAILABLE = False
    _orjson_reason = str(_e) if not isinstance(_e, ImportError) else "not found in {}".format(_LIB_DIR)
    print("[INFO] JSON backend: stdlib json (orjson unavailable: {})".format(_orjson_reason), flush=True)

import json  # оставляем для совместимости с остальным кодом
import argparse
import glob
import time
from pathlib import Path
from datetime import datetime

# Защита от UnicodeEncodeError при печати путей/имён с битой кодировкой
# (суррогатные символы из неверно закодированной кириллицы и т.п.).
# reconfigure доступен с Python 3.7 — заменяем ошибочные символы вместо краха.
try:
    sys.stdout.reconfigure(errors='replace')
    sys.stderr.reconfigure(errors='replace')
except (AttributeError, Exception):
    pass

# Глобальный таймер — время старта скрипта
_SCRIPT_START = time.monotonic()


def _ts():
    """Возвращает строку [HH:MM:SS] от начала запуска."""
    elapsed = int(time.monotonic() - _SCRIPT_START)
    h = elapsed // 3600
    m = (elapsed % 3600) // 60
    s = elapsed % 60
    return "[{:02d}:{:02d}:{:02d}]".format(h, m, s)


def _safe(s):
    """
    Делает строку безопасной для print — заменяет суррогатные и
    невалидные символы. Нужно для имён файлов/путей с битой кодировкой
    (например кириллица в неверной кодировке даёт суррогаты, которые
    ломают print с UnicodeEncodeError).
    """
    try:
        return str(s).encode('utf-8', errors='replace').decode('utf-8', errors='replace')
    except Exception:
        return repr(s)

# =============================================================================
# НАСТРАИВАЕМЫЕ ПУТИ
# =============================================================================
# Эвристика для Java: если True, .class файлы чьё базовое имя совпадает
# с .java файлом из src.json считаются собственными (compiled_from_src).
# Покрывает случай когда трассировщик не видит компиляцию javac.
# Включает внутренние классы: File$Inner.class → File.java
# --trust-java в командной строке перекрывает это значение.
TRUST_JAVA = True

# Шаг прогресса для Pass 4 iter (в процентах).
# 10 = каждые 10% (11 строк на итерацию)
# 1  = каждый 1%  (101 строка — подробно)
# 25 = каждые 25% (5 строк — кратко)
PASS4_PROGRESS_STEP_PCT = 10
# =============================================================================
BASE_DIR         = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILDOGRAPHY_DIR = os.path.join(BASE_DIR, "buildography", "builds")
RESULTS_DIR      = os.path.join(BASE_DIR, "results")
UTILITIES_FILE   = os.path.join(BASE_DIR, "lib", "utilities.yaml")
GENERATE_JSON_SCRIPT = os.path.join(BASE_DIR, "scripts", "generate_json_v2_test.sh")

# -----------------------------------------------------------------------------
# СОГЛАСОВАННЫЕ (ДОВЕРЕННЫЕ) НОСИТЕЛИ
# -----------------------------------------------------------------------------
# Доверенные материалы кладутся в src/ как обычные "проекты" с плоским именем:
#
#   src/TRUSTED.GENERAL/{src,bin}          — действует для ВСЕХ изделий
#   src/TRUSTED.<ИМЯ ИЗДЕЛИЯ>/{src,bin}    — действует только для этого изделия
#
# Плоское имя (точка вместо вложенности) выбрано сознательно: unpack.sh и
# generate_json.sh строго двухуровневые — они обходят src/*/ и ищут внутри
# ровно src и bin. Вложенность src/TRUSTED/GENERAL/ они бы молча пропустили
# ("Subdir not found, skipping") и вышли с кодом 0, не создав ни одного json.
# При плоском имени оба скрипта работают без правок.
#
# Инвентаризация делается тем же generate_json.sh, поэтому сверка идёт
# ПО ХЕШАМ, а не по путям: путь говорит "откуда взяли", хеш — "что именно
# взяли", и только второе отличает согласованный носитель от постороннего
# кэша, поднятого по тому же адресу.
# -----------------------------------------------------------------------------
TRUSTED_PREFIX  = "TRUSTED."
TRUSTED_GENERAL = "TRUSTED.GENERAL"
# =============================================================================


# =============================================================================
# ЧТЕНИЕ HASH_CMD ИЗ BASH СКРИПТА (для redundant.txt)
# =============================================================================
def read_hash_cmd(script_path):
    """Читает значение HASH_CMD из bash скрипта."""
    if not os.path.exists(script_path):
        print(_ts() + "   redundant.txt: generate script not found: {}".format(
            os.path.basename(script_path)))
        print(_ts() + "   redundant.txt: hash_algorithm field will be empty")
        return ''
    try:
        import re
        with open(script_path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                stripped = line.strip()
                if stripped.startswith('#'):
                    continue
                m = re.match(r'^HASH_CMD\s*=\s*(.+)$', stripped)
                if m:
                    value = m.group(1).strip()
                    # Убираем кавычки и inline комментарий
                    value = re.sub(r'\s*#.*$', '', value)
                    value = value.strip('"\'')
                    if value:
                        print(_ts() + "   redundant.txt: HASH_CMD={} (from {})".format(
                            value, os.path.basename(script_path)))
                        return value
        print(_ts() + "   redundant.txt: HASH_CMD not found in {}".format(
            os.path.basename(script_path)))
        return ''
    except Exception as e:
        print(_ts() + "   redundant.txt: failed to read HASH_CMD: {}".format(e))
        return ''


# =============================================================================
# РАСШИРЕНИЯ ФАЙЛОВ
# =============================================================================
SOURCE_EXTENSIONS = {
    '.c', '.cpp', '.cc', '.cxx', '.c++', '.h', '.hpp', '.hh', '.hxx', '.h++',
    '.py', '.pyx', '.pxd', '.pxi',
    '.go',
    '.java',
    '.rs',
    '.js', '.ts', '.jsx', '.tsx', '.mjs', '.cjs',
    '.rb', '.rake', '.gemspec',
    '.sh', '.bash', '.zsh', '.fish', '.ksh', '.csh',
    '.pl', '.pm', '.pod', '.t',
    '.cs',
    '.swift',
    '.kt', '.kts',
    '.scala', '.sc',
    '.php', '.phtml', '.php3', '.php4', '.php5', '.php7',
    '.hs', '.lhs',
    '.erl', '.hrl', '.ex', '.exs',
    '.lua',
    '.r', '.R',
    '.f', '.f77', '.f90', '.f95', '.f03', '.for', '.ftn',
    '.asm', '.s', '.S',
    '.vhd', '.vhdl', '.v', '.sv', '.svh',
    '.m', '.mm',
    '.d',
    '.nim',
    '.zig',
    '.ml', '.mli',
    '.fs', '.fsi', '.fsx',
    '.clj', '.cljs', '.cljc',
    '.groovy', '.gvy', '.gy', '.gsh',
    '.dart',
    '.jl',
    '.sql',
    '.cmake', '.mk',
}

SOURCE_BASENAMES = {
    'Makefile', 'makefile', 'GNUmakefile', 'Kbuild', 'Kconfig'
}

EXCLUDED_EXTENSIONS = {
    '.sh', '.bash', '.zsh', '.fish', '.ksh', '.csh',
    '.sql', '.cmake', '.mk',
}

COMPILED_EXTENSIONS = {
    '.c', '.cc', '.cpp', '.cxx', '.c++', '.h', '.hh', '.hpp', '.hxx',
    '.s', '.S', '.asm',
    '.rs',
    '.java', '.kt', '.kts', '.scala',
    '.go',
    '.cs',
    '.swift',
    '.d',
    '.nim',
    '.zig',
    '.ml', '.mli',
    '.fs', '.fsi', '.fsx',
    '.hs', '.lhs',
    '.erl', '.hrl',
    '.f', '.f77', '.f90', '.f95', '.f03', '.for', '.ftn',
}

INTERPRETED_EXTENSIONS = (SOURCE_EXTENSIONS - COMPILED_EXTENSIONS - EXCLUDED_EXTENSIONS) | {
    '.pyc', '.pyo', '.pyd'
}

PYTHON_EXTENSIONS = {'.py', '.pyx', '.pxd', '.pxi'}


# =============================================================================
# ЗАГРУЗКА СПИСКОВ ИЗ UTILITIES.YAML
# =============================================================================
def load_utilities_lists(utilities_path):
    """Загружает множества компиляторов, линкеров и интерпретаторов."""
    compilers = set()
    linkers = set()
    interpreters = set()
    if not os.path.exists(utilities_path):
        print(_ts() + " utilities.yaml not found: {}".format(utilities_path))
        return compilers, linkers, interpreters
    try:
        import yaml
        with open(utilities_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        utilities = data.get('utilities', {})
        compilers = set(utilities.get('compilers', []))
        linkers = set(utilities.get('linkers', []))
        interpreters = set(utilities.get('interpreters', []))
        print(_ts() + " Loaded {} compilers, {} linkers, {} interpreters".format(
            len(compilers), len(linkers), len(interpreters)))
        return compilers, linkers, interpreters
    except ImportError:
        # fallback: простой парсер
        try:
            compilers = []
            linkers = []
            interpreters = []
            with open(utilities_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            in_compilers = False
            in_linkers = False
            in_interpreters = False
            for line in lines:
                stripped = line.rstrip()
                if stripped.strip() == 'compilers:':
                    in_compilers = True
                    in_linkers = False
                    in_interpreters = False
                    continue
                if stripped.strip() == 'linkers:':
                    in_linkers = True
                    in_compilers = False
                    in_interpreters = False
                    continue
                if stripped.strip() == 'interpreters:':
                    in_interpreters = True
                    in_compilers = False
                    in_linkers = False
                    continue
                if in_compilers:
                    if stripped and not stripped.startswith(' ') and not stripped.startswith('\t'):
                        in_compilers = False
                    else:
                        val = stripped.strip()
                        if val.startswith('- '):
                            compilers.append(val[2:].strip())
                if in_linkers:
                    if stripped and not stripped.startswith(' ') and not stripped.startswith('\t'):
                        in_linkers = False
                    else:
                        val = stripped.strip()
                        if val.startswith('- '):
                            linkers.append(val[2:].strip())
                if in_interpreters:
                    if stripped and not stripped.startswith(' ') and not stripped.startswith('\t'):
                        in_interpreters = False
                    else:
                        val = stripped.strip()
                        if val.startswith('- '):
                            interpreters.append(val[2:].strip())
            print(_ts() + " Loaded {} compilers, {} linkers, {} interpreters (fallback)".format(
                len(compilers), len(linkers), len(interpreters)))
            return set(compilers), set(linkers), set(interpreters)
        except Exception as e:
            print(_ts() + " Failed to parse utilities.yaml: {}".format(e))
            return set(), set()


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================
def is_source_file(path):
    p = Path(path)
    ext = p.suffix.lower()
    if ext in EXCLUDED_EXTENSIONS:
        return False
    if ext in SOURCE_EXTENSIONS:
        return True
    if p.name in SOURCE_BASENAMES:
        return True
    return False


def is_compiled_extension(path):
    ext = os.path.splitext(path)[1].lower()
    return ext in COMPILED_EXTENSIONS


def is_interpreted_extension(path):
    ext = os.path.splitext(path)[1].lower()
    return ext in INTERPRETED_EXTENSIONS


def is_python_extension(path):
    ext = os.path.splitext(path)[1].lower()
    return ext in PYTHON_EXTENSIONS


def get_versioned_filepath(filepath):
    """
    Универсальная функция: если файл существует, возвращает путь с _vN перед расширением.
    Работает для любых расширений.
    """
    if not os.path.exists(filepath):
        return filepath
    base, ext = os.path.splitext(filepath)
    version = 1
    while os.path.exists("{}_v{}{}".format(base, version, ext)):
        version += 1
    return "{}_v{}{}".format(base, version, ext)


# =============================================================================
# ПРОГРЕСС
# =============================================================================
def progress_log(label, current, total, step_pct=10):
    """
    Печатает сообщение о прогрессе каждые step_pct% (по умолчанию 10%).
    Не вызывает print на каждый элемент — не замедляет обработку.
    """
    if total <= 0:
        return
    step = max(1, total * step_pct // 100)
    if current % step == 0 or current == total:
        pct = current * 100 // total
        print(_ts() + "     {} {}/{} ({}%)".format(label, current, total, pct))


# =============================================================================
# ЗАГРУЗКА ДАННЫХ (с нормализацией путей)
# =============================================================================
def load_signatures(paths):
    all_signatures = []
    for path in paths:
        print(_ts() + "   Loading signatures: {}".format(os.path.basename(path)))
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            data = json.load(f)
        sigs = data.get('signatures', [])
        for s in sigs:
            # Добавляем нормализованный путь
            s['path_norm'] = os.path.normpath(s.get('path', ''))
        print(_ts() + "   Signatures in {}: {}".format(os.path.basename(path), len(sigs)))
        all_signatures.extend(sigs)
    print(_ts() + "   Total signatures (merged): {}".format(len(all_signatures)))
    return all_signatures


def load_bin_signatures(project_name):
    bin_path = os.path.join(RESULTS_DIR, project_name, "sources", "{}_bin.json".format(project_name))
    if not os.path.exists(bin_path):
        print(_ts() + "   bin.json not found: {}".format(bin_path))
        return set(), set()
    print(_ts() + "   Loading bin signatures: {}".format(os.path.basename(bin_path)))
    with open(bin_path, 'r', encoding='utf-8', errors='replace') as f:
        data = json.load(f)
    sigs = data.get('signatures', [])
    hashes = set()
    paths = set()
    for e in sigs:
        h = e.get('hash', '').strip()
        if h:
            hashes.add(h)
        p = e.get('path', '')
        if p:
            paths.add(os.path.normpath(p))
    return hashes, paths


# =============================================================================
# СОГЛАСОВАННЫЕ НОСИТЕЛИ: ПОИСК, ПРОВЕРКА ИМЁН, ЗАГРУЗКА
# =============================================================================
def _expected_trusted_names(project_name):
    """Имена каталогов доверенных носителей, допустимые для данного изделия."""
    return [TRUSTED_GENERAL, TRUSTED_PREFIX + project_name]


def discover_trusted_dirs(project_name, interactive=True):
    """
    Ищет в results/ каталоги согласованных носителей для данного изделия.

    Возвращает список имён каталогов, пригодных к использованию.

    Проверка имени строгая и регистрозависимая. Каталог, который совпадает
    с ожидаемым без учёта регистра, но не совпадает точно (trusted.general
    вместо TRUSTED.GENERAL), считается ОШИБКОЙ: о нём печатается
    предупреждение и, если interactive, задаётся вопрос о продолжении.
    Это сделано намеренно — молча проигнорированный доверенный носитель
    приводит к тому, что весь его состав уезжает в "происхождение не
    установлено", и причина этого неочевидна.
    """
    if not os.path.isdir(RESULTS_DIR):
        return []

    expected = _expected_trusted_names(project_name)
    expected_ci = {name.lower(): name for name in expected}

    present = sorted([
        entry for entry in os.listdir(RESULTS_DIR)
        if os.path.isdir(os.path.join(RESULTS_DIR, entry))
    ])

    good = []
    problems = []   # (найденное_имя, ожидаемое_имя, причина)

    for entry in present:
        low = entry.lower()
        if not low.startswith(TRUSTED_PREFIX.lower()):
            continue

        if entry in expected:
            good.append(entry)
            continue

        if low in expected_ci:
            # Совпало без учёта регистра — расхождение именно в регистре
            problems.append((entry, expected_ci[low], "регистр имени"))
            continue

        # Каталог вида TRUSTED.* , но не относящийся к этому изделию.
        # Это нормально (доверенный носитель другого изделия) — молчим,
        # кроме случая, когда он похож на имя текущего изделия по регистру.
        suffix = entry[len(TRUSTED_PREFIX):]
        if suffix.lower() == project_name.lower() and suffix != project_name:
            problems.append((entry, TRUSTED_PREFIX + project_name,
                             "регистр имени изделия"))

    if problems:
        print("")
        print(_ts() + "   [WARNING] Каталоги согласованных носителей с неверным именем:")
        for found, exp, why in problems:
            print(_ts() + "     найдено : {}".format(found))
            print(_ts() + "     ожидается: {}   ({})".format(exp, why))
        print(_ts() + "   Такие каталоги НЕ будут использованы как доверенные.")
        print(_ts() + "   Всё их содержимое попадёт в 'происхождение не установлено'.")
        print("")
        if interactive and sys.stdin is not None and sys.stdin.isatty():
            try:
                answer = input("   Продолжить без этих носителей? [y/N]: ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = ""
            if answer not in ("y", "yes", "д", "да"):
                print(_ts() + "   Остановлено пользователем. "
                              "Исправьте имена каталогов и запустите заново.")
                sys.exit(1)
            print(_ts() + "   Продолжаем без этих носителей (решение пользователя).")
        else:
            print(_ts() + "   [NOTE] Неинтерактивный запуск — продолжаем "
                          "без этих носителей.")

    if good:
        print(_ts() + "   Согласованные носители: {}".format(", ".join(good)))
    else:
        print(_ts() + "   Согласованные носители не найдены "
                      "(ожидались: {})".format(", ".join(expected)))
    return good


def _json_line_value(line):
    """
    Достаёт значение строкового поля из строки вида:   "path": "значение",
    Возвращает None, если строка не такой формы.
    """
    i = line.find(':')
    if i < 0:
        return None
    s = line.find('"', i + 1)
    if s < 0:
        return None
    e = line.rfind('"')
    if e <= s:
        return None
    val = line[s + 1:e]
    if '\\' in val:
        val = val.replace('\\\\', '\x00').replace('\\"', '"').replace('\x00', '\\')
    return val


def _stream_signatures(jpath):
    """
    Потоковый разбор json, который пишет generate_json.sh.

    Отдаёт пары (hash, path), НЕ материализуя файл целиком.

    Зачем. Инвентаризация согласованного носителя может быть огромной:
    на СВВП 0.9.6 это 2.23 млн файлов и 62 ГБ, что даёт json на 1.3-2 ГБ.
    Обычный json.load развернул бы его в список из 2.23 млн словарей по
    пять ключей — пять-восемь гигабайт объектов Python на пике, то есть
    гарантированный OOM, причём ДО того как построен хоть какой-то индекс.

    Формат фиксирован, его печатает printf в process_subdir:
        {
            "path": "...",
            "hash": "...",
            "archive": 0,
            "parents_hash": "...",
            "parents_chain": [...]
        }
    Каждое поле на своей строке; переводы строк внутри значений
    экранируются в json_escape, поэтому построчный разбор корректен.

    Проверка на "parents_hash" нужна, чтобы не спутать его с "hash".
    """
    cur_path = None
    with open(jpath, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            st = line.lstrip()
            if st.startswith('"path"'):
                cur_path = _json_line_value(st)
            elif st.startswith('"hash"'):
                h = _json_line_value(st)
                if h and cur_path is not None:
                    yield h, cur_path
                    cur_path = None


class TrustedIndex:
    """
    Компактный индекс хешей согласованных носителей.

    Устройство выбрано под объём в миллионы записей:

      - ключ хранится ЦЕЛЫМ числом, а не 64-символьной строкой
        (строка весит около 113 байт, целое примерно 32);

      - значением хранится не полный путь, а КОНТЕЙНЕР — ближайший
        архив. В отчёте фигурирует именно он, а контейнеров на носителе
        тысячи, а не миллионы, поэтому при интернировании все пути
        схлопываются в несколько тысяч строковых объектов.

    Точный путь файла внутри носителя при этом не хранится. Если он
    понадобится для разбора единичного случая, он находится по хешу
    в самом {носитель}_src.json одной командой grep.

    Обращение по строковому хешу, как и к обычному dict.
    """

    __slots__ = ('_by_hash', '_pool', 'labels', 'counts')

    def __init__(self):
        self._by_hash = {}
        self._pool = {}
        self.labels = []
        self.counts = {}

    def _intern(self, s):
        got = self._pool.get(s)
        if got is None:
            self._pool[s] = s
            got = s
        return got

    def add(self, h_str, label, kind, container):
        hi = _hash_to_int(h_str)
        if hi is None or hi in self._by_hash:
            return False
        self._by_hash[hi] = (self._intern(label),
                             self._intern(kind),
                             self._intern(container))
        return True

    def __contains__(self, h_str):
        hi = _hash_to_int(h_str)
        return hi is not None and hi in self._by_hash

    def __getitem__(self, h_str):
        return self._by_hash[_hash_to_int(h_str)]

    def get(self, h_str, default=None):
        hi = _hash_to_int(h_str)
        if hi is None:
            return default
        return self._by_hash.get(hi, default)

    def __len__(self):
        return len(self._by_hash)

    @property
    def pool_size(self):
        return len(self._pool)


def load_trusted_inventory(trusted_dirs):
    """
    Загружает инвентаризации согласованных носителей в TrustedIndex.

    Хранится только первое вхождение каждого хеша — для атрибуции
    достаточно одного основания, а дубли внутри носителя ничего не дают.

    Разбор потоковый. Если потоковый разбор не дал ни одной записи на
    непустом файле (например, формат генератора изменился), выполняется
    откат на обычный json.load с ГРОМКИМ сообщением — чтобы расхождение
    форматов было видно, а не проявлялось нехваткой памяти.
    """
    idx = TrustedIndex()

    for label in trusted_dirs:
        idx.labels.append(label)
        n = 0
        for kind in ("src", "bin"):
            jpath = os.path.join(RESULTS_DIR, label, "sources",
                                 "{}_{}.json".format(label, kind))
            if not os.path.exists(jpath):
                print(_ts() + "   [{}] {}.json отсутствует — пропускаем".format(
                    label, kind))
                continue

            size_mb = os.path.getsize(jpath) / (1024.0 * 1024.0)
            print(_ts() + "   [{}] {}.json: {:.0f} МБ, потоковый разбор...".format(
                label, kind, size_mb))

            seen = 0
            added = 0
            try:
                for h, p in _stream_signatures(jpath):
                    seen += 1
                    if idx.add(h, label, kind, _trusted_container(p)):
                        added += 1
                    if seen % 500000 == 0:
                        print(_ts() + "     [{}] {}: прочитано {}, "
                                      "уникальных {}".format(label, kind,
                                                             seen, len(idx)))
            except OSError as exc:
                print(_ts() + "   [{}] ошибка чтения {}.json: {}".format(
                    label, kind, exc))
                continue

            if seen == 0 and size_mb > 0:
                print(_ts() + "   [{}] [WARNING] потоковый разбор {}.json не дал "
                              "ни одной записи.".format(label, kind))
                print(_ts() + "   [{}] [WARNING] Вероятно изменился формат "
                              "generate_json.sh. Откат на json.load — "
                              "возможен большой расход памяти.".format(label))
                try:
                    with open(jpath, 'r', encoding='utf-8',
                              errors='replace') as f:
                        data = json.load(f)
                    for e in data.get('signatures', []):
                        h = (e.get('hash') or '').strip()
                        seen += 1
                        if idx.add(h, label, kind,
                                   _trusted_container(e.get('path', ''))):
                            added += 1
                    del data
                    gc.collect()
                except (ValueError, OSError) as exc:
                    print(_ts() + "   [{}] не удалось прочитать {}.json: "
                                  "{}".format(label, kind, exc))
                    continue

            n += added
            print(_ts() + "   [{}] {}.json: записей {}, уникальных "
                          "хешей {}".format(label, kind, seen, added))
        idx.counts[label] = n

    if idx.labels:
        print(_ts() + "   Доверенных хешей всего: {} "
                      "(различных контейнеров: {})".format(
                          len(idx), idx.pool_size))
        _log_memory("после загрузки TRUSTED")
    return idx


def load_buildography_data(paths):
    hashes = set()
    raw_cmds = []
    for path in paths:
        print(_ts() + "   Loading buildography: {}".format(os.path.basename(path)))
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            data = json.load(f, strict=False)
        before = len(hashes)
        component_commands = data.get('component_commands', [])
        raw_cmds.extend(component_commands)
        for cmd in component_commands:
            deps = cmd.get('dependencies', {})
            if isinstance(deps, dict):
                for h in deps.values():
                    h = h.strip()
                    if h:
                        hashes.add(h)
            elif isinstance(deps, list):
                for dep in deps:
                    h = dep.get('hash', '').strip()
                    if h:
                        hashes.add(h)
            outputs = cmd.get('output', {})
            if isinstance(outputs, dict):
                for h in outputs.values():
                    h = h.strip()
                    if h:
                        hashes.add(h)
            elif isinstance(outputs, list):
                for out in outputs:
                    h = out.get('hash', '').strip()
                    if h:
                        hashes.add(h)
        added = len(hashes) - before
        print(_ts() + "   Hashes from {}: {} (total pool: {})".format(
            os.path.basename(path), added, len(hashes)))
    print(_ts() + "   Total buildography hashes (merged): {}".format(len(hashes)))
    return hashes, raw_cmds


# Компиляторы фронтенды которые пишут вывод в пайп (stdout) а не в файл.
# Они читают .c/.cpp/.cc и знают целевой .o (в аргументах), но сам .o
# создаёт следующая в цепочке команда 'as' (ассемблер).
_COMPILER_FRONTENDS = {'cc1', 'cc1plus'}
# Обёртки которые тоже могут иметь исходник на входе и .o в аргументах
_COMPILER_WRAPPERS  = {'gcc', 'g++', 'cc', 'c++'}
_ASSEMBLER_TOOLS    = {'as'}


def link_compiler_to_assembler(raw_cmds):
    """
    Восстанавливает разорванную цепочку компиляции C/C++.

    Проблема: g++/cc1plus компилируют .cpp, но пишут ассемблерный вывод
    в пайп (-o -), поэтому у них output=0. Ассемблер 'as' создаёт .o,
    но читает .s из пайпа и не имеет .cpp в зависимостях. В результате
    связь .cpp -> .o теряется и .cpp попадает в not_compiled.

    Решение через дерево процессов (parent_id):
      g++ (id=P)
        ├─ cc1plus (parent_id=P)  компилирует .cpp
        └─ as      (parent_id=P)  создаёт .o
    cc1plus и as — братья с общим parent_id. Находим для каждого cc1plus
    его брата 'as' по parent_id и копируем hash .o из as в output cc1plus.

    Это надёжнее сопоставления по basename: имена .o могут совпадать
    в разных модулях (build_operator/logger.o и build_client/logger.o),
    но parent_id уникален для каждого вызова компилятора.

    Модифицирует raw_cmds на месте. Возвращает количество связанных пар.
    """
    # Индекс: parent_id -> список output .o (path, hash) от команд 'as'
    parent_to_as_output = {}
    for cmd in raw_cmds:
        cl = cmd.get('command', [])
        if not cl:
            continue
        tool = os.path.basename(str(cl[0]))
        if tool not in _ASSEMBLER_TOOLS:
            continue
        pid = cmd.get('parent_id')
        if pid is None:
            continue
        outputs = cmd.get('output', {})
        items = outputs.items() if isinstance(outputs, dict) else \
                [(o.get('path', ''), o.get('hash', '')) for o in outputs
                 if isinstance(o, dict)]
        for path, h in items:
            h = h.strip() if h else ''
            if path.endswith('.o') and h:
                parent_to_as_output.setdefault(pid, []).append((path, h))

    if not parent_to_as_output:
        return 0

    linked = 0
    for cmd in raw_cmds:
        cl = cmd.get('command', [])
        if not cl:
            continue
        tool = os.path.basename(str(cl[0]))
        if tool not in _COMPILER_FRONTENDS:
            continue

        # Уже есть output? Пропускаем — цепочка не разорвана.
        existing_out = cmd.get('output', {})
        has_output = (isinstance(existing_out, dict) and existing_out) or \
                     (isinstance(existing_out, list) and existing_out)
        if has_output:
            continue

        pid = cmd.get('parent_id')
        if pid is None:
            continue

        # Находим брата 'as' с тем же parent_id
        as_outputs = parent_to_as_output.get(pid)
        if not as_outputs:
            continue

        # Копируем output .o из as в output cc1plus.
        # Обычно один .o на пару, но копируем все на случай нескольких.
        synth = []
        for path, h in as_outputs:
            synth.append({'path': path, 'hash': h,
                          'synthesized': 'cc1plus_as_link'})
        if synth:
            cmd['output'] = synth
            linked += 1

    return linked


# Компиляторы с ИНТЕГРИРОВАННЫМ ассемблером (clang/clang++): компилируют и
# ассемблируют в одном процессе, отдельного 'as' нет. Для gcc/g++ здесь на
# случай, если трассировщик не поймал выход и на уровне драйвера.
_INTEGRATED_COMPILERS = {'clang', 'clang++', 'gcc', 'g++', 'cc', 'c++'}


def _o_target_from_argv(argv):
    """Возвращает аргумент после -o (или -oX.o), если это .o, иначе None."""
    for i, t in enumerate(argv):
        t = str(t)
        if t == '-o' and i + 1 < len(argv):
            return str(argv[i + 1])
        if t.startswith('-o') and len(t) > 2:
            return t[2:]
    return None


def link_compiler_output_via_consumers(raw_cmds):
    """
    Восстанавливает цепочку компиляции для clang (интегрированный ассемблер).

    Проблема: у clang/clang++ ассемблер встроен, отдельного 'as' нет, а этот
    трассировщик НЕ фиксирует итоговый .o в поле output команды
    'clang++ -c foo.cpp -o foo.o' (output пустой или .tmp). При этом сам .o
    существует и записан как ЗАВИСИМОСТЬ (вход) следующей команды — линковщика
    'ld' (или драйвера clang при линковке). В результате узел .o не имеет
    команды-производителя, обратный обход от бинаря обрывается на .o и .cpp
    ложно попадает в not_compiled/избыточные.

    Решение: строим индекс всех .o, встречающихся во ВХОДАХ любых команд
    (basename -> [(нормпуть, hash)]). Для каждой команды-компилятора с '-c'
    и '-o X.o', у которой output пуст, берём hash того же .o из этого индекса
    и синтезируем ей выход. Сопоставление по basename; при коллизии basename
    уточняем по суффиксу относительного пути '-o' (путь CMakeFiles/<t>.dir/…
    уникален). Это аналог link_compiler_to_assembler, но источник hash —
    потребитель .o, а не брат 'as'.

    Модифицирует raw_cmds на месте. Возвращает количество сшитых команд.
    """
    # Индекс .o из ВХОДОВ (dependencies) всех команд.
    o_index = {}  # basename -> list[(normpath, hash)]
    # Все хеши, которые ХОТЬ КТО-ТО заявляет своим выходом. Нужно для
    # самоограничения: если хеш нужного .o уже кем-то производится, цепочка
    # замкнута по хешу и вмешиваться НЕЛЬЗЯ (иначе замаскируем реальную картину).
    all_output_hashes = set()
    for cmd in raw_cmds:
        deps = cmd.get('dependencies', {})
        if isinstance(deps, dict):
            items = deps.items()
        elif isinstance(deps, list):
            items = [(d.get('path', ''), d.get('hash', '')) for d in deps
                     if isinstance(d, dict)]
        else:
            items = []
        for path, h in items:
            if not path or not path.endswith('.o'):
                continue
            h = h.strip() if h else ''
            if not h:
                continue
            o_index.setdefault(os.path.basename(path), []).append(
                (os.path.normpath(path), h))

        outs = cmd.get('output', {})
        if isinstance(outs, dict):
            out_items = outs.items()
        elif isinstance(outs, list):
            out_items = [(o.get('path', ''), o.get('hash', '')) for o in outs
                         if isinstance(o, dict)]
        else:
            out_items = []
        for _p, h in out_items:
            h = h.strip() if h else ''
            if h:
                all_output_hashes.add(h)

    if not o_index:
        return 0, 0

    linked = 0
    intact = 0
    for cmd in raw_cmds:
        cl = cmd.get('command', [])
        if not cl:
            continue
        tool = os.path.basename(str(cl[0]))
        if tool not in _INTEGRATED_COMPILERS:
            continue
        if '-c' not in [str(x) for x in cl]:
            continue

        # Уже есть непустой output? Цепочка не разорвана — не трогаем.
        existing_out = cmd.get('output', {})
        has_output = (isinstance(existing_out, dict) and existing_out) or \
                     (isinstance(existing_out, list) and existing_out)
        if has_output:
            continue

        ot = _o_target_from_argv(cl)
        if not ot or not ot.endswith('.o'):
            continue

        cands = o_index.get(os.path.basename(ot))
        if not cands:
            continue

        hit = None
        if len(cands) == 1:
            hit = cands[0]
        else:
            rel = os.path.normpath(ot)
            sfx = [c for c in cands
                   if c[0] == rel or c[0].endswith(os.sep + rel)]
            if len(sfx) == 1:
                hit = sfx[0]
            # >1 суффикса — неоднозначно, пропускаем (безопаснее не гадать)

        if hit:
            # САМООГРАНИЧЕНИЕ: если этот хеш уже кем-то производится (например,
            # дочерним 'clang -cc1', который записал объектник во временный файл),
            # цепочка замкнута по хешу — не вмешиваемся.
            if hit[1] in all_output_hashes:
                intact += 1
                continue
            cmd['output'] = [{'path': ot, 'hash': hit[1],
                              'synthesized': 'clang_o_from_consumer'}]
            linked += 1

    return linked, intact


# =============================================================================
# ФУНКЦИИ ДЛЯ ПРОХОДА 2 (КОМПИЛИРУЕМЫЕ) – ТРАНЗИТИВНАЯ ВЕРСИЯ С ПОДДЕРЖКОЙ ХЕШЕЙ
# =============================================================================
def build_transitive_good_commands(raw_cmds, bin_hashes, bin_paths):
    """
    Возвращает множество индексов команд, которые (транзитивно) приводят к bin.
    Команда считается "хорошей", если:
      - хотя бы один её выход есть в bin (напрямую), или
      - хотя бы один её выход используется как вход другой "хорошей" команды.
    Проверка по хешам и нормализованным путям.
    Алгоритм: BFS — O(N), один проход вместо N итераций.
    """
    from collections import deque

    bin_hashes_set = set(bin_hashes)
    bin_paths_set = set(bin_paths)

    # Для каждой команды: множества выходов и входов (пути и хеши)
    cmd_output_paths  = []
    cmd_output_hashes = []
    cmd_input_paths   = []
    cmd_input_hashes  = []

    # Обратный индекс: выход (путь/хеш) -> команды у которых это ВХОД
    # То есть: "кто потребляет этот файл как зависимость"
    out_path_to_consumers  = {}   # norm_path -> set(idx)
    out_hash_to_consumers  = {}   # hash      -> set(idx)

    total_cmds = len(raw_cmds)
    print(_ts() + "   Pass 2: indexing {} commands...".format(total_cmds))

    for idx, cmd in enumerate(raw_cmds):
        progress_log("Pass 2 indexing commands", idx + 1, total_cmds)

        out_paths  = set()
        out_hashes = set()
        in_paths   = set()
        in_hashes  = set()

        # Выходы
        outputs = cmd.get('output', {})
        if isinstance(outputs, dict):
            for path, h in outputs.items():
                if path:
                    out_paths.add(os.path.normpath(path))
                if h:
                    out_hashes.add(h.strip())
        elif isinstance(outputs, list):
            for out in outputs:
                path = out.get('path', '') if isinstance(out, dict) else str(out)
                h    = out.get('hash', '') if isinstance(out, dict) else ''
                if path:
                    out_paths.add(os.path.normpath(path))
                if h:
                    out_hashes.add(h.strip())

        # Входы (dependencies)
        deps = cmd.get('dependencies', {})
        if isinstance(deps, dict):
            for path, h in deps.items():
                if path:
                    in_paths.add(os.path.normpath(path))
                if h:
                    in_hashes.add(h.strip())
        elif isinstance(deps, list):
            for dep in deps:
                path = dep.get('path', '') if isinstance(dep, dict) else str(dep)
                h    = dep.get('hash', '') if isinstance(dep, dict) else ''
                if path:
                    in_paths.add(os.path.normpath(path))
                if h:
                    in_hashes.add(h.strip())

        cmd_output_paths.append(out_paths)
        cmd_output_hashes.append(out_hashes)
        cmd_input_paths.append(in_paths)
        cmd_input_hashes.append(in_hashes)

        # Индекс: выход текущей команды -> команды которые берут его как вход
        # Заполняем по входам текущей команды: если in_path совпадёт с out_path
        # другой команды, та другая команда должна знать что idx её потребляет.
        # Строим это позже за второй проход — сейчас просто сохраняем.

    # Строим обратный индекс: out_path/out_hash -> кто использует как вход
    # (нужен для BFS: когда команда становится "хорошей", мы должны найти
    #  команды чьи ВЫХОДЫ она использует как ВХОДЫ — т.е. её "поставщиков")
    # На самом деле для BFS нужен индекс в другую сторону:
    # вход команды idx -> какие команды производят этот вход (поставщики)
    # Строим: out_path -> set(producer_idx), out_hash -> set(producer_idx)
    out_path_to_producer  = {}   # norm_path -> set(idx) команд-производителей
    out_hash_to_producer  = {}   # hash      -> set(idx) команд-производителей

    # И: out_path/out_hash -> set(consumer_idx) — потребители выхода
    # (нужно чтобы от "хорошей" команды идти к её поставщикам)
    # Поставщик команды idx — команда j, чей выход совпадает с входом idx.
    # Строим через входы:
    in_path_to_cmd  = {}  # norm_path_входа -> set(idx) — команды у которых это вход
    in_hash_to_cmd  = {}  # hash_входа      -> set(idx)

    for idx in range(total_cmds):
        for p in cmd_output_paths[idx]:
            out_path_to_producer.setdefault(p, set()).add(idx)
        for h in cmd_output_hashes[idx]:
            out_hash_to_producer.setdefault(h, set()).add(idx)
        for p in cmd_input_paths[idx]:
            in_path_to_cmd.setdefault(p, set()).add(idx)
        for h in cmd_input_hashes[idx]:
            in_hash_to_cmd.setdefault(h, set()).add(idx)

    # --- BFS ---
    # Семантика: "хорошая" команда — та, чей выход (прямо или транзитивно) попадает в bin.
    # Стартуем с команд у которых выход напрямую в bin.
    # Затем для каждой хорошей команды смотрим: кто производит её входы?
    # Те производители тоже хорошие — добавляем в очередь.

    good_cmds = set()
    queue = deque()

    print(_ts() + "   Pass 2: seeding BFS from bin outputs...")
    for idx in range(total_cmds):
        if (any(p in bin_paths_set  for p in cmd_output_paths[idx]) or
            any(h in bin_hashes_set for h in cmd_output_hashes[idx])):
            good_cmds.add(idx)
            queue.append(idx)

    print(_ts() + "   Pass 2: BFS start — seed size: {}".format(len(good_cmds)))

    processed = 0
    while queue:
        idx = queue.popleft()
        processed += 1
        if processed % 10000 == 0:
            print(_ts() + "   Pass 2: BFS processed {}, good so far: {}, queue: {}".format(
                processed, len(good_cmds), len(queue)))

        # Для каждого входа команды idx ищем поставщиков (команды чей выход = этот вход)
        for in_path in cmd_input_paths[idx]:
            for producer_idx in out_path_to_producer.get(in_path, ()):
                if producer_idx not in good_cmds:
                    good_cmds.add(producer_idx)
                    queue.append(producer_idx)

        for in_hash in cmd_input_hashes[idx]:
            for producer_idx in out_hash_to_producer.get(in_hash, ()):
                if producer_idx not in good_cmds:
                    good_cmds.add(producer_idx)
                    queue.append(producer_idx)

    print(_ts() + "   Pass 2: BFS done — total good commands: {}".format(len(good_cmds)))

    # Освобождаем крупные индексы — они больше не нужны
    del cmd_output_paths, cmd_output_hashes, cmd_input_paths, cmd_input_hashes
    del out_path_to_producer, out_hash_to_producer, in_path_to_cmd, in_hash_to_cmd
    gc.collect()

    return good_cmds


def build_good_compiler_inputs(raw_cmds, compiler_basenames, bin_hashes, bin_paths):
    """
    Возвращает множество ключей (хешей и нормализованных путей) входных файлов,
    которые были использованы в командах компиляторов/линковщиков, транзитивно приводящих к bin.
    """
    # Получаем все "хорошие" команды (не только компиляторы)
    all_good_cmds = build_transitive_good_commands(raw_cmds, bin_hashes, bin_paths)

    # Отфильтровываем команды, которые являются компиляторами (по первому аргументу)
    good_keys = set()
    for idx in all_good_cmds:
        cmd = raw_cmds[idx]
        cmd_list = cmd.get('command', [])
        if not cmd_list:
            continue
        if os.path.basename(cmd_list[0]) not in compiler_basenames:
            continue

        # Добавляем все входные файлы этой команды
        deps = cmd.get('dependencies', {})
        if isinstance(deps, dict):
            for path, h in deps.items():
                path = path.strip()
                h = h.strip()
                if h:
                    good_keys.add(h)
                if path:
                    good_keys.add(os.path.normpath(path))
        elif isinstance(deps, list):
            for dep in deps:
                if isinstance(dep, dict):
                    path = dep.get('path', '').strip()
                    h = dep.get('hash', '').strip()
                else:
                    path = str(dep).strip()
                    h = ''
                if h:
                    good_keys.add(h)
                if path:
                    good_keys.add(os.path.normpath(path))
    return good_keys


# =============================================================================
# ФУНКЦИИ ДЛЯ ПРОХОДА 2 – ОРИГИНАЛЬНЫЙ АНАЛИЗ (оставляем без изменений)
# =============================================================================
def analyze_pass2(direct, parent, redundant, good_compiler_input_keys):
    """Второй проход: оставляет в direct/parent только те компилируемые файлы,
    которые были входами команд с выходами в bin. Остальные перемещает в redundant."""
    direct_out = []
    parent_out = []
    not_compiled = []
    moved_count = 0

    def was_compiled(entry):
        h = entry.get('hash', '')
        p = entry.get('path_norm', entry.get('path', ''))
        # Проверка по хешу
        if h and h in good_compiler_input_keys:
            return True
        # Проверка по нормализованному пути (уже нормализован)
        if p and p in good_compiler_input_keys:
            return True
        return False

    for entry in direct:
        if is_compiled_extension(entry.get('path', '')):
            if not was_compiled(entry):
                # Файл БЫЛ в buildography (он direct), компилировался,
                # но результат не попал в дистрибутив → not_compiled.
                # НЕ добавляем в redundant — эти категории исключают друг друга:
                # redundant-by-hash = файлы которых НЕТ в buildography.
                not_compiled.append({
                    'path': entry['path'],
                    'hash': entry['hash'],
                    'source': 'direct',
                })
                moved_count += 1
                continue
        direct_out.append(entry)

    for entry in parent:
        if is_compiled_extension(entry.get('path', '')):
            if not was_compiled(entry):
                # Файл БЫЛ в buildography (он parent), компилировался,
                # но результат не попал в дистрибутив → not_compiled.
                # НЕ добавляем в redundant (см. комментарий выше).
                not_compiled.append({
                    'path': entry['path'],
                    'hash': entry['hash'],
                    'parent_hash': entry.get('parent_hash', ''),
                    'source': 'parent',
                })
                moved_count += 1
                continue
        parent_out.append(entry)

    print(_ts() + "   Pass 2 done: moved to not_compiled={}".format(moved_count))
    return direct_out, parent_out, redundant, not_compiled


# =============================================================================
# ФУНКЦИИ ДЛЯ ПРОХОДА 3 (ИНТЕРПРЕТИРУЕМЫЕ) – ОПТИМИЗИРОВАННЫЕ (без изменений)
# =============================================================================
def build_interpreted_files_with_cmds(raw_cmds, interpreter_basenames):
    """
    Возвращает (input_files, output_files), где каждый элемент списка — словарь
    с ключами 'path', 'hash', 'path_norm', 'cmd_index'.
    """
    input_files = []
    output_files = []
    seen_input = set()
    seen_output = set()

    total_cmds = len(raw_cmds)
    print(_ts() + "   Pass 3: scanning {} commands for interpreter calls...".format(total_cmds))
    for cmd_idx, cmd in enumerate(raw_cmds):
        progress_log("Pass 3 scanning commands", cmd_idx + 1, total_cmds)
        cmd_list = cmd.get('command', [])
        if not cmd_list or os.path.basename(cmd_list[0]) not in interpreter_basenames:
            continue

        # Входные файлы
        deps = cmd.get('dependencies', {})
        if isinstance(deps, dict):
            for path, h in deps.items():
                path = path.strip()
                h = h.strip()
                if path and is_interpreted_extension(path):
                    norm_path = os.path.normpath(path)
                    key = h if h else norm_path
                    if key not in seen_input:
                        seen_input.add(key)
                        input_files.append({
                            'path': path,
                            'path_norm': norm_path,
                            'hash': h,
                            'cmd_index': cmd_idx
                        })
        elif isinstance(deps, list):
            for dep in deps:
                if isinstance(dep, dict):
                    path = dep.get('path', '').strip()
                    h = dep.get('hash', '').strip()
                else:
                    path = str(dep).strip()
                    h = ''
                if path and is_interpreted_extension(path):
                    norm_path = os.path.normpath(path)
                    key = h if h else norm_path
                    if key not in seen_input:
                        seen_input.add(key)
                        input_files.append({
                            'path': path,
                            'path_norm': norm_path,
                            'hash': h,
                            'cmd_index': cmd_idx
                        })

        # Выходные файлы
        outputs = cmd.get('output', {})
        if isinstance(outputs, dict):
            for path, h in outputs.items():
                path = path.strip()
                h = h.strip()
                if path and is_interpreted_extension(path):
                    norm_path = os.path.normpath(path)
                    key = h if h else norm_path
                    if key not in seen_output:
                        seen_output.add(key)
                        output_files.append({
                            'path': path,
                            'path_norm': norm_path,
                            'hash': h,
                            'cmd_index': cmd_idx
                        })
        elif isinstance(outputs, list):
            for out in outputs:
                if isinstance(out, dict):
                    path = out.get('path', '').strip()
                    h = out.get('hash', '').strip()
                else:
                    path = str(out).strip()
                    h = ''
                if path and is_interpreted_extension(path):
                    norm_path = os.path.normpath(path)
                    key = h if h else norm_path
                    if key not in seen_output:
                        seen_output.add(key)
                        output_files.append({
                            'path': path,
                            'path_norm': norm_path,
                            'hash': h,
                            'cmd_index': cmd_idx
                        })

    return input_files, output_files


def analyze_interpreted(signatures, input_files, output_files, bin_hashes, bin_paths, raw_cmds,
                        all_good_cmds=None, seen_in_trace=None):
    """
    Классифицирует интерпретируемые файлы из signatures на четыре категории:
      - executed:   интерпретируемые файлы (любого языка), которые были входными
                    для команд интерпретаторов — .py под python, .js под qjs и т.п.
                    (добавляется поле "commands" со списком полных команд)
      - compiled:   выходные файлы интерпретаторов + входные любых языков, чьи выходы попали в bin
                    (транзитивно — если выход команды транзитивно попадает в bin)
      - copied:     файлы, присутствующие в bin.json, но не вошедшие в executed/compiled
      - use_untraceable: хеш файла есть в трассе сборки, но ни один
                    интерпретатор его не использовал. Файл при сборке
                    присутствовал, однако доказать его использование нечем —
                    и назвать избыточным тоже нельзя.
      - izb:        остальные (избыточные): хеша нет в трассе вообще
    all_good_cmds — множество индексов команд транзитивно приводящих к bin (из pass2 BFS).
                    Если передано — используется вместо прямой проверки cmd_has_bin_output.
    seen_in_trace — множество хешей интерпретируемых файлов, встречающихся в
                    трассе у ЛЮБОЙ команды. Отделяет "не прослеживается" от
                    "не используется"; если не передано, различие не делается
                    и всё идёт в izb, как было раньше.

    Зачем нужна use_untraceable. Измерено на NPUR.69035-01: 107 файлов .js/.ts
    из состава miv-agent попадали в izb как избыточные. В действительности
    приложение собирается в ОДИН самодостаточный файл на 36 МБ
    (miv-agent-0.9.4.tgz содержит ровно одну запись ./miv-agent), который
    подписывается bsign и уходит в дистрибутив. Исходные файлы вшиты внутрь,
    поэтому по хешу не совпадают ни с чем в bin.json. При этом сборка бандла
    в трассу не попала: у build_miv_agent.sh из содержательных входов только
    собственный текст, а все 18 выходов — журнал Svace. Единственная команда,
    читающая эти файлы, — md5sum из calc_md5. То есть "izb" утверждал
    избыточность, имея на руках лишь отсутствие доказательств использования.
    Это разные вещи, и в отчёте их смешивать нельзя: в списке на удаление
    оказывались файлы, вшитые в поставляемый подписанный артефакт.
    Возвращает кортеж (executed, compiled_used, compiled_unused, copied,
                       use_untraceable, izb).
    """
    # Множества для быстрой проверки
    bin_hashes_set = set(bin_hashes)
    bin_paths_set = set(bin_paths)

    # Строим множества входных и выходных файлов (хеши и нормализованные пути)
    input_hashes = {inp['hash'] for inp in input_files if inp.get('hash')}
    input_paths_norm = {inp['path_norm'] for inp in input_files if inp.get('path_norm')}
    output_hashes = {out['hash'] for out in output_files if out.get('hash')}
    output_paths_norm = {out['path_norm'] for out in output_files if out.get('path_norm')}

    # Словари для быстрого получения индексов команд по хешу/пути
    input_by_hash = {}
    input_by_path = {}
    for inp in input_files:
        h = inp.get('hash')
        if h:
            input_by_hash.setdefault(h, set()).add(inp['cmd_index'])
        p = inp.get('path_norm')
        if p:
            input_by_path.setdefault(p, set()).add(inp['cmd_index'])

    output_by_hash = {}
    output_by_path = {}
    for out in output_files:
        h = out.get('hash')
        if h:
            output_by_hash.setdefault(h, set()).add(out['cmd_index'])
        p = out.get('path_norm')
        if p:
            output_by_path.setdefault(p, set()).add(out['cmd_index'])

    # Для каждой команды определяем попадает ли её выход (транзитивно) в bin.
    # Если передан all_good_cmds из pass2 BFS — используем его (транзитивная проверка).
    # Иначе — прямая проверка выходов команды.
    if all_good_cmds is not None:
        # Транзитивная проверка: команда "хорошая" если её выход транзитивно попадает в bin
        cmd_has_bin_output = [idx in all_good_cmds for idx in range(len(raw_cmds))]
        good_count = sum(1 for x in cmd_has_bin_output if x)
        print(_ts() + "   Pass 3: using transitive good_cmds: {}/{} commands lead to bin".format(
            good_count, len(raw_cmds)))
    else:
        # Прямая проверка — только команды чей выход напрямую в bin
        cmd_has_bin_output = [False] * len(raw_cmds)
        for cmd_idx, cmd in enumerate(raw_cmds):
            outputs = cmd.get('output', {})
            if isinstance(outputs, dict):
                for path, h in outputs.items():
                    path = path.strip()
                    h = h.strip()
                    if (h and h in bin_hashes_set) or (path and os.path.normpath(path) in bin_paths_set):
                        cmd_has_bin_output[cmd_idx] = True
                        break
            elif isinstance(outputs, list):
                for out in outputs:
                    if isinstance(out, dict):
                        path = out.get('path', '').strip()
                        h = out.get('hash', '').strip()
                    else:
                        path = str(out).strip()
                        h = ''
                    if (h and h in bin_hashes_set) or (path and os.path.normpath(path) in bin_paths_set):
                        cmd_has_bin_output[cmd_idx] = True
                        break

    # Преобразуем команды в строки для вывода
    cmd_idx_to_command = {}
    for idx, cmd in enumerate(raw_cmds):
        cmd_list = cmd.get('command', [])
        if cmd_list:
            cmd_idx_to_command[idx] = ' '.join(cmd_list)

    # Предварительная фильтрация интерпретируемых сигнатур
    interpreted_entries = [s for s in signatures if is_interpreted_extension(s.get('path', ''))]
    total_interp = len(interpreted_entries)
    print(_ts() + "   Pass 3: classifying {} interpreted files...".format(total_interp))

    executed = []
    compiled_used = []
    compiled_unused = []
    copied = []
    use_untraceable = []
    izb = []
    seen_in_trace = seen_in_trace or set()
    added_compiled_paths = set()

    # Цикл по интерпретируемым файлам
    for i, entry in enumerate(interpreted_entries):
        progress_log("Pass 3 classifying", i + 1, total_interp)
        path = entry['path']
        h = entry.get('hash', '')
        # Гарантируем что p_norm всегда нормализован
        p_norm = entry.get('path_norm', '') or os.path.normpath(path) if path else ''

        # --- 1. Проверка на copied (файл присутствует в bin.json напрямую) ---
        # Это первый приоритет: если файл скопирован в дистрибутив — он точно не избыточен
        in_bin = (h and h in bin_hashes_set) or (p_norm and p_norm in bin_paths_set)
        if in_bin:
            copied.append({'path': path, 'hash': h})
            continue

        # --- 2. Проверка на выходной файл интерпретатора ---
        is_output = (h and h in output_hashes) or (p_norm and p_norm in output_paths_norm)
        if is_output:
            # Файл сгенерирован интерпретатором, но самого файла нет в bin (уже проверили)
            compiled_unused.append({'path': path, 'hash': h})
            added_compiled_paths.add(p_norm)
            continue

        # --- 3. Проверка на leads_to_bin (входной файл, чей выход попал в bin) ---
        leads_to_bin = False
        cmd_indices = set()
        if h and h in input_hashes:
            cmd_indices = input_by_hash.get(h, set())
        elif p_norm and p_norm in input_paths_norm:
            cmd_indices = input_by_path.get(p_norm, set())
        if cmd_indices:
            for cmd_idx in cmd_indices:
                if cmd_has_bin_output[cmd_idx]:
                    leads_to_bin = True
                    break
        if leads_to_bin:
            compiled_used.append({'path': path, 'hash': h})
            added_compiled_paths.add(p_norm)
            continue

        # --- 4. Проверка на executed (любой интерпретируемый файл, который был
        #        входом команды интерпретатора: .py под python, .js под qjs и т.д.).
        #        Ранее было ограничено только Python — из-за этого .js, исполняемые
        #        через qjs, ложно уходили в избыточные. ---
        was_executed = (h and h in input_hashes) or (p_norm and p_norm in input_paths_norm)
        if was_executed:
            cmd_indices_exec = input_by_hash.get(h, set()) if h else input_by_path.get(p_norm, set())
            commands = []
            for idx in cmd_indices_exec:
                cmd_str = cmd_idx_to_command.get(idx)
                if cmd_str and cmd_str not in commands:
                    commands.append(cmd_str)
            executed.append({'path': path, 'hash': h, 'commands': commands})
            continue

        # --- 4.5. Файл есть в трассе, но его не использовал ни один
        #          интерпретатор. Присутствовал при сборке — значит
        #          утверждать избыточность нельзя (см. комментарий к
        #          use_untraceable в докстринге).
        if h and h in seen_in_trace:
            use_untraceable.append({'path': path, 'hash': h})
            continue

        # --- 5. Остальное — избыточное: хеша нет в трассе вообще ---
        izb.append({'path': path, 'hash': h})

    # Добавляем выходные файлы интерпретатора которых нет в signatures
    # Добавляем выходные файлы интерпретатора которых нет в signatures.
    # ВАЖНО: в этом цикле обрабатываются АРТЕФАКТЫ интерпретаторов (не файлы
    # из src.json). Отчёт compiled_unused должен содержать только исходные
    # файлы из src.json, поэтому сюда артефакты НЕ добавляем — только в
    # compiled_used (если артефакт попал в bin, это подтверждает использование
    # исходника, и такой артефакт полезно видеть в списке использованных).
    for out in output_files:
        path = out.get('path', '')
        p_norm = out.get('path_norm', '')
        if not p_norm:
            continue
        if p_norm not in added_compiled_paths:
            h = out.get('hash', '')
            in_bin = (h and h in bin_hashes_set) or (p_norm and p_norm in bin_paths_set)
            if in_bin:
                compiled_used.append({'path': path, 'hash': h})
                added_compiled_paths.add(p_norm)
            # else: артефакт не в дистрибутиве — НЕ пишем в compiled_unused,
            # так как это не исходный файл из src.json

    print(_ts() + "   Pass 3: executed={}, compiled_used={}, "
                  "compiled_unused={}, copied={}, use_untraceable={}, "
                  "not_used={}".format(
                      len(executed), len(compiled_used), len(compiled_unused),
                      len(copied), len(use_untraceable), len(izb)))
    return (executed, compiled_used, compiled_unused, copied,
            use_untraceable, izb)


# =============================================================================
# АНАЛИЗ — ПРОХОД 1
# =============================================================================
def analyze_pass1(signatures, buildography_hashes):
    direct = []
    parent = []
    redundant = []
    processed = 0
    for entry in signatures:
        path = entry.get('path', '')
        file_hash = entry.get('hash', '')
        parents_chain = entry.get('parents_chain', [])
        if not is_source_file(path):
            continue
        processed += 1
        if processed % 5000 == 0:
            print(_ts() + "   Pass 1: analyzed {} source files...".format(processed))
        if file_hash in buildography_hashes:
            direct.append({'path': path, 'hash': file_hash, 'path_norm': entry.get('path_norm', '')})
            continue
        found_parent = None
        for ph in parents_chain:
            if ph in buildography_hashes:
                found_parent = ph
                break
        if found_parent:
            parent.append({'path': path, 'hash': file_hash, 'parent_hash': found_parent, 'path_norm': entry.get('path_norm', '')})
        else:
            redundant.append({'path': path, 'hash': file_hash, 'path_norm': entry.get('path_norm', '')})
    print(_ts() + "   Pass 1 done: direct={}, parent={}, redundant={}".format(
        len(direct), len(parent), len(redundant)))
    return direct, parent, redundant


# =============================================================================
# ЗАПИСЬ РЕЗУЛЬТАТОВ (JSON и текстовые)
# =============================================================================
def get_try_dir(base_dir, keep=False):
    """
    Возвращает путь к папке try{N} внутри base_dir.

    Режим по умолчанию (keep=False):
      - всегда использует try1
      - удаляет try1 если существует
      - удаляет try2, try3, ... если существуют

    Режим --keep (keep=True):
      - находит следующий свободный tryN
      - ничего не удаляет
    """
    import shutil

    if keep:
        # Находим следующий свободный номер
        n = 1
        while True:
            try_dir = os.path.join(base_dir, "try{}".format(n))
            if not os.path.exists(try_dir):
                return try_dir
            n += 1
    else:
        # Удаляем try1 и все tryN
        n = 1
        while True:
            try_dir = os.path.join(base_dir, "try{}".format(n))
            if os.path.exists(try_dir):
                shutil.rmtree(try_dir)
                print(_ts() + "   Removed old results: {}".format(
                    os.path.basename(try_dir)))
                n += 1
            else:
                break
        # Всегда возвращаем try1
        return os.path.join(base_dir, "try1")


def write_json_result(output_path, category, files):
    result = {
        'category': category,
        'total': len(files),
        'generated_at': datetime.now().isoformat(),
        'files': files,
    }
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    print(_ts() + "   Written {} entries -> {}".format(len(files), output_path))


def write_txt_result(output_path, category_label, entries):
    """
    Универсальная запись txt файла для любой категории.
    Формат: путь<TAB>хеш
    Если entries пуст — файл сохраняется с суффиксом _EMPTY в имени.
    Возвращает True если файл непустой, False если empty.
    """
    seen = set()
    rows = []
    for entry in entries:
        path  = entry.get('path', '').strip()
        hash_ = entry.get('hash', '').strip()
        if not path and not hash_:
            continue
        key = (path, hash_)
        if key in seen:
            continue
        seen.add(key)
        rows.append((path, hash_))
    rows.sort(key=lambda x: x[0])

    # Если пустой — меняем имя файла на *_EMPTY.txt
    if not rows:
        base, ext = os.path.splitext(output_path)
        output_path = base + "_EMPTY" + ext

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("# {}\n".format(category_label))
        f.write("# Generated: {}\n".format(datetime.now().isoformat()))
        f.write("# Total: 0\n")
        f.write("# empty\n")
    if not rows:
        print(_ts() + "   Empty -> {}".format(os.path.basename(output_path)))
        return False

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("# {}\n".format(category_label))
        f.write("# Generated: {}\n".format(datetime.now().isoformat()))
        # Записей и уникальных файлов — разные числа, и разница бывает в разы:
        # один файл дистрибутива попадает в отчёт столько раз, сколько путей
        # у него внутри носителя. Без второй цифры "14 замечаний" читается
        # как 14 файлов, хотя их семь.
        f.write("# Total: {}, уникальных файлов: {}\n".format(
            len(rows), len({r[1] for r in rows if r[1]})))
        f.write("# Format: path<TAB>hash\n")
        f.write("#\n")
        for path, hash_ in rows:
            f.write("{}\t{}\n".format(path, hash_))
    print(_ts() + "   Written {} entries -> {}".format(len(rows), os.path.basename(output_path)))
    return True


def write_redundant_txt(output_path, project_name, redundant, not_compiled, hash_algorithm=''):
    """Записывает объединённый список redundant + not_compiled в формате path<TAB>hash."""
    seen = set()
    rows = []
    for entry in redundant + not_compiled:
        path = entry.get('path', '').strip()
        hash_ = entry.get('hash', '').strip()
        if not path or not hash_:
            continue
        key = (path, hash_)
        if key in seen:
            continue
        seen.add(key)
        rows.append((path, hash_))
    rows.sort(key=lambda x: x[0])

    versioned_path = get_versioned_filepath(output_path)
    if versioned_path != output_path:
        print(_ts() + "   File exists, writing to: {}".format(os.path.basename(versioned_path)))

    with open(versioned_path, 'w', encoding='utf-8') as f:
        f.write("# Redundant files report: {}\n".format(project_name))
        f.write("# Generated: {}\n".format(datetime.now().isoformat()))
        f.write("# Total: {}\n".format(len(rows)))
        f.write("# Hash algorithm: {}\n".format(hash_algorithm if hash_algorithm else '(unknown)'))
        f.write("# Sources: redundant.json + not_compiled.json\n")
        f.write("# Format: path<TAB>hash\n")
        f.write("#\n")
        for path, hash_ in rows:
            f.write("{}\t{}\n".format(path, hash_))
    print(_ts() + "   Written {} entries -> {}".format(len(rows), versioned_path))
    return len(rows)


def write_interpreted_izb_txt(output_path, izb_list):
    """Записывает текстовый файл со списком избыточных интерпретируемых файлов в формате path<TAB>hash."""
    seen = set()
    rows = []
    for entry in izb_list:
        path = entry.get('path', '').strip()
        hash_ = entry.get('hash', '').strip()
        if not path or not hash_:
            continue
        key = (path, hash_)
        if key in seen:
            continue
        seen.add(key)
        rows.append((path, hash_))
    rows.sort(key=lambda x: x[0])

    versioned_path = get_versioned_filepath(output_path)
    if versioned_path != output_path:
        print(_ts() + "   File exists, writing to: {}".format(os.path.basename(versioned_path)))

    with open(versioned_path, 'w', encoding='utf-8') as f:
        f.write("# Interpreted redundant files (izb)\n")
        f.write("# Generated: {}\n".format(datetime.now().isoformat()))
        f.write("# Total: {}\n".format(len(rows)))
        f.write("# Format: path<TAB>hash\n")
        f.write("#\n")
        for path, hash_ in rows:
            f.write("{}\t{}\n".format(path, hash_))
    print(_ts() + "   Written {} entries -> {}".format(len(rows), versioned_path))


def write_interpreted_executed_txt(output_path, executed_list):
    """Записывает текстовый файл со списком выполненных Python-файлов в формате path<TAB>hash (без команд)."""
    seen = set()
    rows = []
    for entry in executed_list:
        path = entry.get('path', '').strip()
        hash_ = entry.get('hash', '').strip()
        if not path or not hash_:
            continue
        key = (path, hash_)
        if key in seen:
            continue
        seen.add(key)
        rows.append((path, hash_))
    rows.sort(key=lambda x: x[0])

    versioned_path = get_versioned_filepath(output_path)
    if versioned_path != output_path:
        print(_ts() + "   File exists, writing to: {}".format(os.path.basename(versioned_path)))

    with open(versioned_path, 'w', encoding='utf-8') as f:
        f.write("# Interpreted executed Python files\n")
        f.write("# Generated: {}\n".format(datetime.now().isoformat()))
        f.write("# Total: {}\n".format(len(rows)))
        f.write("# Format: path<TAB>hash\n")
        f.write("#\n")
        for path, hash_ in rows:
            f.write("{}\t{}\n".format(path, hash_))
    print(_ts() + "   Written {} entries -> {}".format(len(rows), versioned_path))


# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# АНАЛИЗ — ПРОХОД 4: проверка происхождения бинарей дистрибутива
#
# Итеративное расширение графа только для нужных цепочек:
#   1. Первый проход — out_to_deps для bin_hashes
#   2. Находим промежуточные артефакты (dep in output_hashes)
#   3. Повторные проходы — расширяем out_to_deps для промежуточных
#   4. Повторяем пока frontier не пуст
#   5. Классифицируем бинари
#
# Ловит все сценарии включая транзитивную компиляцию внешних исходников.
# RAM: только нужные части графа, не весь граф.
# =============================================================================

def _hash_to_int(h):
    try:
        return int(h, 16) & 0xFFFFFFFFFFFFFFFF if h else None
    except ValueError:
        return None


def _log_memory(label):
    try:
        # --- Память процесса (Python) ---
        with open('/proc/self/status', 'r') as f:
            status = f.read()
        def _get_status_kb(field):
            for line in status.splitlines():
                if line.startswith(field + ':'):
                    return int(line.split()[1])
            return 0
        vmrss  = _get_status_kb('VmRSS')
        vmvirt = _get_status_kb('VmSize')
        vmswap = _get_status_kb('VmSwap')

        # --- Системная память ---
        sys_info = {}
        with open('/proc/meminfo', 'r') as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    key = parts[0].rstrip(':')
                    try:
                        sys_info[key] = int(parts[1])  # в кБ
                    except ValueError:
                        pass

        mem_total     = sys_info.get('MemTotal', 0)
        mem_available = sys_info.get('MemAvailable', 0)
        mem_used      = mem_total - mem_available
        mem_cached    = sys_info.get('Cached', 0) + sys_info.get('Buffers', 0)
        swap_total    = sys_info.get('SwapTotal', 0)
        swap_free     = sys_info.get('SwapFree', 0)
        swap_used     = swap_total - swap_free

        print(_ts() + "   [{label}]".format(label=label))
        print(_ts() + "     Process : RSS={rss:.1f} MB  VIRT={virt:.1f} MB  SWAP={swap:.1f} MB".format(
            rss=vmrss/1024, virt=vmvirt/1024, swap=vmswap/1024))
        print(_ts() + "     System  : used={used:.1f}/{total:.1f} GB  available={avail:.1f} GB  "
              "cache={cache:.1f} GB  swap={swused:.1f}/{swtotal:.1f} GB".format(
            used=mem_used/1024/1024,
            total=mem_total/1024/1024,
            avail=mem_available/1024/1024,
            cache=mem_cached/1024/1024,
            swused=swap_used/1024/1024,
            swtotal=swap_total/1024/1024))

        # Предупреждение если мало свободной памяти
        if mem_available > 0 and mem_available < mem_total * 0.1:
            print(_ts() + "     [WARN] Low memory: {:.1f} GB available ({:.0f}% of total)".format(
                mem_available/1024/1024, mem_available/mem_total*100))

    except Exception as e:
        print(_ts() + "   {}: could not read memory: {}".format(label, e))


# Расширения ресурсных файлов — не являются признаком внешних исходников.
# Если такой файл попадает в зависимости бинаря — это не делает его external_built.
RESOURCE_EXTENSIONS = {
    # Конфигурация и данные
    '.xml', '.properties', '.yaml', '.yml', '.json', '.toml', '.ini', '.cfg',
    '.txt', '.csv', '.tsv',
    # Документация
    '.md', '.rst', '.html', '.htm', '.css', '.adoc',
    # Изображения и медиа
    '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.bmp', '.tiff',
    '.ttf', '.woff', '.woff2', '.eot', '.otf',
    '.mp3', '.mp4', '.wav', '.ogg',
    # Java манифесты и метаданные пакетов
    '.mf', '.sf',       # MANIFEST.MF, signature files
    '.dsp', '.dtd', '.xsd', '.xsl', '.xslt',
    # Скрипты сборки (не исходники)
    '.bat', '.cmd', '.ps1',
    # Прочие ресурсы
    '.sql', '.graphql',
    '.lock',            # package-lock.json, Cargo.lock и т.д.
    '.map',             # source maps
}

SYSTEM_PATH_PREFIXES = (
    '/usr/lib/', '/usr/lib64/', '/lib/', '/lib64/',
    '/usr/include/', '/usr/local/lib/', '/usr/local/include/',
    '/etc/', '/proc/', '/sys/', '/dev/',
    '/usr/share/', '/var/',
    '/tmp/hsperfdata_',  # JVM performance data
    '/run/', '/snap/',
    # Cross-compiler sysroots
    '/usr/arm-linux-gnueabi/', '/usr/arm-linux-gnueabihf/',
    '/usr/aarch64-linux-gnu/', '/usr/mips-linux-gnu/',
    '/usr/mipsel-linux-gnu/', '/usr/powerpc-linux-gnu/',
    '/usr/powerpc64-linux-gnu/', '/usr/powerpc64le-linux-gnu/',
    '/usr/riscv64-linux-gnu/', '/usr/s390x-linux-gnu/',
    '/usr/x86_64-linux-gnu/', '/usr/i686-linux-gnu/',
    '/usr/sparc64-linux-gnu/', '/usr/m68k-linux-gnu/',
    '/usr/sh4-linux-gnu/', '/usr/hppa-linux-gnu/',
    # Linker scripts
    '/usr/lib/ldscripts/',
    # pkgconfig / cmake
    '/usr/lib/pkgconfig/', '/usr/share/pkgconfig/',
    '/usr/lib64/pkgconfig/', '/usr/local/lib/pkgconfig/',
    '/usr/share/cmake/', '/usr/lib/cmake/',
    # Python / Perl stdlib
    '/usr/lib/python', '/usr/lib/python3',
    '/usr/lib/perl', '/usr/lib/perl5',
    # -------------------------------------------------------------------------
    # Каталоги исполняемых файлов сборочной машины.
    #
    # Это инструментарий хоста сборки: компиляторы, cmake, упаковщики, утилиты.
    # Его никто не передаёт как носитель, сверять его по хешу не с чем, и
    # компилятор физически не может быть источником СОДЕРЖИМОГО файла
    # дистрибутива — он его производит, но не является его материалом.
    # Поэтому здесь путь — оправданный и единственный доступный признак.
    #
    # Проверено на трассе NPUR.34018-01: все 235 выходов в этих каталогах —
    # файлы *.dpkg-new, которые создаёт dpkg при установке пакетов сборочной
    # машины. Ни одного файла изделия, staged install в системные каталоги
    # сборка не делает.
    #
    # ВНИМАНИЕ: этот список относится к путям на ХОСТЕ СБОРКИ. Не путать с
    # DISTRIB_SYSTEM_PREFIXES внутри analyze_pass4 — там пути ВНУТРИ
    # дистрибутива, и usr/bin/ в том списке привёл бы к тому, что собственные
    # бинари изделия стали бы считаться системными.
    # -------------------------------------------------------------------------
    '/usr/bin/', '/bin/', '/sbin/', '/usr/sbin/',
    '/usr/local/bin/', '/usr/local/sbin/',
    '/usr/doc/',
    # -------------------------------------------------------------------------
    # ТПО — многоязычный тулчейн сборочной машины (clang/gcc/cmake/JDK/node/
    # Go/Rust/PHP/Bazel), разворачиваемый в /opt/centrem/tpo.
    #
    # Намеренно НЕ '/opt/centrem/' целиком: в /opt/centrem/svvp лежит СВВП, у
    # которого есть инвентаризация, и он должен проверяться ПО ХЕШУ через
    # TRUSTED. Списание СВВП по пути вернуло бы ровно ту дыру, ради закрытия
    # которой всё это и делается: путь сказал бы "взято из СВВП", а что именно
    # оттуда взяли — не проверил бы никто.
    # -------------------------------------------------------------------------
    '/opt/centrem/tpo/',
)

import re as _re
# Паттерн versioned .so: libfoo.so, libfoo.so.1, libfoo.so.1.2, libfoo.so.1.2.3 и т.д.
_SO_VERSIONED_RE = _re.compile(r'\.so(\.\d+)*$')


def _so_base_name(filename):
    """
    Возвращает базовое имя .so без версионного суффикса.
    Примеры:
      libfoo.so.1.2.3  -> libfoo.so
      libfoo.so.1      -> libfoo.so
      libfoo.so        -> libfoo.so
      libfoo.a         -> None (не .so)
    """
    m = _SO_VERSIONED_RE.search(filename)
    if m is None:
        return None
    base = filename[:m.start()] + '.so'
    return base


def _is_system_path(path):
    """Возвращает True если путь относится к системным файлам хоста сборки."""
    return any(path.startswith(pfx) for pfx in SYSTEM_PATH_PREFIXES)


# Каталоги исполняемых файлов хоста — для отдельного reason в filtered_deps
_HOST_BIN_PREFIXES = ('/usr/bin/', '/bin/', '/sbin/', '/usr/sbin/',
                      '/usr/local/bin/', '/usr/local/sbin/')


def _filter_reason(path):
    """
    Основание, по которому зависимость отфильтрована.

    Фильтр по пути всегда грубее фильтра по хешу, поэтому он обязан быть
    видимым в отчёте: иначе через месяц никто не восстановит, что именно он
    съел и почему.
    """
    ext = os.path.splitext(path)[1].lower()
    if ext in ('.h', '.hpp', '.hxx', '.h++', '.hh'):
        return 'header'
    if path.startswith('/opt/centrem/tpo/'):
        return 'build_host_toolchain_tpo'
    if any(path.startswith(pfx) for pfx in _HOST_BIN_PREFIXES):
        return 'build_host_executable'
    return 'system_path'


def _is_allowed_external_dep(dep_path):
    """
    Возвращает True если внешняя зависимость является допустимой и не подозрительной.
    Такие зависимости исключаются из external_deps и не делают бинарь external_built.

    Категории допустимых внешних зависимостей:
      1. Любой .h / .hpp / .hxx — системные заголовочные файлы
      2. Системные .so / .so.N / .a — библиотеки из системных путей
      3. pkgconfig / cmake find-файлы из системных путей
      4. Linker scripts из системных путей
      5. Python/Perl stdlib из системных путей
      6. Любой путь из SYSTEM_PATH_PREFIXES (общий фильтр)
    """
    if not dep_path:
        return False

    # 1. Любой заголовочный файл — всегда допустим
    ext = os.path.splitext(dep_path)[1].lower()
    if ext in ('.h', '.hpp', '.hxx', '.h++', '.hh'):
        return True

    # 2. Системный путь (общий фильтр — покрывает .so, .a, pkgconfig и т.д.)
    if _is_system_path(dep_path):
        return True

    # 3. .so / versioned .so в любом пути — системные библиотеки линковщика
    #    (иногда лежат не в /usr/lib, а в sysroot или build-tree)
    basename = os.path.basename(dep_path)
    if _SO_VERSIONED_RE.search(basename):
        # Разрешаем только если путь выглядит системным или содержит /lib/
        if ('/lib/' in dep_path or '/lib64/' in dep_path or
                '/include/' in dep_path or dep_path.startswith('/usr/') or
                dep_path.startswith('/lib')):
            return True

    # 4. Linker scripts без расширения в системных путях (уже покрыто п.2)
    # 5. .pc / .cmake файлы в системных путях (уже покрыто п.2)

    return False


def _count_cmds(buildography_files):
    total = 0
    for path in buildography_files:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            data = json.load(f, strict=False)
        total += len(data.get('component_commands', []))
    return total


def _collect_output_hashes(buildography_files):
    """
    Быстрый предварительный проход — собирает все output хеши из buildography.
    Нужен чтобы в _scan_pass корректно определять промежуточные артефакты.
    """
    output_hashes = set()
    for file_path in buildography_files:
        with open(file_path, 'rb') as f:
            data = _json_loads(f.read())
        for cmd in data.get('component_commands', []):
            outputs = cmd.get('output', {})
            if isinstance(outputs, dict):
                for _, h in outputs.items():
                    hi = _hash_to_int(h.strip() if h else '')
                    if hi is not None:
                        output_hashes.add(hi)
            elif isinstance(outputs, list):
                for out in outputs:
                    if isinstance(out, dict):
                        hi = _hash_to_int(out.get('hash', '').strip())
                        if hi is not None:
                            output_hashes.add(hi)
        del data
    return output_hashes


def _scan_pass(buildography_files, target_hashes, total_cmds, label,
               compiler_linker_basenames=None,
               src_hashes_int=None, all_output_hashes=None,
               progress_step_pct=10):
    """
    Один проход по buildography.
    Для команд чьи выходы пересекаются с target_hashes —
    собираем их зависимости.

    compiler_linker_basenames — множество имён компиляторов и линкеров из utilities.yaml.
    Если задано, в out_to_deps попадают только зависимости команд-компиляторов/линкеров.

    src_hashes_int, all_output_hashes — если заданы, фильтруем dep прямо здесь:
      - системные пути (/usr/lib/, /usr/include/ и т.д.) → отбрасываем
      - хеши из src.json → отбрасываем (не подозрительные)
      - допустимые внешние (.h, .so из системных путей) → отбрасываем
      - промежуточные артефакты (есть в output buildography) → сохраняем для iter2/3
      - реально подозрительные → сохраняем
    Это снижает размер out_to_deps на порядок и уменьшает RAM.

    Возвращает:
      out_to_deps        : dict {out_hash_int -> [(dep_hash_int, dep_path, dep_hash_str)]}
                           содержит ТОЛЬКО подозрительные dep (не системные, не src, не /tmp/ промежуточные)
      frontier_hashes    : set хешей промежуточных артефактов для следующей итерации
                           (dep которые есть в all_output_hashes — нужны для iter2/3)
      output_hashes_seen : set всех output хешей встреченных в этом проходе
      dep_hashes_seen    : set всех dep хешей встреченных в этом проходе
    """
    out_to_deps        = {}
    frontier_hashes    = set()  # хеши промежуточных артефактов — только int, без путей
    output_hashes_seen = set()
    dep_hashes_seen    = set()

    # Флаг — применять ли фильтрацию dep
    do_filter = src_hashes_int is not None and all_output_hashes is not None

    # Для раннего выхода: отслеживаем сколько целей из frontier уже найдено
    targets_remaining = set(target_hashes)

    # Статистика фильтрации — для отображения прогресса
    stat_deps_total    = 0  # всего dep встречено
    stat_deps_system   = 0  # отброшено: системные пути
    stat_deps_src      = 0  # отброшено: наши исходники
    stat_deps_allowed  = 0  # отброшено: допустимые внешние
    stat_deps_kept     = 0  # сохранено: подозрительные + промежуточные
    stat_cmds_relevant = 0  # команд у которых output в frontier
    _last_stat_report  = 0  # когда последний раз печатали статистику

    processed = 0
    for file_path in buildography_files:
        with open(file_path, 'rb') as f:
            raw = f.read()
        data = _json_loads(raw)
        cmds = data.get('component_commands', [])

        for cmd in cmds:
            processed += 1
            progress_log(label, processed, total_cmds, step_pct=progress_step_pct)

            # Выходы
            out_ints = set()
            outputs = cmd.get('output', {})
            if isinstance(outputs, dict):
                for _, h in outputs.items():
                    hi = _hash_to_int(h.strip() if h else '')
                    if hi is not None:
                        output_hashes_seen.add(hi)
                        out_ints.add(hi)
            elif isinstance(outputs, list):
                for out in outputs:
                    if isinstance(out, dict):
                        hi = _hash_to_int(out.get('hash', '').strip())
                        if hi is not None:
                            output_hashes_seen.add(hi)
                            out_ints.add(hi)

            # Индексируем если выход в target_hashes
            relevant = out_ints & target_hashes
            if not relevant:
                continue

            # Отмечаем найденные цели
            targets_remaining -= relevant

            # Проверяем является ли команда компилятором/линкером
            if compiler_linker_basenames is not None:
                cmd_list = cmd.get('command', [])
                cmd_tool = os.path.basename(cmd_list[0]) if cmd_list else ''
                is_compiler_or_linker = cmd_tool in compiler_linker_basenames
            else:
                is_compiler_or_linker = True

            # Зависимости — с фильтрацией или без
            deps_raw = cmd.get('dependencies', {})
            dep_list = []

            def _process_dep(path, h):
                """Обрабатывает одну dep запись — фильтрует или добавляет."""
                nonlocal stat_deps_total, stat_deps_system, stat_deps_src
                nonlocal stat_deps_allowed, stat_deps_kept
                hi = _hash_to_int(h)
                if hi is None:
                    return
                dep_hashes_seen.add(hi)
                stat_deps_total += 1
                if do_filter:
                    # Системный путь → выбрасываем (не подозрительно)
                    if _is_system_path(path):
                        stat_deps_system += 1
                        return
                    # Наш исходник → выбрасываем (не подозрительно)
                    if hi in src_hashes_int:
                        stat_deps_src += 1
                        return
                    # Ресурсный файл → выбрасываем (не является признаком внешних исходников)
                    _dep_ext = os.path.splitext(path)[1].lower()
                    if _dep_ext in RESOURCE_EXTENSIONS:
                        stat_deps_allowed += 1
                        return
                    # Допустимая внешняя зависимость → выбрасываем
                    if _is_allowed_external_dep(path):
                        stat_deps_allowed += 1
                        return
                    # Промежуточный артефакт (есть в output buildography) →
                    # добавляем только хеш в frontier, путь не храним
                    if hi in all_output_hashes:
                        frontier_hashes.add(hi)
                        stat_deps_kept += 1
                        return
                    # Реально подозрительный → сохраняем полностью
                stat_deps_kept += 1
                dep_list.append((hi, path, h))

            if isinstance(deps_raw, dict):
                for path, h in deps_raw.items():
                    _process_dep(path, h.strip() if h else '')
            elif isinstance(deps_raw, list):
                for dep in deps_raw:
                    if isinstance(dep, dict):
                        _process_dep(dep.get('path', ''), dep.get('hash', '').strip())

            # Индексируем зависимости только для компиляторов/линкеров
            if is_compiler_or_linker:
                stat_cmds_relevant += 1
            if dep_list and is_compiler_or_linker:
                for out_hi in relevant:
                    if out_hi in out_to_deps:
                        out_to_deps[out_hi].extend(dep_list)
                    else:
                        out_to_deps[out_hi] = list(dep_list)

            # Периодически выводим статистику фильтрации (каждые 10% или 10000 команд)
            if do_filter and processed - _last_stat_report >= max(10000, total_cmds // 10):
                _last_stat_report = processed
                kept_pct = (stat_deps_kept * 100 // stat_deps_total) if stat_deps_total else 0
                print(_ts() + "   {} filter stats: cmds={}/{} relevant={} "
                      "deps_total={} system={}% src={}% allowed={}% kept={}% "
                      "out_to_deps_keys={}".format(
                    label, processed, total_cmds, stat_cmds_relevant,
                    stat_deps_total,
                    stat_deps_system * 100 // stat_deps_total if stat_deps_total else 0,
                    stat_deps_src    * 100 // stat_deps_total if stat_deps_total else 0,
                    stat_deps_allowed* 100 // stat_deps_total if stat_deps_total else 0,
                    kept_pct,
                    len(out_to_deps),  # только количество ключей — без итерации по значениям
                ))

        # Ранний выход: все цели frontier найдены — дальше читать незачем
        if not targets_remaining:
            print(_ts() + "   {}: early exit at {}/{} cmds — all {} targets found".format(
                label, processed, total_cmds, len(target_hashes)))
            del data, cmds
            break

        del data, cmds
        gc.collect()

    # Итоговая статистика фильтрации
    if do_filter and stat_deps_total > 0:
        print(_ts() + "   {} filter summary: deps_total={} "
              "system={}% src={}% allowed={}% kept={}% "
              "out_to_deps_keys={} kept_total={}".format(
            label, stat_deps_total,
            stat_deps_system  * 100 // stat_deps_total,
            stat_deps_src     * 100 // stat_deps_total,
            stat_deps_allowed * 100 // stat_deps_total,
            stat_deps_kept    * 100 // stat_deps_total,
            len(out_to_deps),
            stat_deps_kept,  # просто счётчик, не итерация
        ))

    return out_to_deps, frontier_hashes, output_hashes_seen, dep_hashes_seen


# =============================================================================
# ОПРЕДЕЛЕНИЕ КОНТЕЙНЕРОВ ВНЕШНИХ ПАКЕТОВ (deb / pip / npm)
# =============================================================================

# Инструменты → читаемая команда
_PKG_TOOL_TO_CMD = {
    'apt':       'apt download',
    'apt-get':   'apt-get install',
    'apt-cache': 'apt-cache',
    'dpkg':      'dpkg -i',
    'dpkg-deb':  'dpkg-deb',
    'pip':       'pip install',
    'pip2':      'pip install',
    'pip3':      'pip install',
    'npm':       'npm install',
    'yarn':      'yarn add',
    'wget':      'wget',
    'curl':      'curl',
}
for _v in ['pip3.5','pip3.6','pip3.7','pip3.8','pip3.9','pip3.10','pip3.11']:
    _PKG_TOOL_TO_CMD[_v] = 'pip install'

# Инструменты, которые пакет действительно получают или создают. Только они
# могут быть записаны источником пакета в build_package_source_index.
#
# Измерено на NPUR.69035-01: build.sh для воспроизводимости сборки делает
#     find . -exec touch -d @${SOURCE_DATE_EPOCH} {} \;
# по всему каталогу, поэтому touch попадает в трассу со ВСЕМИ 213 пакетами в
# выходах и опережает настоящего производителя. Раньше принимался любой
# инструмент и запоминался первый, и источником пакета оказывался "touch".
_PKG_SOURCE_TOOLS = frozenset([
    'apt', 'apt-get', 'aptitude',
    'pip', 'pip2', 'pip3', 'pip3.5', 'pip3.6', 'pip3.7', 'pip3.8',
    'pip3.9', 'pip3.10', 'pip3.11',
    'npm', 'npx', 'yarn',
    'wget', 'curl',
    'dpkg-deb', 'dpkg-buildpackage', 'dpkg-source', 'debuild', 'dh_builddeb',
    'rpmbuild', 'wheel',
])

# Из них — те, что дают адрес источника. Их основание содержательнее сборки
# или переупаковки, поэтому при конкуренции они побеждают.
_PKG_DOWNLOAD_TOOLS = frozenset(['apt', 'apt-get', 'aptitude',
                                 'wget', 'curl',
                                 'pip', 'pip2', 'pip3'])
_PKG_DOWNLOAD_CMDS = frozenset(['apt download', 'apt-get install',
                                'pip install', 'wget', 'curl'])


def _deb_name_variants(name):
    """
    Варианты имени Debian-пакета, различающиеся кодировкой эпохи версии.

    На диске эпоха записывается как %3a (cpp_4%3a6.3.0-4_amd64.deb), а в
    трассе встречается и двоеточием, и без эпохи вовсе. Без нормализации
    4 пакета из 66 на NPUR.69035-01 не сопоставлялись по имени.
    """
    out = set()
    low = name
    if '%3a' in low or '%3A' in low:
        plain = low.replace('%3a', ':').replace('%3A', ':')
        out.add(plain)
        # и совсем без эпохи: name_4:6.3.0-4_amd64.deb -> name_6.3.0-4_amd64.deb
        if '_' in plain:
            head, rest = plain.split('_', 1)
            if ':' in rest:
                out.add('{}_{}'.format(head, rest.split(':', 1)[1]))
    elif ':' in low:
        out.add(low.replace(':', '%3a'))
        head, _, rest = low.partition('_')
        if ':' in rest:
            out.add('{}_{}'.format(head, rest.split(':', 1)[1]))
    out.discard(name)
    return out


# =============================================================================
# ОПОЗНАНИЕ ОПЕРАЦИИ КОМАНДЫ
# =============================================================================
# Определять инструмент по argv[0] недостаточно. В реальной трассе встречается:
#
#   /usr/bin/python3 /usr/bin/pip3 download --index http://localhost:9000 ...
#   xargs -i wheel pack {}
#   /usr/bin/python3 -c "import sys, setuptools, tokenize; ... 'setup.py' ..."
#   /usr/bin/python3 .../pep517/_in_process.py prepare_metadata_for_build_wheel
#
# В первых двух случаях argv[0] — это python3 и xargs, то есть интерпретатор и
# обвязка; настоящий инструмент стоит дальше. В третьем случае имя собираемого
# пакета и путь к setup.py вообще находятся ВНУТРИ строки после -c и ни одним
# отдельным элементом argv не представлены.
#
# Поэтому опознание идёт по двум каналам: цепочка первых неопционных токенов
# (она покрывает обвязки) и текстовый поиск ключевых слов по всей команде
# (он покрывает тела -c).
# =============================================================================

# Операции, при которых содержимое файла СОЗДАЁТСЯ
_OP_COMPILED  = 'compiled'    # компилятор/ассемблер/линковщик
_OP_BUILT     = 'built'       # сборка пакета из исходников (setup.py, -m build)
# Операции, при которых содержимое ПЕРЕНОСИТСЯ или ПРЕОБРАЗУЕТСЯ
_OP_REPACKED  = 'repacked'    # переупаковка (wheel pack, dpkg-deb -b, tar -c)
_OP_SIGNED    = 'signed'      # подпись готового файла (bsign, gpg)
_OP_EXTRACTED = 'extracted'   # распаковка (unzip, tar -x, dpkg -x)
_OP_COPIED    = 'copied'      # копирование (cp, install, mv)
_OP_TRANSFORMED = 'transformed'  # правка готового бинаря (strip, objcopy, patchelf)
_OP_DOWNLOADED = 'downloaded' # получение по сети (pip download, wget, curl)
_OP_OTHER     = 'other'

# Операции, которые НЕ создают содержимое, а только переносят или правят его.
# Для них происхождение файла надо искать во входе команды.
#
# strip и objcopy попадают сюда по той же причине, что bsign: они меняют байты
# готового файла, значит его хеш не совпадёт ни с одним исходником, и файл
# выглядит "новым" — хотя содержательно это тот же сторонний бинарь.
DERIVING_OPERATIONS = frozenset((_OP_REPACKED, _OP_SIGNED, _OP_EXTRACTED,
                                 _OP_COPIED, _OP_TRANSFORMED))

_OP_TRANSFORM_TOOLS = frozenset((
    'strip', 'objcopy', 'patchelf', 'chrpath', 'genchecksum',
    'llvm-strip', 'llvm-objcopy', 'ranlib', 'llvm-ranlib',
))
_OP_JS_TOOLS = frozenset(('node', 'nodejs', 'npm', 'npx', 'yarn',
                          'webpack', 'rollup', 'esbuild', 'tsc'))

_OP_SIGN_TOOLS    = frozenset(('bsign', 'gpg', 'gpg2', 'gostsum', 'gostsum12'))
_OP_REPACK_TOOLS  = frozenset(('wheel', 'dpkg-deb', 'jar', 'cpack', 'zip'))
_OP_EXTRACT_TOOLS = frozenset(('unzip', 'dpkg-split', 'unxz', 'gunzip', 'bunzip2'))
_OP_COPY_TOOLS    = frozenset(('cp', 'install', 'mv', 'copy', 'rsync'))
_OP_PIP_TOOLS     = frozenset(('pip', 'pip2', 'pip3', 'pip3.5', 'pip3.6', 'pip3.7',
                               'pip3.8', 'pip3.9', 'pip3.10', 'pip3.11'))
_OP_NET_TOOLS     = frozenset(('wget', 'curl'))

# Токены, после которых идёт адрес индекса/репозитория
_OP_URL_OPTS = frozenset(('--index', '--index-url', '-i', '--extra-index-url',
                          '--find-links', '-f'))

_OP_MAX_SCAN = 8000   # ограничение на длину склеенной команды для поиска слов


def _op_head_names(argv, limit=6):
    """
    Базовые имена первых неопционных токенов команды.
    Покрывает обвязки: python3 -> pip3 -> download, xargs -> wheel -> pack.
    """
    names = []
    for tok in argv:
        if not isinstance(tok, str) or not tok or tok.startswith('-'):
            continue
        names.append(os.path.basename(tok))
        if len(names) >= limit:
            break
    return names


def _op_extract_url(argv):
    """Первый http(s)-адрес в команде: явный токен или значение --index и т.п."""
    for i, tok in enumerate(argv):
        if not isinstance(tok, str):
            continue
        if tok.startswith('http://') or tok.startswith('https://'):
            return tok
        if '=' in tok:
            opt, _, val = tok.partition('=')
            if opt in _OP_URL_OPTS and val.startswith(('http://', 'https://')):
                return val
        if tok in _OP_URL_OPTS and i + 1 < len(argv):
            nxt = argv[i + 1]
            if isinstance(nxt, str) and nxt.startswith(('http://', 'https://')):
                return nxt
    return ''


def _op_tar_mode(argv):
    """Для tar: 'create' | 'extract' | '' — по флагам команды."""
    for tok in argv:
        if not isinstance(tok, str) or not tok.startswith('-'):
            continue
        if tok in ('--create',):
            return 'create'
        if tok in ('--extract', '--get'):
            return 'extract'
        if tok.startswith('--'):
            continue
        # Короткие флаги могут быть склеены: -czf, -xvf
        body = tok[1:]
        if 'c' in body:
            return 'create'
        if 'x' in body or 't' in body:
            return 'extract'
    # tar без ведущего дефиса: tar cf / tar xf
    if len(argv) > 1 and isinstance(argv[1], str) and not argv[1].startswith('-'):
        body = argv[1]
        if body and body[0] == 'c':
            return 'create'
        if body and body[0] in ('x', 't'):
            return 'extract'
    return ''


def detect_operation(argv, compiler_basenames=None, linker_basenames=None):
    """
    Определяет род операции команды и уточняющую подробность.

    Возвращает (operation, detail), где operation — одна из констант _OP_*,
    а detail — имя инструмента либо, для загрузки, адрес источника.

    Порядок проверок важен: от самого определённого признака к самому общему.
    Компилятор проверяется первым, потому что это единственная операция,
    которая действительно создаёт содержимое из исходного текста.
    """
    if not argv or not isinstance(argv, (list, tuple)):
        return _OP_OTHER, ''

    names = _op_head_names(argv)
    nameset = set(names)
    text = ' '.join(t for t in argv if isinstance(t, str))[:_OP_MAX_SCAN]

    # 1. Компиляция / сборка объектного кода
    if compiler_basenames and (nameset & set(compiler_basenames)):
        return _OP_COMPILED, (nameset & set(compiler_basenames)).pop()
    if linker_basenames and (nameset & set(linker_basenames)):
        return _OP_COMPILED, (nameset & set(linker_basenames)).pop()

    # 2. Получение по сети
    net = nameset & _OP_NET_TOOLS
    if net:
        return _OP_DOWNLOADED, _op_extract_url(argv) or net.pop()
    pip = nameset & _OP_PIP_TOOLS
    if pip and 'download' in nameset:
        return _OP_DOWNLOADED, _op_extract_url(argv) or 'pip download'
    if ('apt' in nameset or 'apt-get' in nameset) and 'download' in nameset:
        return _OP_DOWNLOADED, 'apt download'

    # 3. Подпись готового файла
    sign = nameset & _OP_SIGN_TOOLS
    if sign:
        tool = sign.pop()
        if tool.startswith('gpg') and not any(
                t in text for t in ('--sign', '--detach-sign', '--clearsign')):
            pass   # gpg без подписи — не считаем операцией подписи
        else:
            return _OP_SIGNED, tool

    # 4. Переупаковка
    if 'wheel' in nameset and 'pack' in nameset:
        return _OP_REPACKED, 'wheel pack'
    if 'dpkg-deb' in nameset and ('-b' in argv or '--build' in argv):
        return _OP_REPACKED, 'dpkg-deb --build'
    if 'tar' in nameset:
        mode = _op_tar_mode(argv)
        if mode == 'create':
            return _OP_REPACKED, 'tar --create'
        if mode == 'extract':
            return _OP_EXTRACTED, 'tar --extract'
    rep = nameset & _OP_REPACK_TOOLS
    if rep:
        return _OP_REPACKED, rep.pop()

    # 5. Сборка пакета из исходников.
    #    Признаки ищем по всему тексту команды, т.к. setup.py и имя пакета
    #    часто находятся внутри строки после -c.
    if '-m' in argv:
        try:
            mi = list(argv).index('-m')
            if mi + 1 < len(argv) and argv[mi + 1] in ('build', 'pip', 'setuptools'):
                if argv[mi + 1] == 'build':
                    return _OP_BUILT, 'python -m build'
                if argv[mi + 1] == 'pip' and 'download' in nameset:
                    return _OP_DOWNLOADED, _op_extract_url(argv) or 'pip download'
                if argv[mi + 1] == 'pip':
                    return _OP_BUILT, 'python -m pip'
        except ValueError:
            pass
    if 'setup.py' in text or 'bdist_wheel' in text or 'sdist' in text:
        return _OP_BUILT, 'setup.py'
    if 'pep517' in text or '_in_process.py' in text:
        return _OP_BUILT, 'pep517'
    if pip and ('install' in nameset or 'wheel' in nameset):
        return _OP_BUILT, 'pip install'

    # 6. Распаковка
    ext = nameset & _OP_EXTRACT_TOOLS
    if ext:
        return _OP_EXTRACTED, ext.pop()
    if 'dpkg' in nameset and any(t in argv for t in ('-x', '-X', '--extract',
                                                    '--unpack', '--fsys-tarfile')):
        return _OP_EXTRACTED, 'dpkg --unpack'
    if 'ar' in nameset and any(isinstance(t, str) and t.startswith('x')
                               for t in argv[1:2]):
        return _OP_EXTRACTED, 'ar x'

    # 7. Правка готового бинаря — меняет байты, но не создаёт содержимое
    tr = nameset & _OP_TRANSFORM_TOOLS
    if tr:
        return _OP_TRANSFORMED, tr.pop()

    # 8. Сборка средствами JS-тулчейна.
    #    node/npm в сборочной трассе, производящие файл дистрибутива, именно
    #    генерируют его (бандл, минификат), поэтому это создание содержимого.
    js = nameset & _OP_JS_TOOLS
    if js:
        return _OP_BUILT, js.pop()

    # 9. Копирование
    cp = nameset & _OP_COPY_TOOLS
    if cp:
        return _OP_COPIED, cp.pop()

    return _OP_OTHER, (names[0] if names else '')


def _detect_package_type(path):
    """
    Определяет тип внешнего пакета по пути файла.
    Возвращает (package_type, container) или (None, None).

    Типы:
      'deb'  — файл внутри .deb_dir/ или .deb/
      'pip'  — файл внутри venv/site-packages/ или .whl_dir/
      'npm'  — файл внутри node_modules/
    """
    parts = path.replace('\\', '/').split('/')

    # --- deb ---
    for i, part in enumerate(parts):
        name = part[:-4] if part.endswith('_dir') else part
        if name.lower().endswith('.deb') or name.lower().endswith('.rpm'):
            return 'deb', name

    # --- pip: venv/site-packages ---
    for i, part in enumerate(parts):
        if part in ('site-packages', 'dist-packages'):
            # container = имя пакета (следующий компонент)
            pkg = parts[i + 1] if i + 1 < len(parts) else 'unknown'
            # убираем __pycache__ и подпапки — берём только имя пакета
            if pkg.startswith('__'):
                pkg = parts[i - 1] if i > 0 else 'unknown'
            return 'pip', pkg

    # --- pip: .whl_dir ---
    for part in parts:
        name = part[:-4] if part.endswith('_dir') else part
        if name.lower().endswith('.whl'):
            return 'pip', name

    # --- npm: node_modules ---
    for i, part in enumerate(parts):
        if part == 'node_modules':
            pkg = parts[i + 1] if i + 1 < len(parts) else 'unknown'
            # scoped packages: @scope/name
            if pkg.startswith('@') and i + 2 < len(parts):
                pkg = pkg + '/' + parts[i + 2]
            return 'npm', pkg

    return None, None


def build_external_package_index(buildography_files):
    """
    Строит индекс: container_name -> {package_type, source, command}
    Ищет в buildography команды apt/pip/npm у которых в output есть
    .deb/.whl файлы — это и есть источник пакета.

    Возвращает dict: container_name (str) -> dict с полями:
      package_type, source (путь откуда скачан), command (читаемая строка)
    """
    import re as _re2
    index = {}  # container_name -> {package_type, source, command}

    for file_path in buildography_files:
        try:
            with open(file_path, 'rb') as f:
                data = _json_loads(f.read())
        except Exception:
            continue

        for cmd in data.get('component_commands', []):
            cmd_list = cmd.get('command', [])
            if not cmd_list:
                continue
            tool = os.path.basename(str(cmd_list[0]))
            # Источником пакета может быть только инструмент, который пакет
            # получает или создаёт (см. комментарий к _PKG_SOURCE_TOOLS).
            if tool not in _PKG_SOURCE_TOOLS:
                continue
            readable_cmd = _PKG_TOOL_TO_CMD.get(tool, tool)

            # Смотрим зависимости — там исходный путь пакета (откуда скачан)
            deps = cmd.get('dependencies', {})
            dep_paths = []
            if isinstance(deps, dict):
                dep_paths = list(deps.keys())
            elif isinstance(deps, list):
                dep_paths = [
                    d.get('path', '') if isinstance(d, dict) else str(d)
                    for d in deps
                ]

            # Смотрим выходы — там конечный путь пакета
            outputs = cmd.get('output', {})
            out_paths = []
            if isinstance(outputs, dict):
                out_paths = list(outputs.keys())
            elif isinstance(outputs, list):
                out_paths = [
                    o.get('path', '') if isinstance(o, dict) else str(o)
                    for o in outputs
                ]

            # Индексируем .deb/.rpm/.whl из выходов
            for out_path in out_paths:
                bn = os.path.basename(out_path)
                bn_lower = bn.lower()
                if (bn_lower.endswith('.deb') or bn_lower.endswith('.rpm') or
                        bn_lower.endswith('.whl')):
                    # Ищем источник в зависимостях (тот же basename)
                    source = ''
                    for dep in dep_paths:
                        if os.path.basename(dep).lower() == bn_lower:
                            source = dep
                            break
                    if not source and dep_paths:
                        # Берём первую зависимость с похожим именем
                        for dep in dep_paths:
                            if bn_lower.split('_')[0] in dep.lower():
                                source = dep
                                break

                    if bn_lower.endswith('.deb') or bn_lower.endswith('.rpm'):
                        pkg_type = 'deb'
                    else:
                        pkg_type = 'pip'

                    rec = {
                        'package_type': pkg_type,
                        'source':       source or out_path,
                        'command':      readable_cmd,
                    }
                    prev = index.get(bn)
                    if prev is None:
                        index[bn] = rec
                    elif (tool in _PKG_DOWNLOAD_TOOLS and
                          prev.get('command') not in _PKG_DOWNLOAD_CMDS):
                        # Загрузка содержательнее сборки: у неё есть адрес
                        # источника. Поэтому перебивает ранее найденное.
                        index[bn] = rec

        del data

    return index


def classify_external_package_content(untraced_external, buildography_files):
    """
    Из списка untraced_external выделяет файлы внутри известных пакетов
    (deb/pip/npm) в отдельную категорию external_package_content.

    Возвращает:
      pkg_content   — list записей для external_package_content
      remaining     — list записей которые остаются в untraced_external
    """
    print(_ts() + "   Building external package index from buildography...")
    pkg_index = build_external_package_index(buildography_files)
    print(_ts() + "   Package index: {} known containers".format(len(pkg_index)))

    pkg_content = []
    remaining   = []

    for entry in untraced_external:
        path = entry.get('path', '')
        pkg_type, container = _detect_package_type(path)

        if pkg_type is None:
            remaining.append(entry)
            continue

        # Ищем источник в индексе
        # Для deb: container это имя .deb файла — ищем напрямую
        # Для pip/npm: container это имя пакета — ищем по подстроке
        pkg_info = pkg_index.get(container, {})
        if not pkg_info and pkg_type == 'pip':
            # Для pip пакетов ищем .whl в индексе по имени пакета
            for key, val in pkg_index.items():
                if container.lower() in key.lower() and val['package_type'] == 'pip':
                    pkg_info = val
                    break

        new_entry = dict(entry)
        new_entry['package_type'] = pkg_type
        new_entry['container']    = container
        if pkg_info:
            new_entry['source']  = pkg_info.get('source', '')
            new_entry['command'] = pkg_info.get('command', pkg_type)
        else:
            new_entry['source']  = ''
            new_entry['command'] = pkg_type
        pkg_content.append(new_entry)

    return pkg_content, remaining


def write_external_package_content_json(output_path, entries):
    """
    Записывает external_package_content.json сгруппированный по пакетам.
    """
    # Группируем по (package_type, container)
    groups = {}
    for e in entries:
        key = (e.get('package_type', ''), e.get('container', ''))
        groups.setdefault(key, []).append(e)

    packages = []
    for (pkg_type, container), group in sorted(groups.items()):
        # Берём source и command из первой записи
        source  = group[0].get('source', '')
        command = group[0].get('command', pkg_type)
        files   = [{'path': e.get('path',''), 'hash': e.get('hash','')}
                   for e in group]
        packages.append({
            'package_type': pkg_type,
            'container':    container,
            'source':       source,
            'command':      command,
            'files_count':  len(files),
            'files':        files,
        })

    result = {
        'category':       'external_package_content',
        'generated':      datetime.now().isoformat(),
        'total_files':    len(entries),
        'total_packages': len(packages),
        'packages':       packages,
    }

    versioned_path = get_versioned_filepath(output_path)
    if versioned_path != output_path:
        print(_ts() + "   File exists, writing to: {}".format(
            os.path.basename(versioned_path)))

    with open(versioned_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(_ts() + "   Written {} files in {} packages -> {}".format(
        len(entries), len(packages), versioned_path))


def write_external_package_content_txt(output_path, entries):
    """
    Записывает external_package_content.txt.
    Формат: path<TAB>hash
    """
    rows = sorted(entries, key=lambda e: e.get('path', ''))

    versioned_path = get_versioned_filepath(output_path)
    if versioned_path != output_path:
        print(_ts() + "   File exists, writing to: {}".format(
            os.path.basename(versioned_path)))

    with open(versioned_path, 'w', encoding='utf-8') as f:
        f.write("# external_package_content\n")
        f.write("# Generated: {}\n".format(datetime.now().isoformat()))
        f.write("# Total files: {}, Total packages: {}\n".format(
            len(entries),
            len(set((e.get('package_type',''), e.get('container',''))
                    for e in entries))))
        f.write("# Format: path<TAB>hash\n")
        f.write("#\n")
        for e in rows:
            f.write("{}\t{}\n".format(
                e.get('path', ''),
                e.get('hash', ''),
            ))
    print(_ts() + "   Written {} entries -> {}".format(len(rows), versioned_path))


def build_apt_download_hashes(buildography_files):
    """
    Сканирует buildography и собирает имена .deb файлов из output команд apt download.
    Логика:
      1. Видим apt download — фиксируем имена .deb из output
      2. Если путь бинаря в bin.json содержит такое имя → external_package_content
      3. Если такой хеш есть в src.json → тоже external_package_content
         (он уже учтён в binaries_in_src, не надо писать в untraced_from_src)
    Возвращает:
      apt_deb_names : set имён .deb файлов {'libgcc-6-dev_...deb', ...}
    """
    apt_deb_names = set()

    for file_path in buildography_files:
        try:
            with open(file_path, 'rb') as f:
                data = _json_loads(f.read())
        except Exception as e:
            print(_ts() + "   build_apt_download_hashes: skip {}: {}".format(
                os.path.basename(file_path), e))
            continue

        for cmd in data.get('component_commands', []):
            cmd_list = cmd.get('command', [])
            if not cmd_list:
                continue
            tool = os.path.basename(str(cmd_list[0]))
            # И apt, и apt-get: build.sh изделий вызывает
            #     apt-get download ${pkg}
            # а принимался только apt, из-за чего индекс оставался пустым и
            # нулевой фильтр Прохода 4 не срабатывал ни разу.
            # detect_operation оба варианта знает — выравниваем.
            if tool not in ('apt', 'apt-get'):
                continue
            if 'download' not in [str(x) for x in cmd_list[1:3]]:
                continue

            # Это apt download — берём все .deb из output
            outputs = cmd.get('output', [])
            if isinstance(outputs, dict):
                items = [{'path': p, 'hash': h} for p, h in outputs.items()]
            elif isinstance(outputs, list):
                items = outputs
            else:
                items = []

            for out in items:
                if isinstance(out, dict):
                    path = out.get('path', '')
                else:
                    continue
                bn = os.path.basename(path)
                if bn.lower().endswith('.deb'):
                    apt_deb_names.add(bn)
                    apt_deb_names.update(_deb_name_variants(bn))

        del data

    print(_ts() + "   apt download index: {} .deb packages known".format(len(apt_deb_names)))
    return apt_deb_names


# =============================================================================
# ОПРЕДЕЛЕНИЕ ПРОИСХОЖДЕНИЯ ФАЙЛА ДИСТРИБУТИВА
# =============================================================================
# Задача: для файла, который текущая логика относит к compiled_from_src,
# установить ТЕРМИНАЛ его цепочки происхождения — то есть файл, из содержимого
# которого он в конечном счёте получен.
#
# Зачем это нужно. Категория compiled_from_src — это else-ветка Pass 4. Файл
# попадает в неё при трёх условиях: его хеш является выходом прослеженной
# команды; среди переживших фильтрацию зависимостей нет подозрительных
# внешних; хеша нет в src.json. Участие исходного текста изделия не
# проверяется ни на одном шаге, поэтому название категории утверждает больше,
# чем код измеряет.
#
# Измерения на реальном изделии (NPUR.34018-01, 124 уникальных файла):
#   wheel pack       53   переупаковка распакованного чужого колеса
#   python -m build  27   сборка чужого пакета из чужого sdist
#   bsign            22   ГОСТ-подпись готового ELF внутри .deb
#   unzip             6   побайтовая распаковка из архива
#   pip install       5
#   dpkg              4
#   node              4
#   tar               2
#   pip download      1
# Ни одного компилятора. То есть ни один из этих файлов не собран из исходных
# текстов изделия, хотя все они так помечены.
#
# Происхождение (ОСЬ 1, она же категория):
#   product_src       — терминал в src.json изделия
#   approved_source   — терминал на согласованном носителе (TRUSTED), по хешу
#   download          — терминал на сетевой загрузке, с адресом источника
#   unresolved        — терминал не опознан
#
# Операция (ОСЬ 2, атрибут записи, не категория):
#   compiled / built / repacked / signed / extracted / copied / transformed /
#   downloaded / other — см. detect_operation.
# =============================================================================

ORIGIN_PRODUCT_SRC = 'product_src'
ORIGIN_APPROVED    = 'approved_source'
ORIGIN_DOWNLOAD    = 'download'
ORIGIN_UNRESOLVED  = 'unresolved'

# Максимум зависимостей, разбираемых у одной команды. Защита от "жирных"
# команд: у распаковщиков и линковщиков в трассе встречаются списки на
# сотни тысяч записей, и без ограничения проход по ним съедает память.
_ORIGIN_MAX_DEPS_PER_CMD = 3000
# Максимум хешей во фронтире одного раунда
_ORIGIN_MAX_FRONTIER = 20000
# Сколько производящих команд хранить на один хеш.
# Больше одной нужно потому, что файл копируют, подписывают и переупаковывают,
# а также из-за ошибочной привязки хеша к постороннему пути в трассе
# (см. collect_producers). Восьми хватает с большим запасом: на NPUR.34018-01
# максимум было четыре.
_ORIGIN_MAX_PRODUCERS = 8
# Предел числа раундов обратного обхода.
#
# Это ПРЕДОХРАНИТЕЛЬ, а не план: цикл останавливается сам, как только фронтир
# пуст, и лишние раунды просто не выполняются. Поэтому высокое значение ничего
# не стоит, а низкое молча портит результат.
#
# Измерено на NPUR.34018-01: при лимите 3 оставалось 88 неразрешённых, при
# лимите 8 — 52, причём обход вставал сам на пятом раунде. То есть прежнее
# значение 3 (взятое по аналогии с MAX_ITERATIONS в Pass 4, без измерения)
# обрезало работающие цепочки.
#
# Переопределяется переменной окружения ORIGIN_ROUNDS.
try:
    _ORIGIN_MAX_ROUNDS = max(1, int(os.environ.get('ORIGIN_ROUNDS', '20')))
except ValueError:
    _ORIGIN_MAX_ROUNDS = 20


def _iter_cmd_pairs(section):
    """
    Обходит dependencies/output команды, которые в buildography встречаются
    в двух форматах: dict {путь: хеш} и list [{path:..., hash:...}].
    Отдаёт пары (путь, хеш).
    """
    if isinstance(section, dict):
        for p, h in section.items():
            yield p, (h.strip() if isinstance(h, str) else '')
    elif isinstance(section, list):
        for item in section:
            if isinstance(item, dict):
                yield (item.get('path', ''),
                       (item.get('hash') or '').strip())


def collect_producers(buildography_files, target_hashes,
                      compiler_basenames=None, linker_basenames=None,
                      max_deps=_ORIGIN_MAX_DEPS_PER_CMD,
                      max_producers=_ORIGIN_MAX_PRODUCERS,
                      with_siblings=True):
    """
    Один проход по buildography. Для каждого хеша из target_hashes собирает
    ВСЕ команды, у которых этот хеш стоит в output. Возвращает
      { hash: [ {operation, detail, cmd_id, argv, out_paths, deps}, ... ] }

    Почему все, а не первая. Один и тот же хеш законно встречается в выходах
    нескольких команд: файл копируют, подписывают, переупаковывают, и копия
    сохраняет содержимое. Кроме того, в трассе встречается ОШИБОЧНАЯ привязка
    хеша к постороннему пути — проверено на NPUR.34018-01, где хеш колеса
    pykerberos приписан файлу документации Pillow:

        hash f1e02420... ->
            /tmp/ZM1202.BFB/pypi/.../pykerberos-1.2.1-...whl      (3 копии)
            /tmp/build-via-sdist-o6ha__62/Pillow-7.2.0/docs/releasenotes/3.1.0.rst

    Для ГОСТ-хеша совпадение содержимого .rst и .whl невозможно, то есть это
    дефект трассы. Выбор ПЕРВОГО производителя приводил к тому, что обход
    уходил по входам чужой команды (сборки Pillow) и терял настоящую цепочку
    pykerberos, из-за чего 48 файлов оказывались в "происхождение не
    установлено" без всяких оснований.

    Храня всех производителей и проверяя входы каждого, мы перестаём зависеть
    от того, какой из них попался первым.

    Системные зависимости отбрасываются сразу — они составляют подавляющую
    часть списка (libc, locale-archive, gconv-modules и прочие чтения
    динамического загрузчика) и для происхождения бесполезны.
    """
    if not target_hashes:
        return {}

    producers = {}
    targets = set(target_hashes)

    for file_path in buildography_files:
        with open(file_path, 'rb') as f:
            data = _json_loads(f.read())
        for cmd in data.get('component_commands', []):
            hits = {}
            for p, h in _iter_cmd_pairs(cmd.get('output', {})):
                if h and h in targets:
                    hits.setdefault(h, []).append(p)
            if not hits:
                continue
            # Пропускаем команду, если по всем её попаданиям уже набран лимит
            if all(len(producers.get(h, ())) >= max_producers for h in hits):
                continue

            argv = cmd.get('command') or []
            operation, detail = detect_operation(
                argv, compiler_basenames, linker_basenames)

            deps = []
            if operation == _OP_DOWNLOADED:
                # Загрузка — терминал, обходить дальше по её входам не надо.
                # Но сам АДРЕС источника лежит именно во входах: apt копирует
                # пакет из репозитория, и копия побайтно совпадает с
                # оригиналом, поэтому у входа тот же хеш, что у выхода.
                # Берём только такие входы — для отчёта нужен путь, иначе в
                # основании стоит безадресное "apt download". На
                # NPUR.69035-01 это /opt/astra-repo/dev/pool/main/...
                for dp, dh in _iter_cmd_pairs(cmd.get('dependencies', {})):
                    if dh and dh in target_hashes:
                        deps.append((dp, dh))
                        if len(deps) >= 4:
                            break
            else:
                n = 0
                for dp, dh in _iter_cmd_pairs(cmd.get('dependencies', {})):
                    if not dh:
                        continue
                    # Отбрасываем ТОЛЬКО файлы хоста сборки по абсолютному
                    # пути. _is_allowed_external_dep здесь намеренно НЕ
                    # применяется: он отвечает на другой вопрос ("является ли
                    # зависимость подозрительной для external_built") и для
                    # этого разрешает любой .so, в пути которого есть /lib/.
                    # Для происхождения это гибельно — вход bsign вида
                    # ./usr/lib/apache2/modules/mod_x.so, то есть ровно тот
                    # файл, из которого получен подписанный, отбрасывался бы
                    # как "системная библиотека", и терминал цепочки терялся.
                    if _is_system_path(dp):
                        continue
                    deps.append((dp, dh))
                    n += 1
                    if n >= max_deps:
                        break

            argv_short = ' '.join(t for t in argv if isinstance(t, str))[:300]
            for h, out_paths in hits.items():
                lst = producers.setdefault(h, [])
                if len(lst) >= max_producers:
                    continue
                lst.append({
                    'operation': operation,
                    'detail':    detail,
                    'cmd_id':    cmd.get('id'),
                    'parent_id': cmd.get('parent_id'),
                    'argv':      argv_short,
                    'out_paths': out_paths,
                    'deps':      deps,
                })
        del data

    if with_siblings:
        _fill_sibling_deps(buildography_files, producers, max_deps)

    return producers


def _fill_sibling_deps(buildography_files, producers, max_deps):
    """
    Добирает входы у БРАТЬЕВ по parent_id тем производителям, у которых своих
    содержательных входов нет.

    Зачем. При компиляции с флагом -pipe вызов gcc раскладывается на два
    процесса: cc1 читает исходный текст и пишет ассемблерный код в пайп, а as
    читает его со стандартного ввода и создаёт объектный файл. Промежуточного
    .s не существует. В итоге у cc1 нет выходов, а у as нет входов — он,
    будучи производителем .o, не знает ни одного содержательного файла.
    Обратный обход упирается в него и не доходит до исходника.

    Проверено на NPUR.34018-01, модуль apache mod_lbmethod_bymaclabel:

        id 34713  x86_64-linux-gnu-gcc -pipe -g -O2 ...
          +- id 34714  cc1   : 209 входов, среди них mod_lbmethod_bymaclabel.c
          +- id 34715  as    : 10 входов, все системные, ни одного .s

    cc1 и as — братья под одним gcc, то есть одна компиляция, разложенная на
    процессы. Поэтому при пустых входах берём входы братьев и родителя.

    Правило общее, а не заплатка под -pipe: оно закрывает любой случай, когда
    вызов компилятора разложен на несколько процессов и содержательные входы
    оказались не у того из них, кто создал файл.
    """
    need = set()
    for plist in producers.values():
        for pr in plist:
            if not pr['deps'] and pr.get('parent_id') is not None:
                need.add(pr['parent_id'])
    if not need:
        return

    sib = {}
    for file_path in buildography_files:
        with open(file_path, 'rb') as f:
            data = _json_loads(f.read())
        for cmd in data.get('component_commands', []):
            pid = cmd.get('parent_id')
            cid = cmd.get('id')
            key = pid if pid in need else (cid if cid in need else None)
            if key is None:
                continue
            bucket = sib.setdefault(key, [])
            if len(bucket) >= max_deps:
                continue
            for dp, dh in _iter_cmd_pairs(cmd.get('dependencies', {})):
                if not dh or _is_system_path(dp):
                    continue
                bucket.append((dp, dh))
                if len(bucket) >= max_deps:
                    break
        del data

    n = 0
    for plist in producers.values():
        for pr in plist:
            if not pr['deps']:
                got = sib.get(pr.get('parent_id'))
                if got:
                    pr['sibling_deps'] = got
                    n += 1
    if n:
        print(_ts() + "   Origin: добрано входов у братьев для {} "
                      "производителей".format(n))


def _pick_main_producer(plist, target_path):
    """
    Выбирает "главного" производителя из списка — того, чьё имя выходного
    файла совпадает с именем искомого.

    Нужен только для полей operation/operation_cmd в отчёте: происхождение
    всё равно ищется по входам ВСЕХ производителей. Но подпись "чем сделан"
    должна называть настоящую команду, а не ту, что попалась первой из-за
    ошибочной привязки хеша.
    """
    if not plist:
        return {}
    want = os.path.basename(target_path or '')
    if want:
        for pr in plist:
            for op in pr.get('out_paths', ()):
                if os.path.basename(op) == want:
                    return pr
    return plist[0]


# Расширения, по которым компонент пути опознаётся как архив/пакет.
# Порядок важен только для составных: .tar.gz проверяется до .gz.
_TRUSTED_ARCHIVE_EXTS = (
    '.tar.gz', '.tar.bz2', '.tar.xz', '.tar.zst', '.tar.lz4', '.tar.lzma',
    '.tgz', '.tbz2', '.txz',
    '.iso', '.deb', '.rpm', '.whl', '.egg', '.jar', '.war', '.ear',
    '.tar', '.zip', '.gz', '.xz', '.bz2', '.zst', '.7z', '.cab', '.apk',
    '.nupkg', '.crate', '.gem',
)


def _trusted_container(trusted_path):
    """
    Из пути внутри согласованного носителя достаёт БЛИЖАЙШИЙ архив/пакет —
    то, что имеет смысл называть основанием доверия.

      TRUSTED.GENERAL/src/svvp.iso/НПУР.../pypi-cache/distlib/distlib-0.3.4.zip/distlib/t64-arm.exe
        -> svvp.iso/НПУР.../pypi-cache/distlib/distlib-0.3.4.zip

    Архив опознаётся ПО РАСШИРЕНИЮ компонента пути, а не по суффиксу
    "_dir", которого в этих путях нет: generate_json.sh вырезает его при
    записи json (get_virtual_path, подстановка "${virtual_path//_dir\\//\\/}").
    Поиск по "_dir/" не находил ничего и возвращал путь целиком — уникальный
    для каждого файла. На СВВП это давало 1 389 343 различных "контейнера"
    при 1 389 341 хеше, то есть интернирование не работало вовсе и индекс
    занимал 816 МБ вместо расчётных 370; вдобавок в отчёте дублировался
    префикс носителя.

    Берётся ПОСЛЕДНИЙ архивный компонент, а не первый. Вложенность на
    реальном носителе глубокая (образ -> каталог изделия -> архив пакета
    -> содержимое), и по первому в основание всегда попадал бы сам образ,
    что для атрибуции бесполезно: надо знать, какой именно пакет внутри.

    Если архива в пути нет, возвращается каталог файла — он всё равно
    гораздо менее разнообразен, чем сами файлы, и интернирование работает.
    """
    if not trusted_path:
        return ''

    parts = trusted_path.split('/')
    # Префикс "<носитель>/<src|bin>/" в основание не входит
    start = 2 if len(parts) > 2 else 0

    last = -1
    for i in range(start, len(parts) - 1):      # последний компонент — файл
        low = parts[i].lower()
        for ext in _TRUSTED_ARCHIVE_EXTS:
            if low.endswith(ext):
                last = i
                break

    if last >= 0:
        return '/'.join(parts[start:last + 1])
    return '/'.join(parts[start:-1]) if len(parts) > start + 1 else trusted_path


_CONTAINER_PKG_EXTS = ('.deb', '.rpm', '.whl', '.egg', '.jar', '.apk',
                       '.nupkg', '.crate', '.gem')


def resolve_origins(entries, src_hashes, trusted, buildography_files,
                    compiler_basenames=None, linker_basenames=None,
                    max_rounds=_ORIGIN_MAX_ROUNDS,
                    bin_hash_to_path=None):
    """
    Для списка записей (dict с полями path/hash) устанавливает происхождение.

    trusted — TrustedIndex: обращение по строковому хешу, как к dict,
    значение (label, kind, container), где container — архив-основание.

    В каждую запись добавляются поля:
      operation      — род операции производящей команды
      operation_cmd  — укороченная командная строка
      origin         — одна из ORIGIN_*
      origin_detail  — основание: путь в src.json / носитель+путь / URL / причина
      origin_chain   — краткая цепочка операций от файла к терминалу

    Возвращает (product_src, approved, download, unresolved) — четыре списка
    тех же самых записей, разложенные по происхождению.
    """
    def _deps_of(pr):
        """
        Входы производителя: свои, а при их отсутствии — добранные у братьев
        по parent_id (см. _fill_sibling_deps, случай компиляции с -pipe).
        """
        return pr['deps'] or pr.get('sibling_deps') or []

    print(_ts() + "   Origin: входных записей {}".format(len(entries)))

    # Группируем записи по хешу — один хеш часто лежит по нескольким путям
    by_hash = {}
    for e in entries:
        h = (e.get('hash') or '').strip()
        by_hash.setdefault(h, []).append(e)
    print(_ts() + "   Origin: уникальных хешей {}".format(len(by_hash)))

    verdict = {}    # hash -> (origin, detail)
    chains  = {}    # hash -> [строки цепочки]
    # hash -> характерный путь. Нужен, чтобы при нескольких производителях
    # одного хеша выбрать того, чей выходной файл называется так же.
    h_to_path = {}
    for h, elist in by_hash.items():
        if elist:
            h_to_path[h] = elist[0].get('path', '')

    def _set(h, origin, detail):
        verdict[h] = (origin, detail)

    # --- Раунд 0: прямая сверка самого файла -------------------------------
    for h in by_hash:
        if not h:
            _set(h, ORIGIN_UNRESOLVED, 'пустой хеш')
            continue
        if h in src_hashes:
            _set(h, ORIGIN_PRODUCT_SRC, 'хеш файла есть в src.json')
        elif h in trusted:
            label, kind, container = trusted[h]
            _set(h, ORIGIN_APPROVED,
                 '{}/{}: {}'.format(label, kind, container))

    # --- Раунды 1..N: обратный обход ---------------------------------------
    # dependents[x] — множество исходных хешей, судьба которых зависит от x
    dependents = {}
    frontier = set(h for h in by_hash if h not in verdict)
    for h in frontier:
        dependents[h] = {h}
    seen = set(frontier)

    rnd = 0
    while frontier and rnd < max_rounds:
        rnd += 1
        print(_ts() + "   Origin: раунд {}, во фронтире {} хешей".format(
            rnd, len(frontier)))
        producers = collect_producers(
            buildography_files, frontier,
            compiler_basenames, linker_basenames)
        print(_ts() + "   Origin: раунд {}, найдено производителей {}".format(
            rnd, len(producers)))

        next_frontier = set()
        for h in frontier:
            roots = dependents.get(h, set())
            unresolved_roots = [r for r in roots if r not in verdict]
            if not unresolved_roots:
                continue

            plist = producers.get(h)
            if not plist:
                for r in unresolved_roots:
                    if r == h:
                        _set(r, ORIGIN_UNRESOLVED,
                             'производящая команда в трассе не найдена')
                continue

            # Шаг цепочки подписываем главным производителем — тем, чьё имя
            # выходного файла совпадает с искомым. Иначе в цепочке появится
            # чужая команда, попавшая сюда по ошибочной привязке хеша.
            main = _pick_main_producer(plist, h_to_path.get(h, ''))
            step = '{}({})'.format(main['operation'], main['detail'] or '?')
            for r in unresolved_roots:
                chains.setdefault(r, []).append(step)

            # Загрузка — терминал сама по себе. Достаточно, чтобы ЛЮБОЙ из
            # производителей оказался загрузкой.
            dl = next((pr for pr in plist
                       if pr['operation'] == _OP_DOWNLOADED), None)
            if dl is not None:
                for r in unresolved_roots:
                    _set(r, ORIGIN_DOWNLOAD,
                         dl['detail'] or 'сетевая загрузка')
                continue

            # Ищем терминал среди входов ВСЕХ производителей: сначала
            # согласованный носитель, затем исходные тексты изделия.
            hit_trusted = None
            hit_src     = None
            for pr in plist:
                for dp, dh in _deps_of(pr):
                    if dh in trusted:
                        hit_trusted = (dp, dh)
                        break
                if hit_trusted is not None:
                    break
            if hit_trusted is None:
                for pr in plist:
                    for dp, dh in _deps_of(pr):
                        if dh in src_hashes:
                            hit_src = (dp, dh)
                            break
                    if hit_src is not None:
                        break

            if hit_trusted is not None:
                label, kind, container = trusted[hit_trusted[1]]
                detail = '{}/{}: {}'.format(label, kind, container)
                for r in unresolved_roots:
                    _set(r, ORIGIN_APPROVED, detail)
                continue
            if hit_src is not None:
                for r in unresolved_roots:
                    _set(r, ORIGIN_PRODUCT_SRC,
                         'вход команды есть в src.json: {}'.format(
                             hit_src[0][:120]))
                continue

            # Ни у одного производителя нет неотфильтрованных входов —
            # идти дальше некуда. Формулировка различается по тому, создаёт
            # операция содержимое или только переносит его: во втором случае
            # отсутствие входа это пробел трассы, в первом может быть нормой.
            all_deps = [d for pr in plist for d in _deps_of(pr)]
            if not all_deps:
                if main['operation'] in DERIVING_OPERATIONS:
                    why = ('операция {} только переносит содержимое, но её '
                           'вход в трассе отсутствует'.format(main['operation']))
                else:
                    why = ('у производящей команды ({}) нет неотфильтрованных '
                           'входов'.format(main['operation']))
                for r in unresolved_roots:
                    _set(r, ORIGIN_UNRESOLVED, why)
                continue

            # Терминал не найден — продолжаем обход назад по входам всех
            # производителей.
            if len(next_frontier) < _ORIGIN_MAX_FRONTIER:
                for dp, dh in all_deps:
                    if dh in seen:
                        continue
                    next_frontier.add(dh)
                    seen.add(dh)
                    h_to_path.setdefault(dh, dp)
                    dependents.setdefault(dh, set()).update(unresolved_roots)
                    if len(next_frontier) >= _ORIGIN_MAX_FRONTIER:
                        break

        frontier = next_frontier

    # Всё, что осталось без вердикта после последнего раунда.
    #
    # Причину различаем честно. Раньше здесь безусловно писалось "обход
    # прерван по глубине", и это вводило в заблуждение: на NPUR.34018-01
    # обход вставал на пятом раунде при лимите восемь, то есть кандидаты
    # кончились сами, а отчёт утверждал, что не хватило глубины. Это
    # противоположные утверждения: в первом случае проверка отработала
    # полностью и источник не нашла, во втором ей не дали доработать.
    if frontier:
        why_rest = ('обход прерван по глубине (израсходован лимит '
                    '{} раундов)'.format(max_rounds))
    else:
        why_rest = ('цепочка прослежена до конца, '
                    'опознанный источник не найден')
    n_rest = 0
    for h in by_hash:
        if h not in verdict:
            _set(h, ORIGIN_UNRESOLVED, why_rest)
            n_rest += 1
    if n_rest:
        print(_ts() + "   Origin: без вердикта после обхода: {} ({})".format(
            n_rest, 'лимит раундов' if frontier else 'кандидаты исчерпаны'))

    # --- Резерв: наследование происхождения от ближайшего пакета ------------
    #
    # Зачем это вообще нужно. Трасса не связывает архив с его содержимым НИ В
    # ОДНУ сторону, и это не частный пробел, а свойство данных. Измерено на
    # NPUR.69035-01:
    #   dpkg --unpack --recursive <каталог>  — 1607 входов, из них .deb: 0
    #   dpkg-deb --build debian/<пакет> ..   — входы: только libc, libz,
    #       locale-archive и прочие библиотеки процесса; выход — временный
    #       файл /tmp/dpkg-deb.XXXXXX, а не .deb (переименование трассой
    #       не сохраняется)
    #   xorriso -as mkisofs                  — входы: только библиотеки,
    #       самого tgz среди них нет
    # Поэтому у файла, извлечённого из пакета, производитель в трассе есть
    # (сама распаковка), а входа у этой распаковки нет — цепочка обрывается
    # на первом шаге, и никакая глубина обхода не поможет.
    #
    # Связь "файл внутри пакета <-> пакет" существует в НАШИХ данных:
    # generate_json.sh считает parents_hash/parents_chain при распаковке
    # своими руками. Для NPUR.69035-01 это закрыло все 103 неопознанных
    # файла: 14 пакетов, все из apt-get download.
    #
    # Два ограничения обязательны:
    #   1. Только БЛИЖАЙШИЙ пакет. Цепочка идёт от ближнего к дальнему:
    #      [0] python3-cffi-backend_1.9.1-2_amd64.deb  (apt-get, touch)
    #      [1] НПУР.69035-01_12_02.tgz                 (bash, cp, touch)
    #      [2] NPUR.69035-01_12_02.iso                 (xorriso)
    #      Подъём на уровень [2] свёл бы любой файл изделия к "образ собран
    #      xorriso", то есть к бессмыслице.
    #   2. Только как РЕЗЕРВ, после обхода. Своя цепочка файла всегда
    #      главнее унаследованной.
    inherited = {}   # id(entry) -> (origin, detail, container_name)
    if bin_hash_to_path:
        need = [e for e in entries
                if verdict.get((e.get('hash') or '').strip(),
                               (ORIGIN_UNRESOLVED, ''))[0] == ORIGIN_UNRESOLVED]
        cand = {}    # entry -> (container_hash, container_name)
        for e in need:
            for ph in (e.get('parents') or []):
                cpath = bin_hash_to_path.get(ph, '')
                if cpath.lower().endswith(_CONTAINER_PKG_EXTS):
                    cand[id(e)] = (ph, cpath.rsplit('/', 1)[-1])
                    break
        if cand:
            cont_hashes = {ch for ch, _ in cand.values()}
            print(_ts() + "   Origin: резерв по контейнеру — записей {}, "
                          "контейнеров {}".format(len(cand), len(cont_hashes)))
            # Хеш контейнера может сам лежать в src.json или на носителе —
            # тогда производитель не нужен вовсе.
            cont_prod = collect_producers(
                buildography_files,
                {ch for ch in cont_hashes
                 if ch not in src_hashes and ch not in trusted},
                compiler_basenames, linker_basenames, with_siblings=False)
            for e in need:
                got = cand.get(id(e))
                if not got:
                    continue
                ch, cname = got
                if ch in src_hashes:
                    inherited[id(e)] = (
                        ORIGIN_PRODUCT_SRC,
                        'хеш пакета есть в src.json', cname)
                    continue
                if ch in trusted:
                    label, kind, container = trusted[ch]
                    inherited[id(e)] = (
                        ORIGIN_APPROVED,
                        '{}/{}: {}'.format(label, kind, container), cname)
                    continue
                plist = cont_prod.get(ch) or []
                dl = next((pr for pr in plist
                           if pr['operation'] == _OP_DOWNLOADED), None)
                if dl is not None:
                    # В detail у apt-get download стоит лишь имя операции.
                    # Настоящий адрес источника лежит во входе той же команды
                    # с тем же хешем: apt копирует пакет из репозитория, и
                    # копия побайтно совпадает с оригиналом. Для отчёта нужен
                    # именно путь — на NPUR.69035-01 это
                    # /opt/astra-repo/dev/pool/main/..., то есть носитель,
                    # который не согласован.
                    src_addr = ''
                    for dp, dh in (dl['deps'] or []):
                        if dh == ch and dp:
                            src_addr = dp
                            break
                    detail = dl['detail'] or 'сетевая загрузка'
                    if src_addr:
                        detail = '{}: {}'.format(detail, src_addr[:160])
                    inherited[id(e)] = (ORIGIN_DOWNLOAD, detail, cname)
                    continue
                for pr in plist:
                    hit = None
                    for dp, dh in (pr['deps'] or []):
                        if dh in trusted:
                            hit = ('t', dp, dh); break
                        if dh in src_hashes:
                            hit = ('s', dp, dh); break
                    if hit:
                        if hit[0] == 't':
                            label, kind, container = trusted[hit[2]]
                            inherited[id(e)] = (
                                ORIGIN_APPROVED,
                                '{}/{}: {}'.format(label, kind, container),
                                cname)
                        else:
                            inherited[id(e)] = (
                                ORIGIN_PRODUCT_SRC,
                                'вход сборки пакета есть в src.json: '
                                '{}'.format(hit[1][:120]), cname)
                        break
            print(_ts() + "   Origin: резерв по контейнеру разрешил "
                          "{} записей".format(len(inherited)))

    # --- Раскладываем записи и дописываем поля ------------------------------
    out = {ORIGIN_PRODUCT_SRC: [], ORIGIN_APPROVED: [],
           ORIGIN_DOWNLOAD: [], ORIGIN_UNRESOLVED: []}

    # Операцию берём у производителя самого файла (первый шаг цепочки)
    own_producers = collect_producers(
        buildography_files, set(by_hash.keys()),
        compiler_basenames, linker_basenames, max_deps=1,
        with_siblings=False)

    for h, elist in by_hash.items():
        origin, detail = verdict.get(h, (ORIGIN_UNRESOLVED, ''))
        for e in elist:
            prod = _pick_main_producer(own_producers.get(h) or [],
                                       e.get('path', ''))
            e['operation']     = prod.get('operation', _OP_OTHER)
            e['operation_cmd'] = prod.get('argv', '')
            ch = chains.get(h)
            inh = inherited.get(id(e))
            if inh is not None:
                # Унаследованное подтверждение слабее прямого, и в отчёте это
                # должно быть видно: подтверждён пакет, а не сам файл.
                e_origin, e_detail, cname = inh
                e['origin']           = e_origin
                e['origin_detail']    = '{} (через контейнер {})'.format(
                    e_detail, cname)
                e['origin_container'] = cname
                e['origin_inherited'] = True
                steps = list(ch or [])
                steps.append('contained in {}'.format(cname))
                e['origin_chain'] = ' <- '.join(steps[:6])
            else:
                e['origin']        = origin
                e['origin_detail'] = detail
                if ch:
                    e['origin_chain'] = ' <- '.join(ch[:6])
                e_origin = origin
            out[e_origin].append(e)

    print(_ts() + "   Origin: product_src={}, approved_source={}, "
                  "download={}, unresolved={}".format(
                      len(out[ORIGIN_PRODUCT_SRC]), len(out[ORIGIN_APPROVED]),
                      len(out[ORIGIN_DOWNLOAD]), len(out[ORIGIN_UNRESOLVED])))
    return (out[ORIGIN_PRODUCT_SRC], out[ORIGIN_APPROVED],
            out[ORIGIN_DOWNLOAD], out[ORIGIN_UNRESOLVED])


def analyze_pass4(bin_entries, src_hashes, buildography_files, script_dir,
                  compiler_basenames=None, linker_basenames=None):
    """
    Проверяет происхождение бинарей дистрибутива.
    Итеративно расширяет граф только для нужных цепочек.

    compiler_basenames, linker_basenames — множества из utilities.yaml.
    Если заданы, внешние зависимости учитываются только для команд компиляторов/линкеров.
    Зависимости cp/install/make/cat и т.д. игнорируются.
    """
    _log_memory("Pass 4 start")

    # Объединяем компиляторы и линкеры в одно множество для фильтра
    if compiler_basenames or linker_basenames:
        compiler_linker_basenames = set(compiler_basenames or set()) | set(linker_basenames or set())
        print(_ts() + "   Pass 4: compiler+linker filter: {} tools".format(
            len(compiler_linker_basenames)))
    else:
        compiler_linker_basenames = None
        print(_ts() + "   Pass 4: compiler+linker filter: disabled")

    # Строим индекс apt download пакетов — они заведомо внешние
    apt_deb_names = build_apt_download_hashes(buildography_files)

    # Конвертируем src_hashes в int
    src_hashes_int = set()
    for h in src_hashes:
        hi = _hash_to_int(h)
        if hi is not None:
            src_hashes_int.add(hi)
    print(_ts() + "   Pass 4: src_hashes_int={}".format(len(src_hashes_int)))

    # Хеши бинарей для проверки
    bin_hashes_int = set()
    for entry in bin_entries:
        hi = _hash_to_int(entry.get('hash', '').strip())
        if hi is not None:
            bin_hashes_int.add(hi)
    print(_ts() + "   Pass 4: bin_hashes_int={}".format(len(bin_hashes_int)))

    total_cmds = _count_cmds(buildography_files)

    # Предварительный проход — собираем все output хеши чтобы корректно
    # определять промежуточные артефакты в _scan_pass
    print(_ts() + "   Pass 4: pre-collecting all output hashes...")
    all_output_hashes_pre = _collect_output_hashes(buildography_files)
    print(_ts() + "   Pass 4: pre-collected {} output hashes".format(
        len(all_output_hashes_pre)))

    # Максимальная глубина итераций — покрывает сценарии:
    # iter1: бинарь ← прямые зависимости
    # iter2: .o/.a ← их зависимости (компиляция из скачанных исходников)
    # iter3: .so ← их зависимости (редкий случай)
    MAX_ITERATIONS = 3
    # Максимальный размер frontier — защита от "жирных" команд
    # (линковщики с сотнями тысяч зависимостей)

    # ==========================================================================
    # Итеративное расширение графа
    # ==========================================================================
    # Общий граф — накапливается итеративно
    full_out_to_deps   = {}   # out_hash_int -> [(dep_hi, path, h_str)]
    all_output_hashes  = set()
    all_dep_hashes     = set()

    # Стартуем с бинарей из bin_entries
    frontier = set(bin_hashes_int)
    iteration = 0

    while frontier and iteration < MAX_ITERATIONS:
        iteration += 1
        print(_ts() + "   Pass 4 iter {}/{}: scanning for {} hashes...".format(
            iteration, MAX_ITERATIONS, len(frontier)))
        _log_memory("Pass 4 iter {} start".format(iteration))

        # Для iter1 используем предварительно собранные хеши
        # Для iter2+ используем накопленные all_output_hashes
        _aoh = all_output_hashes_pre if iteration == 1 else all_output_hashes
        out_to_deps, frontier_hashes, output_hashes_seen, dep_hashes_seen = _scan_pass(
            buildography_files, frontier, total_cmds,
            "Pass 4 iter{}".format(iteration),
            compiler_linker_basenames=compiler_linker_basenames,
            src_hashes_int=src_hashes_int,
            all_output_hashes=_aoh,
            progress_step_pct=PASS4_PROGRESS_STEP_PCT,
        )

        # Объединяем с общим графом
        for out_hi, deps in out_to_deps.items():
            if out_hi in full_out_to_deps:
                full_out_to_deps[out_hi].extend(deps)
            else:
                full_out_to_deps[out_hi] = list(deps)

        all_output_hashes.update(output_hashes_seen)
        all_dep_hashes.update(dep_hashes_seen)

        _log_memory("Pass 4 iter {} after scan".format(iteration))
        print(_ts() + "   Pass 4 iter {}: found {} commands, "
              "output_hashes={}, dep_hashes={}".format(
              iteration, len(out_to_deps),
              len(all_output_hashes), len(all_dep_hashes)))

        # Новый frontier — берём из frontier_hashes (уже отфильтрованные промежуточные артефакты)
        # Исключаем те что уже есть в full_out_to_deps
        print(_ts() + "   Pass 4 iter {}: building new frontier from {} candidates...".format(
            iteration, len(frontier_hashes)))
        new_frontier = frontier_hashes - set(full_out_to_deps.keys())
        print(_ts() + "   Pass 4 iter {}: frontier built: {} hashes".format(
            iteration, len(new_frontier)))

        frontier = new_frontier
        print(_ts() + "   Pass 4 iter {}: new frontier size: {}".format(
            iteration, len(frontier)))

        print(_ts() + "   Pass 4 iter {}: merging into full graph...".format(iteration))
        del out_to_deps, frontier_hashes, output_hashes_seen, dep_hashes_seen
        print(_ts() + "   Pass 4 iter {}: running gc.collect()...".format(iteration))
        gc.collect()
        print(_ts() + "   Pass 4 iter {}: gc done".format(iteration))

    print(_ts() + "   Pass 4: graph expansion done after {} iterations".format(iteration))
    print(_ts() + "   Pass 4: full_out_to_deps={}, all_output_hashes={}, "
          "all_dep_hashes={}".format(
          len(full_out_to_deps), len(all_output_hashes), len(all_dep_hashes)))
    _log_memory("after graph expansion")

    # ==========================================================================
    # Классификация бинарей
    # ==========================================================================
    system_binaries        = []  # путь в дистрибутиве — системный (usr/lib, lib и т.д.)
    compiled_from_src      = []  # сценарий 1: собран из src.json, трассировщик подтверждает
    binaries_from_src      = []  # сценарий 2: хеш в src.json, скопирован напрямую
    untraced_from_src      = []  # сценарий 6: хеш в src.json, трассировщик не видит
    external_built         = []  # сценарий 5: собран из внешних исходников
    external_prebuilt      = []  # сценарий 3: готовый бинарь извне, трассировщик видит
    untraced_external      = []  # сценарий 4: не в src.json, трассировщик не видит
    apt_package_content    = []  # apt download: файлы внутри скачанных .deb пакетов

    # Системные пути в дистрибутиве — проверяем путь бинаря в дистрибутиве
    DISTRIB_SYSTEM_PREFIXES = (
        '/usr/lib/', '/usr/lib64/', '/lib/', '/lib64/',
        '/usr/include/', '/usr/local/lib/',
        '/etc/', '/proc/', '/sys/', '/dev/',
        '/usr/share/', '/var/',
        # Те же пути без ведущего слеша (относительные)
        'usr/lib/', 'usr/lib64/', 'lib/', 'lib64/',
        'usr/include/', 'usr/local/lib/',
        'etc/', 'proc/', 'sys/', 'dev/',
        'usr/share/', 'var/',
    )

    # Расширения архивов/пакетов — используются в нескольких функциях
    _CONTAINER_EXTS = ('.iso', '.iso_dir', '.deb', '.deb_dir', '.rpm', '.rpm_dir',
                       '.tar', '.tgz', '.zip', '.gz', '.xz', '.bz2')

    def _get_container(path):
        """
        Извлекает ближайший контейнер (архив/пакет) из пути.
        Возвращает имя файла-контейнера (без _dir суффикса) или None.

        Примеры:
          .../DISK02.iso_dir/repo/kt-watchdog_5.0.2_amd64.deb_dir/usr/bin/foo
            → 'kt-watchdog_5.0.2_amd64.deb'
          .../DISK01.iso_dir/KTDL/devel_debs/libapache2_amd64.deb_dir/usr/lib/foo.so
            → 'libapache2_amd64.deb'
          .../bin/usr/bin/foo   (нет контейнера)
            → None
        """
        parts = path.split('/')
        container = None
        for part in parts:
            # Убираем _dir суффикс если есть
            name = part[:-4] if part.endswith('_dir') else part
            for ext in _CONTAINER_EXTS:
                if name.lower().endswith(ext) and not ext.endswith('_dir'):
                    container = name
                    break
        return container

    def _is_inside_package(path):
        """Возвращает True если путь содержит сегмент .deb/_dir или .rpm/_dir."""
        parts = path.split('/')
        for part in parts:
            name = part[:-4] if part.endswith('_dir') else part
            if name.lower().endswith('.deb') or name.lower().endswith('.rpm'):
                return True
        return False

    def _is_distrib_system_path(path):
        """
        Проверяем путь бинаря внутри дистрибутива.
        ВАЖНО: файлы внутри .deb/.rpm пакетов НЕ считаются системными —
        это содержимое пакета, а не системный путь хоста сборки.
        """
        # Если файл внутри .deb/.rpm — не системный, это пакет
        if _is_inside_package(path):
            return False

        # Убираем префикс типа KTDL.00554-01/bin/ или bin/
        p = path
        for prefix in ('bin/', ):
            idx = p.find(prefix)
            if idx >= 0:
                p = p[idx + len(prefix):]
                break
        # Убираем архивные суффиксы типа foo.iso/bar.deb/
        parts = p.split('/')
        clean_parts = []
        for part in parts:
            name = part[:-4] if part.endswith('_dir') else part
            if any(name.lower().endswith(ext) for ext in
                   ('.iso', '.deb', '.rpm', '.tar', '.tgz', '.zip', '.gz')):
                clean_parts = []
            else:
                clean_parts.append(part)
        clean_path = '/'.join(clean_parts)
        return any(clean_path.startswith(pfx) for pfx in DISTRIB_SYSTEM_PREFIXES)

    total_bin = len(bin_entries)
    print(_ts() + "   Pass 4: classifying {} binaries...".format(total_bin))

    # Кэш результатов _get_ext_deps — только real зависимости.
    # Кэш результатов _get_ext_deps — только real зависимости.
    # filtered не кэшируем — восстанавливаются в постобработке.
    _ext_deps_cache = {}  # hi -> [real dep entries]

    def _get_ext_deps(start_hi):
        """
        BFS по графу зависимостей с кэшем.
        Вычисляется только для реально запрошенных бинарей (не для всего графа).
        """
        from collections import deque as _deque2

        if start_hi in _ext_deps_cache:
            return {"real": _ext_deps_cache[start_hi], "filtered": []}

        real    = []
        visited = set()
        queue   = _deque2([start_hi])

        while queue:
            hi = queue.popleft()
            if hi in visited:
                continue
            visited.add(hi)

            # Берём из кэша если уже вычислено
            if hi != start_hi and hi in _ext_deps_cache:
                real.extend(_ext_deps_cache[hi])
                continue

            for (dep_hi, dep_path, dep_h_str) in full_out_to_deps.get(hi, []):
                if dep_hi in src_hashes_int:
                    continue
                if _is_system_path(dep_path):
                    continue
                # Ресурсный файл — не является признаком внешних исходников
                if os.path.splitext(dep_path)[1].lower() in RESOURCE_EXTENSIONS:
                    continue
                if _is_allowed_external_dep(dep_path):
                    continue
                if dep_hi in all_output_hashes:
                    if dep_hi not in visited:
                        queue.append(dep_hi)
                else:
                    real.append({"path": dep_path, "hash": dep_h_str})

        real = _merge_so_aliases(real)
        _ext_deps_cache[start_hi] = real
        return {"real": real, "filtered": []}


    def _merge_so_aliases(deps):
        """
        Схлопывает versioned .so в одну запись с полем 'aliases'.
        libfoo.so, libfoo.so.1, libfoo.so.1.2.3 → одна запись,
        base_name=libfoo.so, aliases=[все найденные варианты путей].
        """
        # Группируем по директории + базовому имени .so
        groups  = {}   # (dir, base_so_name) -> list of dep dicts
        singles = []   # не .so — оставляем как есть

        for dep in deps:
            p    = dep.get('path', '')
            bn   = os.path.basename(p)
            base = _so_base_name(bn)
            if base is None:
                singles.append(dep)
                continue
            key = (os.path.dirname(p), base)
            groups.setdefault(key, []).append(dep)

        merged = list(singles)
        for (dirn, base_so), group in groups.items():
            if len(group) == 1:
                merged.append(group[0])
            else:
                # Берём запись с минимальным суффиксом (базовый .so если есть)
                primary = min(group, key=lambda d: len(d.get('path', '')))
                aliases = sorted(set(d.get('path', '') for d in group))
                entry = {'hash': primary.get('hash', ''),
                         'path': os.path.join(dirn, base_so),
                         'aliases': aliases}
                merged.append(entry)

        return merged

    _classify_start = time.monotonic()
    for i, entry in enumerate(bin_entries):
        progress_log("Pass 4 classifying", i + 1, total_bin)
        _entry_start = time.monotonic()
        path      = entry.get('path', '')
        h_str     = entry.get('hash', '').strip()
        hi        = _hash_to_int(h_str)
        container = _get_container(path)  # ближайший архив/пакет в пути или None

        def _make_entry(extra=None):
            """Строит базовую запись с опциональным полем container."""
            e = {'path': path, 'hash': h_str}
            if container:
                e['container'] = container
            # Родителей обязательно переносим из исходной записи. Через
            # _make_entry проходят ВСЕ категории Прохода 4, и запись здесь
            # собирается заново — без этой строки parents из bin.json
            # теряется, и резерв по контейнеру в resolve_origins остаётся
            # без данных (на NPUR.69035-01 это оставляло 103 файла в
            # "происхождение не подтверждено" при полностью рабочем резерве).
            _par = entry.get('parents')
            if _par:
                e['parents'] = _par
            if extra:
                e.update(extra)
            return e

        # Нулевой фильтр — файл внутри .deb пакета скачанного через apt download.
        # Проверяем по имени контейнера в пути, не по хешу —
        # хеш содержимого не совпадает с хешем самого .deb архива.
        apt_container = _get_container(path)
        if apt_container and (apt_container in apt_deb_names or
                              _deb_name_variants(apt_container)
                              & apt_deb_names):
            apt_package_content.append(_make_entry({
                'package_type': 'deb',
                'source':       'apt download',
                'command':      'apt download',
            }))
            continue

        # Первый фильтр — системный путь в дистрибутиве
        if _is_distrib_system_path(path):
            system_binaries.append(_make_entry())
            continue

        if hi is None:
            untraced_external.append(_make_entry())
            continue

        if hi not in all_output_hashes and hi not in all_dep_hashes:
            # Нет в трассировщике — проверяем есть ли в src.json
            if h_str in src_hashes:
                untraced_from_src.append(_make_entry())
            else:
                untraced_external.append(_make_entry())
            continue

        if hi not in all_output_hashes and hi in all_dep_hashes:
            # Готовый бинарь — трассировщик видит его как зависимость
            # Проверяем есть ли в src.json
            if h_str in src_hashes:
                binaries_from_src.append(_make_entry())
            else:
                external_prebuilt.append(_make_entry())
            continue

        # Собран — проверяем цепочку зависимостей
        deps_result   = _get_ext_deps(hi)
        _entry_elapsed = time.monotonic() - _entry_start
        if _entry_elapsed > 5.0:
            print(_ts() + "   [SLOW] classifying {}/{}: {:.1f}s path={}".format(
                i+1, total_bin, _entry_elapsed, path[:80]))
        real_ext_deps = deps_result['real']
        filt_ext_deps = deps_result['filtered']

        if real_ext_deps:
            # filtered_deps дедуплицируем по пути — убираем тысячи одинаковых
            # системных путей, оставляем только уникальные
            seen_filtered = set()
            deduped_filtered = []
            for fd in filt_ext_deps:
                fp = fd.get('path', '')
                if fp not in seen_filtered:
                    seen_filtered.add(fp)
                    deduped_filtered.append(fd)

            e = _make_entry({'external_deps': real_ext_deps})
            if deduped_filtered:
                e['filtered_deps'] = deduped_filtered
            external_built.append(e)
        else:
            # Все подозрительные зависимости отфильтрованы — бинарь чистый
            if h_str in src_hashes:
                binaries_from_src.append(_make_entry())
            else:
                # ------------------------------------------------------------
                # ОТЛОЖЕННАЯ ЗАДАЧА: обрыв обратной цепочки.
                #
                # Это else-ветка, и сюда файл попадает по отсутствию
                # возражений, а не по доказательству. Условие "подозрительных
                # зависимостей не осталось" выполняется слишком легко: у
                # команды в трассе 280-320 зависимостей, и подавляющая их
                # часть — чтения динамического загрузчика (libc,
                # locale-archive, gconv-modules, libz), которые целиком
                # уходят в системный фильтр. Проверять оказывается нечего.
                #
                # Кроме того цепочка обрывается по трём техническим причинам:
                #   - расширение фронтира ограничено MAX_ITERATIONS = 3;
                #   - _scan_pass выбрасывает ПУТЬ зависимости, если её хеш
                #     является чьим-то выходом (считает промежуточным
                #     артефактом) — так молча проглатываются внешние архивы;
                #   - команда может не опознаваться как инструмент, если её
                #     настоящий исполнитель стоит не в argv[0].
                #
                # Сейчас это НЕ исправляется сознательно: расширение фронтира
                # и сохранение путей архивов требуют отдельного решения,
                # более изящного, чем увеличение лимита итераций. Пока
                # компенсация сделана снаружи — resolve_origins() разбирает
                # эту категорию по действительному терминалу цепочки и
                # раскладывает её на origin_* отчёты в try{N}.
                #
                # К самому обрыву вернуться нужно.
                # ------------------------------------------------------------
                compiled_from_src.append(_make_entry())

    del full_out_to_deps, all_output_hashes, all_dep_hashes
    del src_hashes_int, bin_hashes_int
    gc.collect()
    _log_memory("Pass 4 done")

    # ==========================================================================
    # Постобработка: восстанавливаем filtered_deps для external_built бинарей
    # Делаем один проход по buildography только для этих бинарей.
    # ==========================================================================
    if external_built:
        print(_ts() + "   Pass 4: restoring filtered_deps for {} external_built "
              "binaries...".format(len(external_built)))

        # Строим индекс: out_hash_int → индекс в external_built
        ext_built_index = {}
        for idx, e in enumerate(external_built):
            hi = _hash_to_int(e.get('hash', '').strip())
            if hi is not None:
                ext_built_index[hi] = idx

        # Один проход по buildography
        for file_path in buildography_files:
            with open(file_path, 'rb') as f:
                data = _json_loads(f.read())
            for cmd in data.get('component_commands', []):
                # Проверяем выходы команды
                outputs = cmd.get('output', {})
                out_his = set()
                if isinstance(outputs, list):
                    for out in outputs:
                        if isinstance(out, dict):
                            hi = _hash_to_int(out.get('hash', '').strip())
                            if hi is not None:
                                out_his.add(hi)
                elif isinstance(outputs, dict):
                    for _, h in outputs.items():
                        hi = _hash_to_int(h.strip() if h else '')
                        if hi is not None:
                            out_his.add(hi)

                relevant = out_his & set(ext_built_index.keys())
                if not relevant:
                    continue

                # Собираем filtered_deps для этой команды
                filtered = []
                seen_paths = set()
                deps_raw = cmd.get('dependencies', {})
                items = deps_raw.items() if isinstance(deps_raw, dict) else                         [(d.get('path',''), d.get('hash','')) for d in deps_raw
                         if isinstance(d, dict)]
                for path, h in items:
                    if path in seen_paths:
                        continue
                    if _is_system_path(path) or _is_allowed_external_dep(path):
                        seen_paths.add(path)
                        filtered.append({"path": path, "hash": h.strip() if h else "",
                                          "reason": _filter_reason(path)})

                # Добавляем filtered_deps к нужным записям
                for out_hi in relevant:
                    idx = ext_built_index[out_hi]
                    if filtered:
                        external_built[idx]['filtered_deps'] = filtered

            del data

        print(_ts() + "   Pass 4: filtered_deps restored")

    print(_ts() + "   Pass 4 done: "
          "compiled_from_src={}, binaries_from_src={}, untraced_from_src={}, "
          "external_built={}, external_prebuilt={}, untraced_external={}, "
          "system_binaries={}, apt_package_content={}".format(
          len(compiled_from_src), len(binaries_from_src), len(untraced_from_src),
          len(external_built), len(external_prebuilt), len(untraced_external),
          len(system_binaries), len(apt_package_content)))

    return (compiled_from_src, binaries_from_src, untraced_from_src,
            external_built, external_prebuilt, untraced_external, system_binaries,
            apt_package_content)



def write_origin_txt(output_path, category_label, entries):
    """
    Записывает отчёт origin_* в формате
        путь<TAB>хеш<TAB>категория<TAB>операция<TAB>основание

    Обычный формат "путь<TAB>хеш" для этих отчётов не годится. Срез по
    происхождению собирает файлы из РАЗНЫХ категорий Pass 4, и без колонки
    категории теряется то, ради чего категории существуют: читатель видит
    файл в origin_unresolved, но не узнаёт, что тот пришёл готовым извне
    (external_prebuilt) или был переупакован. Это два разных замечания.

    Возвращает True, если записана хотя бы одна строка.
    """
    seen = set()
    rows = []
    # Пакет-контейнер -> уникальные файлы в нём. Нужен для сводной шапки:
    # 103 строки с .o и .a внутри gcc-дева читаются как 103 замечания, тогда
    # как замечание одно на пакет, и разработчику нужен именно пакет.
    by_container = {}
    for entry in entries:
        path = (entry.get('path') or '').strip()
        h    = (entry.get('hash') or '').strip()
        if not path and not h:
            continue
        key = (path, h)
        if key in seen:
            continue
        seen.add(key)
        cont = entry.get('origin_container', '')
        if cont:
            by_container.setdefault(cont, set()).add(h or path)
        rows.append((
            path, h,
            entry.get('pass4_category', ''),
            entry.get('operation', ''),
            (entry.get('origin_detail') or '').replace('\t', ' '),
        ))
    rows.sort(key=lambda x: x[0])

    versioned_path = get_versioned_filepath(output_path)
    if versioned_path != output_path:
        print(_ts() + "   File exists, writing to: {}".format(
            os.path.basename(versioned_path)))

    uniq_hashes = len({r[1] for r in rows if r[1]})
    with open(versioned_path, 'w', encoding='utf-8') as f:
        f.write("# {}\n".format(category_label))
        f.write("# Generated: {}\n".format(datetime.now().isoformat()))
        f.write("# Записей: {}, уникальных файлов: {}\n".format(
            len(rows), uniq_hashes))
        f.write("# Format: path<TAB>hash<TAB>pass4_category<TAB>operation"
                "<TAB>origin_detail\n")
        if by_container:
            f.write("#\n")
            f.write("# Происхождение унаследовано от пакета-контейнера.\n")
            f.write("# Замечание — на пакет, а не на каждый файл внутри него.\n")
            f.write("# Пакетов: {}\n".format(len(by_container)))
            f.write("#\n")
            for cname, fset in sorted(by_container.items(),
                                      key=lambda kv: (-len(kv[1]), kv[0])):
                f.write("#   {:<54} файлов: {}\n".format(cname, len(fset)))
        f.write("#\n")
        for r in rows:
            f.write("\t".join(r) + "\n")
    print(_ts() + "   Written {} entries ({} уникальных) -> {}".format(
        len(rows), uniq_hashes, os.path.basename(versioned_path)))
    return bool(rows)


def write_pass4_txt(output_path, category_label, entries):
    """Записывает текстовый файл Прохода 4 в формате path<TAB>hash."""
    seen = set()
    rows = []
    for entry in entries:
        path = entry.get('path', '').strip()
        h = entry.get('hash', '').strip()
        if not path and not h:
            continue
        key = (path, h)
        if key in seen:
            continue
        seen.add(key)
        rows.append((path, h))
    rows.sort(key=lambda x: x[0])

    versioned_path = get_versioned_filepath(output_path)
    if versioned_path != output_path:
        print(_ts() + "   File exists, writing to: {}".format(os.path.basename(versioned_path)))

    with open(versioned_path, 'w', encoding='utf-8') as f:
        f.write("# Pass 4: {}\n".format(category_label))
        f.write("# Generated: {}\n".format(datetime.now().isoformat()))
        f.write("# Total: {}\n".format(len(rows)))
        f.write("# Format: path<TAB>hash\n")
        f.write("#\n")
        for path, h in rows:
            f.write("{}\t{}\n".format(path, h))
    print(_ts() + "   Written {} entries -> {}".format(len(rows), versioned_path))


# =============================================================================
# ОБРАБОТКА ПРОЕКТА
# =============================================================================
def process_project(project_name, compiler_basenames, linker_basenames, interpreter_basenames, by_disk=False, keep=False, trust_java=True):
    print("\n" + "=" * 50)
    print("Processing project: {}".format(project_name))
    print("=" * 50)

    buildography_pattern = os.path.join(BUILDOGRAPHY_DIR, project_name, "*.json")
    buildography_files = sorted(glob.glob(buildography_pattern))
    if not buildography_files:
        print(_ts() + "   No buildography JSON found: {}".format(buildography_pattern))
        return False

    print(_ts() + "   Buildography files found: {}".format(len(buildography_files)))
    for f in buildography_files:
        print(_ts() + "     {}".format(_safe(os.path.basename(f))))

    sources_dir = os.path.join(RESULTS_DIR, project_name, "sources")
    signatures_pattern = os.path.join(sources_dir, "*_src.json")
    signatures_files = sorted(glob.glob(signatures_pattern))
    if not signatures_files:
        print(_ts() + "   No *_src.json found: {}".format(signatures_pattern))
        return False

    print(_ts() + "   Source signature files found: {}".format(len(signatures_files)))
    for f in signatures_files:
        print(_ts() + "     {}".format(_safe(os.path.basename(f))))

    output_dir = os.path.join(RESULTS_DIR, project_name, "izb")
    os.makedirs(output_dir, exist_ok=True)

    # Согласованные носители — ищем до начала разбора, чтобы предупреждение
    # о неверно названном каталоге и вопрос пользователю прозвучали сразу,
    # а не через полчаса анализа.
    trusted_dirs = discover_trusted_dirs(project_name)
    trusted      = load_trusted_inventory(trusted_dirs)

    try:
        signatures = load_signatures(signatures_files)
        buildography_hashes, raw_cmds = load_buildography_data(buildography_files)

        # Восстанавливаем разорванную цепочку компиляции C/C++:
        # cc1plus компилирует .cpp но пишет в пайп (output=0), а .o создаёт
        # 'as'. Связываем их по basename .o чтобы .cpp не попал в not_compiled.
        _linked = link_compiler_to_assembler(raw_cmds)
        if _linked:
            print(_ts() + "   Linked compiler->assembler chains: {} .cpp->.o pairs".format(_linked))

        # Восстанавливаем цепочку для clang (интегрированный ассемблер):
        # трассировщик не пишет .o в output команды 'clang++ -c ... -o X.o',
        # но .o есть во входах ld. Берём hash .o из потребителя и синтезируем
        # выход компилятору, чтобы .cpp не попал в избыточные ложно.
        # Сшивка clang -> .o через потребителя ОТКЛЮЧЕНА ПО УМОЛЧАНИЮ.
        #
        # Проверено на buildography ТДС (60085 и 60099): цепочка .cpp -> .o -> бинарь
        # НЕ разорвана, и правка не нужна. Механика: clang пишет объектный файл во
        # временный файл со случайным именем в /tmp (вида /tmp/xxxx.o.tmp) и затем
        # переименовывает его в целевой .o. Трассировщик записывает ВРЕМЕННЫЙ путь
        # (событие переименования в JSON не сохраняется), но ХЕШ содержимого при
        # переименовании не меняется. При этом дочерний процесс 'clang -cc1' имеет
        # исходник в dependencies и хеш объектника в output, а BFS в
        # build_transitive_good_commands сопоставляет узлы И ПО ХЕШУ — поэтому связь
        # .cpp -> .o -> ld -> бинарь замыкается сама.
        #
        # Замеры (60085): 1106 хешей .o.tmp на выходе, все 1106 совпали с хешами .o
        # во входах ld; 1109 команд с .o.tmp содержат исходник в deps (все -cc1).
        # Эксперимент вкл/выкл на одном buildography: классификация не изменилась
        # (moved to not_compiled = 353 в обоих случаях), менялось лишь число
        # "good commands" (2887 против 1950) за счёт лишних синтезированных рёбер.
        #
        # Работает как САМООГРАНИЧИВАЮЩИЙСЯ фолбэк: сшивает только те команды,
        # для которых хеш .o не заявлен выходом НИ ОДНОЙ команды (т.е. цепочка
        # действительно разорвана). Если хеш уже кем-то производится — не
        # вмешивается и сообщает об этом. Отключить полностью: NO_CLANG_LINK=1
        if os.environ.get('NO_CLANG_LINK') == '1':
            print(_ts() + "   [NO_CLANG_LINK=1] clang->.o fallback DISABLED")
        else:
            _linked_clang, _intact_clang = link_compiler_output_via_consumers(raw_cmds)
            if _intact_clang:
                print(_ts() + "   clang->.o chain already closed by hash for {} commands "
                              "(fallback not needed)".format(_intact_clang))
            if _linked_clang:
                print(_ts() + "   WARNING: clang->.o chain BROKEN for {} commands — "
                              "synthesized outputs via consumers. Трасса не записала "
                              "выход компилятора; результаты проверить отдельно."
                              .format(_linked_clang))

        bin_hashes, bin_paths = load_bin_signatures(project_name)
    except Exception as e:
        print(_ts() + "   Failed to load data: {}".format(e))
        import traceback
        traceback.print_exc()
        return False

    # Загружаем bin_entries для Прохода 4 — только реальные бинари из binaries_in_bin.txt
    # binaries_in_bin.txt содержит пути ELF бинарей из дистрибутива
    # Хеши берём из bin.json по путям
    bin_entries = []  # инициализируем заранее на случай если файлы не найдены
    bin_hash_to_path = {}  # хеш -> путь внутри дистрибутива (для контейнеров)
    pass4_ran   = False  # флаг успешного выполнения Pass 4
    total_bin_count = 0  # будем хранить количество бинарных файлов для статистики
    bin_json_path = os.path.join(RESULTS_DIR, project_name, "sources",
                                 "{}_bin.json".format(project_name))
    binaries_in_bin_path = os.path.join(RESULTS_DIR, project_name, "ext",
                                        "binaries_in_bin.txt")

    if os.path.isfile(bin_json_path) and os.path.isfile(binaries_in_bin_path):
        try:
            # Читаем bin.json — строим индекс path -> hash
            with open(bin_json_path, 'r', encoding='utf-8', errors='replace') as f:
                bin_data = json.load(f)
            if isinstance(bin_data, list):
                raw_files = bin_data
            elif 'signatures' in bin_data:
                raw_files = bin_data['signatures']
            else:
                raw_files = bin_data.get('files', [])

            # Строим индекс по нормализованному пути
            path_to_hash = {}
            # Родители файла — хеши вложенных архивов по пути, от ближнего
            # к дальнему. Нужны для наследования происхождения от контейнера
            # (см. _container_fallback в resolve_origins): трасса не связывает
            # архив с его содержимым ни в одну сторону, поэтому единственная
            # надёжная связь "файл внутри пакета <-> пакет" — эта, посчитанная
            # нами при распаковке.
            path_to_parents = {}
            # hash -> путь, чтобы по хешу родителя узнать его имя и расширение
            bin_hash_to_path = {}
            for item in raw_files:
                p = item.get('path', '').strip()
                h = item.get('hash', '').strip()
                if p and h:
                    # Нормализуем путь — убираем ведущий слеш если есть
                    p_norm = p.lstrip('/')
                    path_to_hash[p_norm] = h
                    path_to_hash[p] = h  # также оригинальный путь
                    bin_hash_to_path.setdefault(h, p_norm)
                    par = []
                    ph = (item.get('parents_hash') or '').strip()
                    if ph:
                        par.append(ph)
                    for x in (item.get('parents_chain') or []):
                        x = (x or '').strip()
                        if x and x not in par:
                            par.append(x)
                    if par:
                        path_to_parents[p_norm] = par

            print(_ts() + "   bin.json loaded: {} entries, "
                  "с родителями: {}".format(
                      len(path_to_hash), len(path_to_parents)))

            # Читаем binaries_in_bin.txt
            loaded = 0
            skipped_type = 0
            skipped_hash = 0
            with open(binaries_in_bin_path, 'r', encoding='utf-8', errors='replace') as f:
                for lineno, line in enumerate(f):
                    raw = line
                    line = line.strip()
                    if not line or line.startswith('TYPE') or line.startswith('---'):
                        continue
                    parts = line.split(None, 1)
                    if len(parts) < 2:
                        continue
                    ftype = parts[0].strip()
                    fpath = parts[1].strip()
                    if lineno < 8:
                        print(_ts() + "   line {}: type={!r} path={!r}".format(
                            lineno, ftype, fpath[:80]))
                    if ftype not in ('ELF', 'PE32', 'MSDOS', 'BINARY_EXT'):
                        skipped_type += 1
                        continue
                    fpath_with_prefix = "{}/{}".format(project_name, fpath)
                    # Нормализуем — убираем суффиксы _dir добавленные analyze-ext
                    # bin/foo.iso_dir/bar.deb_dir/file → KTDL.../bin/foo.iso/bar.deb/file
                    import re
                    fpath_norm = re.sub(r'_dir(?=/|$)', '', fpath_with_prefix)
                    h = (path_to_hash.get(fpath_norm) or
                         path_to_hash.get(fpath_with_prefix) or
                         path_to_hash.get(fpath) or
                         path_to_hash.get(fpath.lstrip('/')))
                    if h:
                        _e = {'path': fpath_norm, 'hash': h}
                        _par = (path_to_parents.get(fpath_norm) or
                                path_to_parents.get(fpath_with_prefix) or
                                path_to_parents.get(fpath))
                        if _par:
                            _e['parents'] = _par
                        bin_entries.append(_e)
                        loaded += 1
                    else:
                        skipped_hash += 1
                        if skipped_hash <= 3:
                            print(_ts() + "   hash not found for: {!r}".format(
                                fpath_norm[:100]))
            print(_ts() + "   binaries_in_bin.txt: loaded={}, skipped_type={}, skipped_hash={}".format(
                loaded, skipped_type, skipped_hash))

            print(_ts() + "   binaries_in_bin.txt: {} ELF/PE binaries loaded for Pass 4".format(
                len(bin_entries)))
            total_bin_count = len(bin_entries)   # сохраняем количество
        except Exception as e:
            print(_ts() + "   Could not load bin entries for Pass 4: {}".format(e))
            import traceback
            traceback.print_exc()
    elif not os.path.isfile(bin_json_path):
        print(_ts() + "   bin.json not found: {} — Pass 4 will be skipped".format(bin_json_path))
    elif not os.path.isfile(binaries_in_bin_path):
        print(_ts() + "   binaries_in_bin.txt not found: {} — Pass 4 will be skipped".format(
            binaries_in_bin_path))
        print(_ts() + "   Run analyze-ext_v3.sh first to generate binaries_in_bin.txt")

    # src_hashes — множество хешей из src.json (все загруженные signatures)
    src_hashes = {
        entry.get('hash', '').strip()
        for entry in signatures
        if entry.get('hash', '').strip()
    }

    # --- Проход 1 ---
    print(_ts() + "   Starting pass 1 (hash analysis)...")
    direct, parent, redundant = analyze_pass1(signatures, buildography_hashes)

    # Срез для Прохода 3: какие ИЗ ИНТЕРПРЕТИРУЕМЫХ файлов вообще
    # встречаются в трассе сборки. Нужен, чтобы отличить "использование не
    # прослеживается" от "не используется" (см. use_untraceable).
    #
    # Считаем именно пересечение, а не держим buildography_hashes до Прохода 3:
    # на крупных изделиях это миллионы хешей, а здесь нужно не более числа
    # интерпретируемых файлов — на NPUR.69035-01 около девяти тысяч.
    interp_seen_in_trace = set()
    for _e in signatures:
        if not is_interpreted_extension(_e.get('path', '')):
            continue
        _h = (_e.get('hash') or '').strip()
        if _h and _h in buildography_hashes:
            interp_seen_in_trace.add(_h)
    print(_ts() + "   Интерпретируемых файлов, встречающихся в трассе: "
                  "{}".format(len(interp_seen_in_trace)))

    # buildography_hashes больше не нужен
    del buildography_hashes
    gc.collect()
    print(_ts() + "   Pass 1 done. Memory freed: buildography_hashes")

    # Разбиваем redundant на:
    #   redundant          — НЕ в buildography И НЕ в bin.json (истинно избыточные)
    #   untraced_in_distrib — НЕ в buildography НО в bin.json (попал мимо трассировщика)
    bin_hashes_set = set(bin_hashes.values()) if isinstance(bin_hashes, dict) else set(bin_hashes)
    true_redundant      = []
    untraced_in_distrib = []
    for entry in redundant:
        h = entry.get('hash', '').strip()
        if h and h in bin_hashes_set:
            untraced_in_distrib.append(entry)
        else:
            true_redundant.append(entry)
    redundant = true_redundant
    print(_ts() + "   Pass 1 split: redundant={}, untraced_in_distrib={}".format(
        len(redundant), len(untraced_in_distrib)))

    # --- Проход 2 (компиляторы) ---
    all_good_cmds = None  # будет передан в pass3
    if compiler_basenames:
        print(_ts() + "   Starting pass 2 (transitive closure from bin using compilers)...")
        # Строим транзитивный граф — он пригодится и для pass2 и для pass3
        all_good_cmds = build_transitive_good_commands(raw_cmds, bin_hashes, bin_paths)

        # Отбираем входы компиляторов из хороших команд
        good_compiler_input_keys = set()
        compiler_linker = set(compiler_basenames) | set(linker_basenames or set())
        for idx in all_good_cmds:
            cmd = raw_cmds[idx]
            cmd_list = cmd.get('command', [])
            if not cmd_list:
                continue
            if os.path.basename(cmd_list[0]) not in compiler_linker:
                continue
            deps = cmd.get('dependencies', {})
            if isinstance(deps, dict):
                for path, h in deps.items():
                    if h: good_compiler_input_keys.add(h.strip())
                    if path: good_compiler_input_keys.add(os.path.normpath(path))
            elif isinstance(deps, list):
                for dep in deps:
                    if isinstance(dep, dict):
                        h = dep.get('hash', '').strip()
                        path = dep.get('path', '').strip()
                    else:
                        h, path = '', str(dep).strip()
                    if h: good_compiler_input_keys.add(h)
                    if path: good_compiler_input_keys.add(os.path.normpath(path))

        print(_ts() + "   Good compiler input keys: {}".format(len(good_compiler_input_keys)))
        direct, parent, redundant, not_compiled = analyze_pass2(
            direct, parent, redundant, good_compiler_input_keys
        )
        del good_compiler_input_keys
        gc.collect()
        print(_ts() + "   Pass 2 done. Memory freed: good_compiler_input_keys")
    else:
        print(_ts() + "   Pass 2 skipped (no compiler list)")
        not_compiled = []

    # --- Проход 3 (интерпретаторы) ---
    if interpreter_basenames:
        print(_ts() + "   Starting pass 3 (interpreted languages)...")
        input_files, output_files = build_interpreted_files_with_cmds(raw_cmds, interpreter_basenames)
        print(_ts() + "   Interpreted input files: {}, output files: {}".format(len(input_files), len(output_files)))
        (executed, compiled_used, compiled_unused, copied,
         use_untraceable, izb) = analyze_interpreted(
            signatures, input_files, output_files, bin_hashes, bin_paths, raw_cmds,
            all_good_cmds=all_good_cmds,
            seen_in_trace=interp_seen_in_trace
        )
        del input_files, output_files
        gc.collect()
        print(_ts() + "   Pass 3 done. Memory freed: input_files, output_files")
    else:
        print(_ts() + "   Pass 3 skipped (no interpreter list)")
        executed = compiled_used = compiled_unused = copied = izb = []
        use_untraceable = []

    # --- Проход 4 (происхождение файлов дистрибутива) ---
    # Освобождаем raw_cmds ДО Pass 4 — Pass 4 перечитает файлы сам
    del raw_cmds
    gc.collect()
    print(_ts() + "   raw_cmds freed before pass 4")

    if bin_entries:
        print(_ts() + "   Starting pass 4 (distrib origin check)...")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        p4_compiled_from_src, p4_binaries_from_src, p4_untraced_from_src, \
        p4_external_built, p4_external_prebuilt, p4_untraced_external, \
        p4_system_binaries, p4_apt_package_content = analyze_pass4(
            bin_entries, src_hashes, buildography_files, script_dir,
            compiler_basenames=compiler_basenames,
            linker_basenames=linker_basenames
        )

        # src_hashes нужен дальше для разбора происхождения — он удаляется
        # после java-эвристики, которая досыпает записи в compiled_from_src.
        del bin_entries
        gc.collect()

        # Выделяем содержимое внешних пакетов из untraced_external
        print(_ts() + "   Starting external package content classification...")
        p4_external_package_content, p4_untraced_external = \
            classify_external_package_content(
                p4_untraced_external, buildography_files)

        # Объединяем apt_package_content с external_package_content
        p4_external_package_content.extend(p4_apt_package_content)
        print(_ts() + "   external_package_content={} (incl. apt={}), untraced_external={}".format(
            len(p4_external_package_content), len(p4_apt_package_content),
            len(p4_untraced_external)))

        pass4_ran = True
        print(_ts() + "   Pass 4 done. Memory freed: bin_entries, src_hashes")

        # =====================================================================
        # Java эвристика (TRUST_JAVA)
        # Если trust_java=True: .class файлы чьё базовое имя совпадает
        # с .java файлом из src.json → переносим из external_built в compiled_from_src
        # Покрывает внутренние классы: File$Inner.class → File.java
        # =====================================================================
        if trust_java and p4_external_built:
            print(_ts() + "   Pass 4: applying Java trust heuristic...")

            # Строим индекс java basename → путь из src.json
            java_basenames = {}
            for sig in signatures:
                p = sig.get('path', '')
                if p.lower().endswith('.java'):
                    bn = os.path.splitext(os.path.basename(p))[0].lower()
                    java_basenames[bn] = p

            print(_ts() + "   Pass 4: java_basenames from src.json: {}".format(
                len(java_basenames)))

            def _is_java_dep_trusted(dep_path):
                """
                Возвращает True если dep является .class файлом
                у которого есть соответствующий .java в src.json.
                Учитывает внутренние классы: File$Inner.class → File.java
                """
                if not dep_path.lower().endswith('.class'):
                    return False
                bn   = os.path.basename(dep_path)
                stem = os.path.splitext(bn)[0]
                base = stem.split('$')[0].lower()
                return base in java_basenames

            still_external    = []
            moved_to_compiled = []
            deps_filtered     = 0

            for e in p4_external_built:
                path = e.get('path', '')
                bn   = os.path.basename(path)

                # Случай 1: сам бинарь — .class файл из src.json
                if bn.lower().endswith('.class'):
                    stem = os.path.splitext(bn)[0]
                    base = stem.split('$')[0].lower()
                    if base in java_basenames:
                        new_e = dict(e)
                        new_e['trust_heuristic'] = True
                        moved_to_compiled.append(new_e)
                        continue

                # Случай 2: бинарь имеет external_deps — фильтруем доверенные .class
                ext_deps = e.get('external_deps', [])
                if ext_deps:
                    # Для Java проектов подозрительными считаем ТОЛЬКО .class файлы
                    # .xml, .properties и другие ресурсы не являются признаком
                    # внешних исходников — игнорируем их при классификации
                    CLASS_EXTS = {'.class'}

                    real_suspicious = [
                        d for d in ext_deps
                        if (os.path.splitext(d.get('path', ''))[1].lower() in CLASS_EXTS
                            and not _is_java_dep_trusted(d.get('path', '')))
                    ]
                    trusted_removed = len(ext_deps) - len(real_suspicious)
                    deps_filtered += trusted_removed

                    if not real_suspicious:
                        # Нет подозрительных .class dep → бинарь чистый
                        new_e = dict(e)
                        new_e.pop('external_deps', None)
                        new_e['trust_heuristic'] = True
                        new_e['trust_heuristic_deps_removed'] = trusted_removed
                        moved_to_compiled.append(new_e)
                        continue
                    elif trusted_removed > 0:
                        # Часть dep убрана — остались только подозрительные .class
                        new_e = dict(e)
                        new_e['external_deps'] = real_suspicious
                        new_e['trust_heuristic_deps_removed'] = trusted_removed
                        still_external.append(new_e)
                        continue

                still_external.append(e)

            print(_ts() + "   Pass 4: Java heuristic: "
                  "moved={} external_built→compiled_from_src, "
                  "deps_filtered={}, still_external={}".format(
                len(moved_to_compiled), deps_filtered, len(still_external)))

            if moved_to_compiled:
                p4_compiled_from_src.extend(moved_to_compiled)
            p4_external_built = still_external

        elif trust_java:
            print(_ts() + "   Pass 4: Java heuristic: external_built is empty, skipping")

        # ------------------------------------------------------------------
        # ПРОИСХОЖДЕНИЕ бинарей дистрибутива.
        #
        # Разбор применяется ко ВСЕМ категориям Pass 4, а не к одной
        # else-ветке. У каждого файла два независимых признака:
        #
        #   СПОСОБ        — как файл появился: собран, пришёл готовым,
        #                   переупакован, подписан. Это категории Pass 4.
        #   ПРОИСХОЖДЕНИЕ — подтверждено ли оно переданными материалами.
        #                   Это origin_*.
        #
        # Замечание может возникнуть по любой оси, и одна не заменяет
        # другую: файл из external_prebuilt, подтверждённый согласованным
        # носителем, всё равно НЕ СОБРАН, и это самостоятельный вопрос.
        # Поэтому записи остаются в своих категориях, им лишь дописываются
        # поля origin/origin_detail/origin_chain/pass4_category.
        #
        # Выполняется ПОСЛЕ java-эвристики и classify_external_package_content,
        # потому что обе перекладывают записи между категориями.
        # ------------------------------------------------------------------
        p4_origin_product, p4_origin_approved = [], []
        p4_origin_download, p4_origin_unresolved = [], []

        _origin_input = []
        for _cat_name, _cat_list in (
                ("compiled_from_src",        p4_compiled_from_src),
                ("binaries_from_src",        p4_binaries_from_src),
                ("untraced_from_src",        p4_untraced_from_src),
                ("external_built",           p4_external_built),
                ("external_prebuilt",        p4_external_prebuilt),
                ("untraced_external",        p4_untraced_external),
                ("system_binaries",          p4_system_binaries),
                ("external_package_content", p4_external_package_content)):
            for _e in _cat_list:
                _e['pass4_category'] = _cat_name
                _origin_input.append(_e)

        if _origin_input:
            # Сколько записей дошло до разбора с родителями. Если ноль при
            # непустом bin_hash_to_path — поле parents потерялось по дороге
            # (запись где-то собрана заново), и резерв по контейнеру работает
            # вхолостую. Именно так 103 файла на NPUR.69035-01 остались
            # неопознанными при полностью исправном резерве.
            _n_par = sum(1 for _e in _origin_input if _e.get('parents'))
            print(_ts() + "   Starting origin resolution for {} entries "
                          "across all Pass 4 categories "
                          "(с родителями: {})...".format(
                              len(_origin_input), _n_par))
            if bin_hash_to_path and not _n_par:
                print(_ts() + "   [WARNING] ни одна запись не донесла parents "
                              "из bin.json — резерв по контейнеру не "
                              "сработает. Проверьте _make_entry.")
            (p4_origin_product, p4_origin_approved,
             p4_origin_download, p4_origin_unresolved) = resolve_origins(
                _origin_input, src_hashes, trusted,
                buildography_files,
                compiler_basenames=compiler_basenames,
                linker_basenames=linker_basenames,
                bin_hash_to_path=bin_hash_to_path)
        del _origin_input

        del src_hashes
        gc.collect()

    else:
        print(_ts() + "   Pass 4 skipped (no bin entries)")
        p4_compiled_from_src = p4_binaries_from_src = p4_untraced_from_src = \
        p4_external_built = p4_external_prebuilt = p4_untraced_external = \
        p4_system_binaries = []
        p4_external_package_content = []
        p4_origin_product = p4_origin_approved = []
        p4_origin_download = p4_origin_unresolved = []
        pass4_ran = False
        del src_hashes
        gc.collect()
        pass4_ran = False

    # Создаём папку try{N} и подпапки для каждого прохода
    izb_base = os.path.join(RESULTS_DIR, project_name, "izb")
    try_dir  = get_try_dir(izb_base, keep=keep)
    os.makedirs(try_dir, exist_ok=True)
    print(_ts() + "   Results directory: {}".format(try_dir))

    pass1_dir = os.path.join(try_dir, "pass1")
    pass2_dir = os.path.join(try_dir, "pass2")
    pass3_dir = os.path.join(try_dir, "pass3")
    pass4_dir = os.path.join(try_dir, "pass4")
    for d in [pass1_dir, pass2_dir, pass3_dir, pass4_dir]:
        os.makedirs(d, exist_ok=True)

    # Отслеживаем непустые txt файлы для summary
    # summary_files[category] = abs_path_to_txt
    summary_src_files = {}  # category -> txt path
    summary_bin_files = {}  # category -> txt path

    def jt(folder, name, category, entries,
           summary_dict=None, summary_key=None):
        """
        Записывает JSON и TXT файлы для категории.
        Если summary_dict и summary_key заданы — регистрирует непустые
        txt файлы для последующего копирования в summary.
        """
        base = os.path.join(folder, "{}_{}".format(project_name, name))
        write_json_result(base + ".json", category, entries)
        nonempty = write_txt_result(base + ".txt", category, entries)
        if summary_dict is not None and summary_key is not None and nonempty:
            summary_dict[summary_key] = base + ".txt"

    # --- Pass 1 ---
    print(_ts() + "   Writing pass 1 results...")
    jt(pass1_dir, "direct",            "direct",    direct)
    jt(pass1_dir, "parent",            "parent",    parent)
    jt(pass1_dir, "redundant-by-hash", "redundant", redundant,
       summary_src_files, "redundant-by-hash")
    jt(pass1_dir, "untraced_in_distrib", "untraced_in_distrib", untraced_in_distrib)

    # --- Pass 2 ---
    print(_ts() + "   Writing pass 2 results...")
    jt(pass2_dir, "compiled_not_copied_to_distr", "compiled_not_copied_to_distr", not_compiled,
       summary_src_files, "compiled_not_copied_to_distr")

    # --- Pass 3 ---
    print(_ts() + "   Writing pass 3 results...")
    jt(pass3_dir, "executed",        "interpreted_executed",        executed)
    jt(pass3_dir, "compiled_used",   "interpreted_compiled_used",   compiled_used)
    jt(pass3_dir, "compiled_unused", "interpreted_compiled_unused", compiled_unused,
       summary_src_files, "compiled_unused")
    jt(pass3_dir, "copied",          "interpreted_copied",          copied)
    jt(pass3_dir, "use_untraceable", "interpreted_use_untraceable", use_untraceable,
       summary_src_files, "use_untraceable")
    jt(pass3_dir, "not_used",        "interpreted_not_used",        izb,
       summary_src_files, "not_used")

    # --- Pass 4 ---
    if pass4_ran:
        print(_ts() + "   Writing pass 4 results...")
        # ------------------------------------------------------------------
        # Категории Pass 4 — отвечают на вопрос "КАК файл появился".
        # Все остаются в try{N} полностью, ни одна не сокращается.
        #
        # В сводку из них идёт только binaries_from_src: готовый бинарь,
        # лежащий в переданных исходных текстах, — это замечание о СПОСОБЕ,
        # и оно не снимается подтверждением происхождения, потому что сам
        # файл находится на проверяемом носителе.
        #
        # Остальные категории в сводку как целое не идут. Их проблемная
        # часть попадает туда через origin_download / origin_unresolved,
        # а подтверждённая не попадает никуда: материалы согласованного
        # носителя в состав проверяемого диска не входят, их сборка
        # проверяется отдельно.
        # ------------------------------------------------------------------
        jt(pass4_dir, "compiled_from_src",  "compiled_from_src",  p4_compiled_from_src)
        jt(pass4_dir, "binaries_from_src",  "binaries_from_src",  p4_binaries_from_src,
           summary_bin_files, "binaries_from_src")
        jt(pass4_dir, "untraced_from_src",  "untraced_from_src",  p4_untraced_from_src)
        jt(pass4_dir, "external_built",     "external_built",     p4_external_built)
        jt(pass4_dir, "external_prebuilt",  "external_prebuilt",  p4_external_prebuilt)
        jt(pass4_dir, "untraced_external",  "untraced_external",  p4_untraced_external)
        jt(pass4_dir, "system_binaries",    "system_binaries",    p4_system_binaries)

        # ------------------------------------------------------------------
        # Срез по ПРОИСХОЖДЕНИЮ — охватывает все категории разом.
        # Свой формат txt с колонками: без указания категории Pass 4
        # читатель не поймёт, пришёл ли файл готовым или был переупакован.
        # ------------------------------------------------------------------
        def ot(name, category, entries, to_summary=False):
            base = os.path.join(pass4_dir, "{}_{}".format(project_name, name))
            write_json_result(base + ".json", category, entries)
            nonempty = write_origin_txt(base + ".txt", category, entries)
            if to_summary and nonempty:
                summary_bin_files[name] = base + ".txt"

        ot("origin_product_src",     "origin_product_src",     p4_origin_product)
        ot("origin_approved_source", "origin_approved_source", p4_origin_approved)
        ot("origin_download",        "origin_download",        p4_origin_download,
           to_summary=True)
        ot("origin_unresolved",      "origin_unresolved",      p4_origin_unresolved,
           to_summary=True)

        # external_package_content — специальный формат, отдельные функции записи.
        # В сводку не идёт: подтверждённое снимается, неподтверждённое придёт
        # через origin_unresolved.
        base_epc = os.path.join(pass4_dir,
                                "{}_external_package_content".format(project_name))
        write_external_package_content_json(base_epc + ".json",
                                            p4_external_package_content)
        write_external_package_content_txt(base_epc + ".txt",
                                           p4_external_package_content)

    # --- Статистика ---
    def pct(n, total):
        return (n / total * 100) if total else 0

    # Компилируемые исходники
    # redundant и not_compiled теперь непересекающиеся категории:
    #   redundant     — файлов НЕТ в buildography (истинно избыточные по хешу)
    #   not_compiled  — файлы БЫЛИ в buildography, но результат не в дистрибутиве
    total_source   = len(direct) + len(parent) + len(redundant) + len(not_compiled)
    n_redundant    = len(redundant)
    n_not_compiled = len(not_compiled)

    # Интерпретируемые файлы
    total_interp   = (len(executed) + len(compiled_used) + len(compiled_unused)
                      + len(copied) + len(use_untraceable) + len(izb))

    # Итого
    total_all      = total_source + total_interp
    total_izb      = n_redundant + n_not_compiled + len(compiled_unused) + len(izb)
    # use_untraceable в избыточные НЕ входит: это не вердикт, а отсутствие
    # вердикта. Показываем третьей строкой, отдельно от используемых и от
    # избыточных, иначе 107 файлов на NPUR.69035-01 молча попадут в одну из
    # двух сторон и читатель об этом не узнает.
    total_unk      = len(use_untraceable)

    sep = "  " + "-" * 48

    print("\n  --- Results for {} ---".format(project_name))

    print("\n  Компилируемые исходники ({} файлов)".format(total_source))
    print(sep)
    print("  Direct (используются напрямую)                     : {:>7}  ({:.1f}%)".format(len(direct),       pct(len(direct),       total_source)))
    print("  Parent (через архив)                               : {:>7}  ({:.1f}%)".format(len(parent),       pct(len(parent),       total_source)))
    print("  Redundant-by-hash (нет в buildography)             : {:>7}  ({:.1f}%)".format(n_redundant,       pct(n_redundant,       total_source)))
    print("  Compiled not copied (в buildography, не в bin)     : {:>7}  ({:.1f}%)".format(n_not_compiled,    pct(n_not_compiled,    total_source)))

    print("\n  Интерпретируемые файлы ({} файлов)".format(total_interp))
    print(sep)
    print("  Executed (запускаются)                             : {:>7}  ({:.1f}%)".format(len(executed),        pct(len(executed),        total_interp)))
    print("  Compiled used (скомпилированы, результат в bin)    : {:>7}  ({:.1f}%)".format(len(compiled_used),   pct(len(compiled_used),   total_interp)))
    print("  Compiled unused (скомпилированы, результат не в bin): {:>7}  ({:.1f}%)".format(len(compiled_unused), pct(len(compiled_unused), total_interp)))
    print("  Copied (есть в дистрибутиве)                       : {:>7}  ({:.1f}%)".format(len(copied),          pct(len(copied),          total_interp)))
    print("  Use untraceable (использование не прослеживается)  : {:>7}  ({:.1f}%)".format(len(use_untraceable), pct(len(use_untraceable), total_interp)))
    print("  Not used (не используются нигде)                   : {:>7}  ({:.1f}%)".format(len(izb),             pct(len(izb),             total_interp)))

    print("\n  Итого ({} файлов)".format(total_all))
    print(sep)
    print("  Используются                                       : {:>7}  ({:.1f}%)".format(total_all - total_izb - total_unk, pct(total_all - total_izb - total_unk, total_all)))
    print("  Избыточные (not_compiled + compiled_unused + not_used): {:>7}  ({:.1f}%)".format(total_izb,             pct(total_izb,             total_all)))
    if total_unk:
        print("  Не прослеживается (требуется пояснение)            : {:>7}  ({:.1f}%)".format(total_unk, pct(total_unk, total_all)))

    if pass4_ran:
        total_bin = (len(p4_compiled_from_src) + len(p4_binaries_from_src) +
                     len(p4_untraced_from_src) + len(p4_external_built) +
                     len(p4_external_prebuilt) + len(p4_untraced_external) +
                     len(p4_system_binaries) + len(p4_external_package_content))
        print("\n  Происхождение бинарей дистрибутива ({} файлов)".format(total_bin))
        print(sep)
        print("  Compiled from src  (появился в ходе сборки)         : {:>7}  ({:.1f}%)".format(
            len(p4_compiled_from_src),  pct(len(p4_compiled_from_src),  total_bin)))
        print("  Binaries from src  (бинарь из src.json, скопирован): {:>7}  ({:.1f}%)".format(
            len(p4_binaries_from_src),  pct(len(p4_binaries_from_src),  total_bin)))
        print("  Untraced from src  (в src.json, трасс. не видит)   : {:>7}  ({:.1f}%)".format(
            len(p4_untraced_from_src),  pct(len(p4_untraced_from_src),  total_bin)))
        print("  External built     (компил. из внешних исх.)       : {:>7}  ({:.1f}%)".format(
            len(p4_external_built),     pct(len(p4_external_built),     total_bin)))
        print("  External prebuilt  (готовый извне, трасс. видит)   : {:>7}  ({:.1f}%)".format(
            len(p4_external_prebuilt),  pct(len(p4_external_prebuilt),  total_bin)))
        print("  Untraced external  (не в src, трасс. не видит)     : {:>7}  ({:.1f}%)".format(
            len(p4_untraced_external),  pct(len(p4_untraced_external),  total_bin)))
        print("  System binaries    (системные пути в дистрибутиве) : {:>7}  ({:.1f}%)".format(
            len(p4_system_binaries),    pct(len(p4_system_binaries),    total_bin)))
        print("  Ext pkg content    (содержимое внешних пакетов)    : {:>7}  ({:.1f}%)".format(
            len(p4_external_package_content),
            pct(len(p4_external_package_content), total_bin)))

        # Вторая ось: разбор ВСЕХ бинарей дистрибутива по происхождению.
        # Таблица выше отвечает на вопрос "как файл появился", эта — на
        # вопрос "подтверждено ли его происхождение переданными материалами".
        _n_origin = (len(p4_origin_product) + len(p4_origin_approved) +
                     len(p4_origin_download) + len(p4_origin_unresolved))
        if _n_origin:
            print("\n  Происхождение тех же файлов ({} записей)".format(_n_origin))
            print(sep)
            print("  Product src      (терминал в src.json изделия)    : {:>7}  ({:.1f}%)".format(
                len(p4_origin_product),    pct(len(p4_origin_product),    _n_origin)))
            print("  Approved source  (согласованный носитель)         : {:>7}  ({:.1f}%)".format(
                len(p4_origin_approved),   pct(len(p4_origin_approved),   _n_origin)))
            print("  Download         (источник вне комплекта)         : {:>7}  ({:.1f}%)".format(
                len(p4_origin_download),   pct(len(p4_origin_download),   _n_origin)))
            print("  Unresolved       (происхождение не подтверждено)  : {:>7}  ({:.1f}%)".format(
                len(p4_origin_unresolved), pct(len(p4_origin_unresolved), _n_origin)))

            _all_origin = (p4_origin_product + p4_origin_approved +
                           p4_origin_download + p4_origin_unresolved)
            _uniq = len({(_e.get('hash') or '') for _e in _all_origin})
            print("  уникальных файлов: {} (записей {})".format(_uniq, _n_origin))

            _ops = {}
            for _e in _all_origin:
                _k = _e.get('operation', 'other')
                _ops[_k] = _ops.get(_k, 0) + 1
            if _ops:
                print("  по операциям: " + ", ".join(
                    "{}={}".format(k, v)
                    for k, v in sorted(_ops.items(), key=lambda x: -x[1])))

            _problem = len(p4_origin_download) + len(p4_origin_unresolved)
            if _problem:
                print("  ТРЕБУЮТ ОБЪЯСНЕНИЯ: {} записей".format(_problem))
    else:
        total_bin = 0

    # =========================================================================
    # SUMMARY TXT: записываем сводку в try_dir/summary.txt
    # =========================================================================
    def _fmt_elapsed(seconds):
        """Форматирует секунды в HH:MM:SS."""
        h = int(seconds) // 3600
        m = (int(seconds) % 3600) // 60
        s = int(seconds) % 60
        return "{:02d}:{:02d}:{:02d}".format(h, m, s)

    _run_elapsed = time.monotonic() - _SCRIPT_START
    _summary_lines = []
    _summary_lines.append("=" * 54)
    _summary_lines.append("  Сводка: {}".format(project_name))
    _summary_lines.append("  Сгенерировано: {}".format(datetime.now().isoformat()))
    _summary_lines.append("  Время выполнения: {}".format(_fmt_elapsed(_run_elapsed)))
    _summary_lines.append("=" * 54)

    _sep = "  " + "-" * 50

    _summary_lines.append("")
    _summary_lines.append("  Компилируемые исходники ({} файлов)".format(total_source))
    _summary_lines.append(_sep)
    _summary_lines.append("  Direct (используются напрямую)                     : {:>7}  ({:.1f}%)".format(len(direct),   pct(len(direct),   total_source)))
    _summary_lines.append("  Parent (через архив)                               : {:>7}  ({:.1f}%)".format(len(parent),   pct(len(parent),   total_source)))
    _summary_lines.append("  Redundant-by-hash (нет в buildography)             : {:>7}  ({:.1f}%)".format(n_redundant,   pct(n_redundant,   total_source)))
    _summary_lines.append("  Compiled not copied (в buildography, не в bin)     : {:>7}  ({:.1f}%)".format(n_not_compiled, pct(n_not_compiled, total_source)))

    _summary_lines.append("")
    _summary_lines.append("  Интерпретируемые файлы ({} файлов)".format(total_interp))
    _summary_lines.append(_sep)
    _summary_lines.append("  Executed (запускаются)                             : {:>7}  ({:.1f}%)".format(len(executed),        pct(len(executed),        total_interp)))
    _summary_lines.append("  Compiled used (скомпилированы, результат в bin)    : {:>7}  ({:.1f}%)".format(len(compiled_used),   pct(len(compiled_used),   total_interp)))
    _summary_lines.append("  Compiled unused (скомпилированы, результат не в bin): {:>7}  ({:.1f}%)".format(len(compiled_unused), pct(len(compiled_unused), total_interp)))
    _summary_lines.append("  Copied (есть в дистрибутиве)                       : {:>7}  ({:.1f}%)".format(len(copied),          pct(len(copied),          total_interp)))
    _summary_lines.append("  Use untraceable (использование не прослеживается)  : {:>7}  ({:.1f}%)".format(len(use_untraceable), pct(len(use_untraceable), total_interp)))
    _summary_lines.append("  Not used (не используются нигде)                   : {:>7}  ({:.1f}%)".format(len(izb),             pct(len(izb),             total_interp)))

    _summary_lines.append("")
    _summary_lines.append("  Итого ({} файлов)".format(total_all))
    _summary_lines.append(_sep)
    _summary_lines.append("  Используются                                       : {:>7}  ({:.1f}%)".format(total_all - total_izb - total_unk, pct(total_all - total_izb - total_unk, total_all)))
    _summary_lines.append("  Избыточные (not_compiled + compiled_unused + not_used): {:>7}  ({:.1f}%)".format(total_izb, pct(total_izb, total_all)))
    if total_unk:
        _summary_lines.append("  Не прослеживается (требуется пояснение)            : {:>7}  ({:.1f}%)".format(total_unk, pct(total_unk, total_all)))

    if pass4_ran:
        _summary_lines.append("")
        _summary_lines.append("  Происхождение бинарей дистрибутива ({} файлов)".format(total_bin))
        _summary_lines.append(_sep)
        _summary_lines.append("  Compiled from src  (появился в ходе сборки)         : {:>7}  ({:.1f}%)".format(len(p4_compiled_from_src),       pct(len(p4_compiled_from_src),       total_bin)))
        _summary_lines.append("  Binaries from src  (бинарь из src.json, скопирован): {:>7}  ({:.1f}%)".format(len(p4_binaries_from_src),       pct(len(p4_binaries_from_src),       total_bin)))
        _summary_lines.append("  Untraced from src  (в src.json, трасс. не видит)   : {:>7}  ({:.1f}%)".format(len(p4_untraced_from_src),       pct(len(p4_untraced_from_src),       total_bin)))
        _summary_lines.append("  External built     (компил. из внешних исх.)       : {:>7}  ({:.1f}%)".format(len(p4_external_built),           pct(len(p4_external_built),          total_bin)))
        _summary_lines.append("  External prebuilt  (готовый извне, трасс. видит)   : {:>7}  ({:.1f}%)".format(len(p4_external_prebuilt),        pct(len(p4_external_prebuilt),       total_bin)))
        _summary_lines.append("  Untraced external  (не в src, трасс. не видит)     : {:>7}  ({:.1f}%)".format(len(p4_untraced_external),        pct(len(p4_untraced_external),       total_bin)))
        _summary_lines.append("  System binaries    (системные пути в дистрибутиве) : {:>7}  ({:.1f}%)".format(len(p4_system_binaries),          pct(len(p4_system_binaries),         total_bin)))
        _summary_lines.append("  Ext pkg content    (содержимое внешних пакетов)    : {:>7}  ({:.1f}%)".format(len(p4_external_package_content), pct(len(p4_external_package_content),total_bin)))

    _summary_lines.append("=" * 54)

    _summary_path = os.path.join(try_dir, "summary.txt")
    with open(_summary_path, 'w', encoding='utf-8') as _sf:
        _sf.write("\n".join(_summary_lines) + "\n")
    print(_ts() + "   Summary written: {}".format(_summary_path))

    # =========================================================================
    # SUMMARY: копируем непустые отчёты в summary{N}/
    # =========================================================================
    import shutil as _shutil
    try_name     = os.path.basename(try_dir)          # try1, try2, ...
    summary_name = try_name.replace("try", "summary") # summary1, summary2, ...
    summary_dir  = os.path.join(os.path.dirname(try_dir), summary_name)
    summary_src_dir = os.path.join(summary_dir, "src")
    summary_bin_dir = os.path.join(summary_dir, "bin")
    os.makedirs(summary_src_dir, exist_ok=True)
    os.makedirs(summary_bin_dir, exist_ok=True)
    print(_ts() + "   Summary directory: {}".format(summary_dir))

    for key, src_path in summary_src_files.items():
        dst = os.path.join(summary_src_dir, os.path.basename(src_path))
        _shutil.copy2(src_path, dst)
        print(_ts() + "   Summary src: {}".format(os.path.basename(dst)))

    for key, src_path in summary_bin_files.items():
        dst = os.path.join(summary_bin_dir, os.path.basename(src_path))
        _shutil.copy2(src_path, dst)
        print(_ts() + "   Summary bin: {}".format(os.path.basename(dst)))

    # binaries_in_src.txt из ext/ — только если в нём есть хоть одна запись.
    #
    # Файл всегда существует и всегда содержит двухстрочную шапку
    # ("TYPE PATH" и черта), поэтому проверки os.path.isfile недостаточно:
    # в сводку уезжал пустой файл на 38 байт. Считаем содержательные строки.
    binaries_in_src_path = os.path.join(
        RESULTS_DIR, project_name, "ext", "binaries_in_src.txt")
    if os.path.isfile(binaries_in_src_path):
        n_bins = 0
        try:
            with open(binaries_in_src_path, 'r', encoding='utf-8',
                      errors='replace') as f:
                for line in f:
                    t = line.strip()
                    if t and not t.startswith(('TYPE', '---', '#')):
                        n_bins += 1
        except OSError:
            n_bins = 0
        if n_bins:
            dst = os.path.join(summary_bin_dir,
                               "{}_binaries_in_src.txt".format(project_name))
            _shutil.copy2(binaries_in_src_path, dst)
            print(_ts() + "   Summary bin: {} ({} записей)".format(
                os.path.basename(dst), n_bins))
        else:
            print(_ts() + "   binaries_in_src.txt пуст — в сводку не копируем")

    print(_ts() + "   Summary done: src={} files, bin={} files".format(
        len(summary_src_files), len(summary_bin_files)))

    # =========================================================================
    # README для summary
    # =========================================================================
    SUMMARY_DESCRIPTIONS = {
        # src/
        "redundant-by-hash": (
            "src/",
            "Избыточные исходные файлы",
            "Файлы из репозитория исходников которые не найдены в buildography\n"
            "  и отсутствуют в дистрибутиве."
        ),
        "compiled_not_copied_to_distr": (
            "src/",
            "Компилируемые файлы результат которых не в дистрибутиве",
            "Исходные файлы компилируемых языков (.c, .cpp, .rs, .go и др.)\n"
            "  которые компилировались в ходе сборки, но результат компиляции\n"
            "  не попал в дистрибутив."
        ),
        "compiled_unused": (
            "src/",
            "Интерпретируемые файлы скомпилированные но не в дистрибутиве",
            "Файлы интерпретируемых языков (.py и др.) которые компилировались\n"
            "  (.pyc и т.д.), но результат компиляции не попал в дистрибутив."
        ),
        "not_used": (
            "src/",
            "Интерпретируемые файлы нигде не используемые",
            "Файлы интерпретируемых языков которые не запускались, не\n"
            "  компилировались и не скопированы в дистрибутив. Их хеши не\n"
            "  встречаются в трассе сборки ни у одной команды, то есть при\n"
            "  сборке к этим файлам никто не обращался."
        ),
        "use_untraceable": (
            "src/",
            "Файлы, использование которых не прослеживается",
            "Хеши этих файлов встречаются в трассе сборки, то есть при сборке\n"
            "  файлы присутствовали и к ним обращались, но ни один\n"
            "  интерпретатор их не использовал и в дистрибутив они не попали\n"
            "  ни сами, ни как результат компиляции.\n"
            "\n"
            "  ЭТО НЕ СПИСОК НА УДАЛЕНИЕ. Отсутствие доказательств\n"
            "  использования не равно доказательству неиспользования.\n"
            "  Типичная причина — сборка в один самодостаточный файл: исходные\n"
            "  тексты вшиваются внутрь бандла, поэтому по хешу не совпадают ни\n"
            "  с чем в дистрибутиве, а сама упаковка в трассу не попадает\n"
            "  (у упаковщиков и архиваторов входные данные не записываются).\n"
            "\n"
            "  Требуется пояснение разработчика по каждой группе файлов: куда\n"
            "  они входят и каким инструментом собираются. После пояснения\n"
            "  файл либо подтверждается как используемый, либо переходит в\n"
            "  избыточные и подлежит удалению с носителя исходных текстов."
        ),
        # bin/
        "origin_download": (
            "bin/",
            "Бинари, полученные по сети",
            "Файлы дистрибутива, цепочка происхождения которых заканчивается сетевой\n"
            "  загрузкой (pip download, wget, curl, apt download). При этом полученный\n"
            "  файл не совпал по хешу ни с одним файлом согласованных носителей.\n"
            "  Происхождение проверяется в следующем порядке: исходные тексты изделия,\n"
            "  согласованный носитель, сетевая загрузка. Согласованные носители\n"
            "  проверяются РАНЬШЕ загрузки, поэтому сюда попадает только то, что\n"
            "  скачано И не найдено среди переданных материалов, то есть получено\n"
            "  из источника вне комплекта поставки.\n"
            "  В поле origin_detail указан адрес источника из командной строки.\n"
            "  Проверка выполняется для ВСЕХ бинарей дистрибутива, независимо от\n"
            "  того, как они появились (компиляция, сборка пакета, переупаковка,\n"
            "  подпись, копирование). Подтверждённые файлы в сводку не попадают."
        ),
        "origin_unresolved": (
            "bin/",
            "Бинари, происхождение которых установить не удалось",
            "Файлы дистрибутива, для которых обратный обход по трассе сборки не привёл\n"
            "  ни к исходным текстам изделия, ни к согласованным носителям, ни к\n"
            "  сетевой загрузке.\n"
            "  Причина указана в поле origin_detail: цепочка прослежена до конца и\n"
            "  опознанный источник не найден; у производящей команды нет\n"
            "  прослеживаемых входов; обход упёрся в предел глубины.\n"
            "  В поле origin_chain — последовательность операций от файла назад,\n"
            "  например: signed(bsign) <- compiled(ld) <- compiled(as).\n"
            "  Проверка выполняется для ВСЕХ бинарей дистрибутива. Файлы, чьё\n"
            "  происхождение подтверждено исходными текстами изделия или\n"
            "  согласованным носителем, в сводку не попадают: они перечислены\n"
            "  в try{N}/pass4/ и вопросов не вызывают."
        ),
        "external_built": (
            "bin/",
            "Бинари, в которые попали сторонние исходные тексты",
            "Бинари дистрибутива собранные с использованием исходных текстов\n"
            "  которых нет в переданных исходниках (src.json)."
        ),
        "external_prebuilt": (
            "bin/",
            "Готовые бинари полученные извне",
            "Пришли готовыми извне (apt, wget, prebuilt)."
        ),
        "untraced_external": (
            "bin/",
            "Бинари полностью неизвестного происхождения",
            "Бинари дистрибутива происхождение которых не установлено: их нет\n"
            "  в исходниках (src.json), и трассировщик не видел ни их сборки,\n"
            "  ни их получения через пакетные менеджеры."
        ),
        "external_package_content": (
            "bin/",
            "Содержимое внешних пакетов (deb/pip/npm)",
            "Файлы внутри внешних пакетов источник которых известен\n"
            "  трассировщику (apt download, pip install, npm install и т.д.)."
        ),
        "binaries_from_src": (
            "bin/",
            "Готовые бинари, лежащие прямо в исходных текстах",
            "Готовые ELF/PE бинари, которые хранятся прямо в переданных исходных\n"
            "  текстах (src.json) и скопированы в дистрибутив без сборки.\n"
            "  Бинарь дистрибутива должен быть собран из исходных текстов, а не\n"
            "  принесён в них готовым: по такому файлу нельзя проверить, из чего\n"
            "  он сделан, даже если он найден в src.json. Требуется либо убрать\n"
            "  файл и собирать его из исходных текстов, либо дать пояснение,\n"
            "  почему он поставляется в готовом виде."
        ),
        "binaries_in_src": (
            "bin/",
            "Полный список ELF/PE бинарей в переданных исходных текстах",
            "Полный список всех ELF/PE бинарей обнаруженных в переданных\n"
            "  исходных текстах."
        ),
        "system_binaries": (
            "bin/",
            "Системные бинари, обнаруженные в дистрибутиве.",
            "Бинари расположенные по системным путям внутри дистрибутива\n"
            "  (/usr/, /lib/ и т.д.)."
        ),
    }

    # Строим README только по файлам которые реально попали в summary
    def _readme_entry(fname, section, title, descr):
        # Пустая строка перед записью и черта под заголовком.
        # Без них имя следующего файла идёт вплотную за текстом предыдущего
        # описания с тем же отступом, и границы записей на глаз неразличимы —
        # тем хуже, чем длиннее описания.
        lines = ["", "  {}".format(fname), "  {}".format(title)]
        if descr:
            lines.append("  " + "-" * 60)
            lines.append("  {}".format(descr))
        return lines

    readme_lines = []
    readme_lines.append("# Summary Report — {}".format(project_name))
    readme_lines.append("# Generated: {}".format(datetime.now().isoformat()))
    readme_lines.append("Краткая справка по отчётам в этой папке.")
    readme_lines.append("Отчёты — текстовые, колонки разделены табуляцией.")
    readme_lines.append("  обычный отчёт : путь<TAB>хеш")
    readme_lines.append("  origin_*      : путь<TAB>хеш<TAB>категория<TAB>операция<TAB>origin_detail")
    readme_lines.append("В шапке каждого отчёта — число записей и число уникальных файлов:")
    readme_lines.append("  это разные числа, один файл попадает в отчёт столько раз,")
    readme_lines.append("  сколько у него путей внутри носителя.")

    # src/ секция
    src_entries = []
    for key, fpath in summary_src_files.items():
        fname = os.path.basename(fpath)
        short = fname.replace("{}_".format(project_name), "").replace(".txt", "")
        desc = SUMMARY_DESCRIPTIONS.get(short)
        if desc:
            src_entries.append((short, desc, fname))

    if src_entries:
        readme_lines.append("=" * 60)
        # Не "избыточные": в секции теперь есть и use_untraceable, который
        # избыточным не является — по нему вердикта нет.
        readme_lines.append(
            "src/  — исходные файлы, требующие решения")
        readme_lines.append("=" * 60)
        for short, (section, title, descr), fname in src_entries:
            readme_lines.extend(_readme_entry(fname, section, title, descr))

    # bin/ секция
    bin_entries_readme = []
    for key, fpath in summary_bin_files.items():
        fname = os.path.basename(fpath)
        short = fname.replace("{}_".format(project_name), "").replace(".txt", "")
        desc = SUMMARY_DESCRIPTIONS.get(short)
        if desc:
            bin_entries_readme.append((short, desc, fname))

    # binaries_in_src всегда в bin/ если скопирован
    bins_in_src_dst = os.path.join(
        summary_bin_dir, "{}_binaries_in_src.txt".format(project_name))
    if os.path.isfile(bins_in_src_dst):
        fname = os.path.basename(bins_in_src_dst)
        bin_entries_readme.append(
            ("binaries_in_src", SUMMARY_DESCRIPTIONS["binaries_in_src"], fname))

    if bin_entries_readme:
        readme_lines.append("=" * 60)
        readme_lines.append(
            "bin/  — бинари, происхождение которых не подтверждено")
        readme_lines.append("=" * 60)
        for short, (section, title, descr), fname in bin_entries_readme:
            readme_lines.extend(_readme_entry(fname, section, title, descr))

    # Пустая сводка — это результат, а не сбой. Без явной строки папка с
    # одним README читается как "что-то не отработало".
    if not src_entries and not bin_entries_readme:
        readme_lines.append("=" * 60)
        readme_lines.append("Замечаний нет.")
        readme_lines.append("=" * 60)
        readme_lines.append("")
        readme_lines.append("  Избыточных исходных файлов не обнаружено.")
        readme_lines.append("  Все бинари дистрибутива прослежены до исходных")
        readme_lines.append("  текстов изделия либо до согласованных носителей.")
        readme_lines.append("")
        readme_lines.append("  Полные результаты разбора — в соседней папке {}/".format(
            os.path.basename(try_dir)))

    readme_path = os.path.join(summary_dir, "README.txt")
    with open(readme_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(readme_lines) + "\n")
    print(_ts() + "   Summary README: {}".format(readme_path))

    # --- Pass 5 (по дискам, если флаг --by-disk) ---
    if by_disk:
        print(_ts() + "   Starting pass 5 (breakdown by disk)...")
        run_pass5(
            try_dir, project_name,
            redundant, not_compiled, izb, compiled_unused,
            p4_compiled_from_src, p4_binaries_from_src, p4_untraced_from_src,
            p4_external_built, p4_external_prebuilt,
            p4_untraced_external, p4_system_binaries
        )
    elif not by_disk:
        print(_ts() + "   Pass 5 skipped (use --by-disk to enable)")

    return True


# =============================================================================
# ПРОХОД 5: разбивка избыточных файлов по дискам
# =============================================================================

def _get_disk(path, separator):
    """
    Извлекает имя диска из пути — первый компонент после separator ('src' или 'bin').
    Например:
      KTDL.00554-01/src/DISK01/file.c → DISK01
      KTDL.00554-01/bin/12_05_DISK02.iso_dir/... → 12_05_DISK02.iso_dir
    """
    parts = path.split('/')
    try:
        idx = parts.index(separator)
        return parts[idx + 1] if idx + 1 < len(parts) else 'unknown'
    except ValueError:
        return 'unknown'


def group_by_disk(entries, separator):
    """Группирует записи по диску. separator = 'src' или 'bin'."""
    groups = {}
    for entry in entries:
        disk = _get_disk(entry.get('path', ''), separator)
        groups.setdefault(disk, []).append(entry)
    return groups


def run_pass5(try_dir, project_name,
              # src категории
              redundant, not_compiled, not_used, compiled_unused,
              # bin категории
              p4_compiled_from_src, p4_binaries_from_src, p4_untraced_from_src,
              p4_external_built, p4_external_prebuilt,
              p4_untraced_external, p4_system_binaries):
    """
    Pass 5: разбивка избыточных файлов по дискам.
    Создаёт папку pass5/src/ и pass5/bin/ с файлами для каждого диска.
    """
    pass5_dir     = os.path.join(try_dir, "pass5")
    pass5_src_dir = os.path.join(pass5_dir, "src")
    pass5_bin_dir = os.path.join(pass5_dir, "bin")
    os.makedirs(pass5_src_dir, exist_ok=True)
    os.makedirs(pass5_bin_dir, exist_ok=True)

    print(_ts() + "   Pass 5: breakdown by disk...")

    def jt5(folder, disk, name, category, entries):
        base = os.path.join(folder, "{}_{}_{}.".format(disk, project_name, name))
        write_json_result(base + "json", category, entries)
        write_txt_result(base + "txt",  category, entries)

    # --- SRC ---
    src_categories = [
        ("redundant-by-hash", "redundant",         redundant),
        ("compiled_not_copied_to_distr", "compiled_not_copied_to_distr", not_compiled),
        ("not_used",          "interpreted_not_used", not_used),
        ("compiled_unused",   "interpreted_compiled_unused", compiled_unused),
    ]
    for name, category, entries in src_categories:
        groups = group_by_disk(entries, 'src')
        for disk, disk_entries in sorted(groups.items()):
            jt5(pass5_src_dir, disk, name, category, disk_entries)
        print(_ts() + "   Pass 5 src {}: {} disks".format(name, len(groups)))

    # --- BIN ---
    bin_categories = [
        ("compiled_from_src",  "compiled_from_src",  p4_compiled_from_src),
        ("binaries_from_src",  "binaries_from_src",  p4_binaries_from_src),
        ("untraced_from_src",  "untraced_from_src",  p4_untraced_from_src),
        ("external_built",     "external_built",     p4_external_built),
        ("external_prebuilt",  "external_prebuilt",  p4_external_prebuilt),
        ("untraced_external",  "untraced_external",  p4_untraced_external),
        ("system_binaries",    "system_binaries",    p4_system_binaries),
    ]
    for name, category, entries in bin_categories:
        groups = group_by_disk(entries, 'bin')
        for disk, disk_entries in sorted(groups.items()):
            jt5(pass5_bin_dir, disk, name, category, disk_entries)
        print(_ts() + "   Pass 5 bin {}: {} disks".format(name, len(groups)))

    print(_ts() + "   Pass 5 done. Results: {}".format(pass5_dir))


# =============================================================================
# MAIN
# =============================================================================
def get_all_projects():
    if not os.path.isdir(BUILDOGRAPHY_DIR):
        return []
    return sorted([
        entry for entry in os.listdir(BUILDOGRAPHY_DIR)
        if os.path.isdir(os.path.join(BUILDOGRAPHY_DIR, entry))
    ])


def main():
    parser = argparse.ArgumentParser(
        description='Анализ происхождения файлов сборки',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '-p', '--single-project',
        metavar='NAME',
        help='Обработать только один указанный проект. '
             'Без флага обрабатываются все проекты из buildography/builds/.'
    )
    parser.add_argument(
        '-d', '--by-disk',
        action='store_true',
        default=False,
        help='Включить Проход 5: разбить результаты по дискам '
             '(pass5/src/ и pass5/bin/).'
    )
    parser.add_argument(
        '-k', '--keep',
        action='store_true',
        default=False,
        help='Сохранять предыдущие результаты: создавать tryN вместо '
             'перезаписи try1. По умолчанию try1 перезаписывается, '
             'а try2, try3, ... удаляются.'
    )
    parser.add_argument(
        '--trust-java',
        action='store_true',
        default=None,
        help='Включить Java-эвристику: .class файлы чьё имя совпадает '
             'с .java из src.json считаются compiled_from_src. '
             'По умолчанию берётся значение TRUST_JAVA из скрипта.'
    )
    parser.add_argument(
        '--no-trust-java',
        action='store_true',
        default=False,
        help='Отключить Java-эвристику независимо от TRUST_JAVA.'
    )
    args = parser.parse_args()

    # Определяем финальное значение trust_java
    if args.no_trust_java:
        args.trust_java = False
    elif args.trust_java is None:
        args.trust_java = TRUST_JAVA

    if not os.path.isdir(BUILDOGRAPHY_DIR):
        print(_ts() + " Buildography directory not found: {}".format(BUILDOGRAPHY_DIR))
        sys.exit(1)

    if not os.path.isdir(RESULTS_DIR):
        print(_ts() + " Results directory not found: {}".format(RESULTS_DIR))
        sys.exit(1)

    compiler_basenames, linker_basenames, interpreter_basenames = load_utilities_lists(UTILITIES_FILE)

    if args.single_project:
        project_dir = os.path.join(BUILDOGRAPHY_DIR, args.single_project)
        if not os.path.isdir(project_dir):
            print(_ts() + " Project not found: {}".format(project_dir))
            sys.exit(1)
        projects = [args.single_project]
    else:
        projects = get_all_projects()
        if not projects:
            print(_ts() + " No projects found in: {}".format(BUILDOGRAPHY_DIR))
            sys.exit(1)

    print(_ts() + " Projects to analyze: {}".format(len(projects)))
    print(_ts() + " Projects: {}".format(', '.join(projects)))
    print(_ts() + " UTILITIES_FILE: {}".format(UTILITIES_FILE))
    print(_ts() + " Compilers: {}, Linkers: {}, Interpreters: {}".format(
        len(compiler_basenames), len(linker_basenames), len(interpreter_basenames)))
    print(_ts() + " Keep previous results: {}".format(args.keep))

    # =========================================================================
    # Проверяем наличие результатов analyze-ext.sh (binaries_in_bin.txt)
    # =========================================================================
    missing_ext = []
    for project_name in projects:
        binaries_in_bin_path = os.path.join(
            RESULTS_DIR, project_name, "ext", "binaries_in_bin.txt")
        if not os.path.isfile(binaries_in_bin_path):
            missing_ext.append(project_name)

    if missing_ext:
        print()
        print(_ts() + " [WARN] binaries_in_bin.txt не найден для проектов:")
        for p in missing_ext:
            print(_ts() + "   - {}".format(p))
        print(_ts() + " Без этого файла Проход 4 (анализ бинарей) будет пропущен.")
        print(_ts() + " Файл генерируется скриптом analyze-ext.sh.")
        print()
        try:
            answer = input(" Запустить analyze-ext.sh сейчас? [y/N]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            answer = 'n'

        if answer in ('y', 'yes', 'д', 'да'):
            analyze_ext_sh = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "analyze-ext.sh")
            if not os.path.isfile(analyze_ext_sh):
                print(_ts() + " [ERROR] analyze-ext.sh not found: {}".format(
                    analyze_ext_sh))
            else:
                import subprocess
                for project_name in missing_ext:
                    print(_ts() + " Running analyze-ext.sh for {}...".format(
                        project_name))
                    cmd = ["bash", analyze_ext_sh,
                           "--single-project", project_name]
                    ret = subprocess.call(cmd)
                    if ret != 0:
                        print(_ts() + " [WARN] analyze-ext.sh exited with code {}".format(ret))
                    else:
                        print(_ts() + " analyze-ext.sh done for {}".format(
                            project_name))
        else:
            print(_ts() + " Продолжаем без analyze-ext.sh. Проход 4 будет пропущен.")

    # =========================================================================
    start_time = datetime.now()
    results = {}

    print(_ts() + " Java trust heuristic: {}".format(
        "ON" if args.trust_java else "OFF"))

    for project_name in projects:
        results[project_name] = process_project(
            project_name, compiler_basenames, linker_basenames, interpreter_basenames,
            by_disk=args.by_disk, keep=args.keep, trust_java=args.trust_java
        )

    elapsed = datetime.now() - start_time
    success_count = sum(1 for v in results.values() if v)
    fail_count = len(results) - success_count

    print("\n" + "=" * 50)
    print("Analysis complete!")
    print("=" * 50)
    print("  Total projects  : {}".format(len(projects)))
    print("  Successful      : {}".format(success_count))
    print("  Failed          : {}".format(fail_count))
    print("  Time elapsed    : {}".format(elapsed))
    print("  Output          : {}".format(RESULTS_DIR))

    if fail_count > 0:
        print("\n  Failed projects:")
        for name, ok in results.items():
            if not ok:
                print("    - {}".format(name))

    print("=" * 50)
    sys.exit(0 if fail_count == 0 else 1)


if __name__ == '__main__':
    main()
