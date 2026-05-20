#!/usr/bin/env python3
"""
=============================================================================
analyze-json.py - Скрипт анализа избыточных файлов исходных текстов
=============================================================================

ОПИСАНИЕ:
    Сравнивает JSON сигнатур исходных текстов с JSON buildography и
    распределяет файлы по категориям. Добавлена поддержка Python-файлов,
    которые были выполнены (interpreted_executed) с выводом команд запуска.
    Проход 4 проверяет происхождение файлов дистрибутива.

ИСПОЛЬЗОВАНИЕ:
    python3 analyze-json.py [OPTIONS]

ОПЦИИ:
    --single-project NAME   Обработать только один указанный проект
    -h, --help              Показать справку

ПРИМЕРЫ:
    python3 analyze-json.py
    python3 analyze-json.py --single-project proj1

=============================================================================
РЕЗУЛЬТИРУЮЩИЕ ФАЙЛЫ (results/{project}/izb/)
=============================================================================

Файлы разбиты по папкам по проходам:
  results/{project}/izb/pass1/  — анализ хешей исходников
  results/{project}/izb/pass2/  — компилируемые языки
  results/{project}/izb/pass3/  — интерпретируемые языки
  results/{project}/izb/pass4/  — происхождение бинарей дистрибутива

  --- Проход 1 (pass1/): анализ хешей исходников ---

  {project}_direct.json
      Исходные файлы, чей хеш напрямую найден в buildography.
      Поля: path, hash

  {project}_parent.json
      Исходные файлы, чей хеш не найден напрямую, но найден хеш
      родительского архива. Поля: path, hash, parent_hash

  {project}_redundant.json
      Исходные файлы, не найденные ни напрямую, ни через архив.
      Включает файлы из not_compiled (перемещаются сюда в Проходе 2).
      Поля: path, hash

  --- Проход 2 (pass2/): компилируемые языки ---

  {project}_not_compiled.json
      Подмножество redundant: файлы компилируемых языков (.c, .cpp, .rs, .go и др.),
      которые компилировались, но результат компиляции не попал в дистрибутив.
      Поля: path, hash, source (direct|parent)

  {project}_redundant.txt
      Объединение redundant.json + not_compiled.json (без дублей).
      Формат: путь<TAB>хеш. Заголовок содержит алгоритм хеширования.

  --- Проход 3 (pass3/): интерпретируемые языки ---

  {project}_interpreted_executed.json
      Python-файлы, которые были входными для команд интерпретаторов.
      Поля: path, hash, commands (список полных команд запуска)

  {project}_interpreted_executed.txt
      Текстовый вариант interpreted_executed.json (без команд).
      Формат: путь<TAB>хеш

  {project}_interpreted_compiled_used.json
      Интерпретируемые файлы сгенерированные интерпретатором или входные
      файлы чьи выходы попали в дистрибутив. Результат компиляции — в bin.
      Поля: path, hash  (TXT не создаётся)

  {project}_interpreted_compiled_unused.json
      Интерпретируемые файлы сгенерированные интерпретатором, но результат
      их компиляции НЕ попал в дистрибутив — избыточные.
      Поля: path, hash

  {project}_interpreted_compiled_unused.txt
      Текстовый вариант interpreted_compiled_unused.json.
      Формат: путь<TAB>хеш

  {project}_interpreted_copied.json / .txt
      Интерпретируемые файлы присутствующие в дистрибутиве напрямую,
      но не вошедшие в executed/compiled_used. Поля: path, hash

  {project}_not_used.json / .txt
      Интерпретируемые файлы которые не используются нигде —
      не запускались, не компилировались, не в дистрибутиве.
      Поля: path, hash

  --- Проход 4 (pass4/): проверка происхождения бинарей дистрибутива ---
  Анализируются только реальные бинари из binaries_in_bin.txt.
  Системные пути в дистрибутиве выделяются отдельно.

  {project}_compiled_from_src.json / .txt
      Бинарь собран в этой сборке, все исходники подтверждены в src.json.
      Чисто. Поля: path, hash

  {project}_binaries_from_src.json / .txt
      Хеш бинаря найден в src.json — бинарь лежал в исходниках и скопирован
      в дистрибутив. Учтён, но факт бинаря в исходниках требует внимания.
      Поля: path, hash

  {project}_untraced_from_src.json / .txt
      Хеш в src.json, но трассировщик не видит попадание в дистрибутив.
      Возможно скопирован напрямую минуя трассировщик. Поля: path, hash

  {project}_external_built.json / .txt
      Бинарь собран (трассировщик видит), но зависимости не из src.json.
      Скомпилирован из внешних исходников. Подозрительно.
      Поля: path, hash, external_deps

  {project}_external_prebuilt.json / .txt
      Трассировщик видит бинарь как зависимость, но он не собирался —
      пришёл готовым извне (apt download, wget, pip). Подозрительно.
      Поля: path, hash

  {project}_untraced_external.json / .txt
      Не в src.json, трассировщик не видит. Полностью неизвестное
      происхождение. Очень подозрительно. Поля: path, hash

  {project}_system_binaries.json / .txt
      Бинарь находится в системном пути дистрибутива (usr/lib, lib и т.д.).
      Анализируется отдельно — системные библиотеки поставляемые дистрибутивом.
      Поля: path, hash

=============================================================================
"""

import array
import bisect
import gc
import sqlite3
import json
import sys
import os
import argparse
import glob
import time
from pathlib import Path
from datetime import datetime

# Глобальный таймер — время старта скрипта
_SCRIPT_START = time.monotonic()


def _ts():
    """Возвращает строку [MM:SS] от начала запуска."""
    elapsed = int(time.monotonic() - _SCRIPT_START)
    return "[{:02d}:{:02d}]".format(elapsed // 60, elapsed % 60)

# =============================================================================
# НАСТРАИВАЕМЫЕ ПУТИ
# =============================================================================
BASE_DIR         = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILDOGRAPHY_DIR = os.path.join(BASE_DIR, "buildography", "builds")
RESULTS_DIR      = os.path.join(BASE_DIR, "results")
UTILITIES_FILE   = os.path.join(BASE_DIR, "lib", "utilities.yaml")
GENERATE_JSON_SCRIPT = os.path.join(BASE_DIR, "scripts", "generate_json_v2_test.sh")
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
    """Загружает множества компиляторов и интерпретаторов."""
    compilers = set()
    interpreters = set()
    if not os.path.exists(utilities_path):
        print(_ts() + " utilities.yaml not found: {}".format(utilities_path))
        return compilers, interpreters
    try:
        import yaml
        with open(utilities_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        utilities = data.get('utilities', {})
        compilers = set(utilities.get('compilers', []))
        interpreters = set(utilities.get('interpreters', []))
        print(_ts() + " Loaded {} compilers, {} interpreters".format(len(compilers), len(interpreters)))
        return compilers, interpreters
    except ImportError:
        # fallback: простой парсер
        try:
            compilers = []
            interpreters = []
            with open(utilities_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            in_compilers = False
            in_interpreters = False
            for line in lines:
                stripped = line.rstrip()
                if stripped.strip() == 'compilers:':
                    in_compilers = True
                    in_interpreters = False
                    continue
                if stripped.strip() == 'interpreters:':
                    in_interpreters = True
                    in_compilers = False
                    continue
                if in_compilers:
                    if stripped and not stripped.startswith(' ') and not stripped.startswith('\t'):
                        in_compilers = False
                    else:
                        val = stripped.strip()
                        if val.startswith('- '):
                            compilers.append(val[2:].strip())
                if in_interpreters:
                    if stripped and not stripped.startswith(' ') and not stripped.startswith('\t'):
                        in_interpreters = False
                    else:
                        val = stripped.strip()
                        if val.startswith('- '):
                            interpreters.append(val[2:].strip())
            print(_ts() + " Loaded {} compilers, {} interpreters (fallback)".format(len(compilers), len(interpreters)))
            return set(compilers), set(interpreters)
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
                not_compiled.append({
                    'path': entry['path'],
                    'hash': entry['hash'],
                    'source': 'direct',
                })
                redundant.append({'path': entry['path'], 'hash': entry['hash']})
                moved_count += 1
                continue
        direct_out.append(entry)

    for entry in parent:
        if is_compiled_extension(entry.get('path', '')):
            if not was_compiled(entry):
                not_compiled.append({
                    'path': entry['path'],
                    'hash': entry['hash'],
                    'parent_hash': entry.get('parent_hash', ''),
                    'source': 'parent',
                })
                redundant.append({'path': entry['path'], 'hash': entry['hash']})
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


def analyze_interpreted(signatures, input_files, output_files, bin_hashes, bin_paths, raw_cmds):
    """
    Классифицирует интерпретируемые файлы из signatures на четыре категории:
      - executed:   только Python-файлы, которые были входными для команд интерпретаторов
                    (добавляется поле "commands" со списком полных команд)
      - compiled:   выходные файлы интерпретаторов + входные любых языков, чьи выходы попали в bin
      - copied:     файлы, присутствующие в bin.json, но не вошедшие в executed/compiled
      - izb:        остальные (избыточные)
    Возвращает кортеж (executed, compiled, copied, izb).
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

    # Для каждой команды запомним, есть ли у неё выходы в bin
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
    izb = []
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

        # --- 4. Проверка на executed (только Python, был входным для интерпретатора) ---
        if is_python_extension(path):
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

        # --- 5. Остальное — избыточное ---
        izb.append({'path': path, 'hash': h})

    # Добавляем выходные файлы интерпретатора которых нет в signatures
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
            else:
                compiled_unused.append({'path': path, 'hash': h})
            added_compiled_paths.add(p_norm)

    return executed, compiled_used, compiled_unused, copied, izb


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
def get_try_dir(base_dir):
    """
    Возвращает путь к папке try{N} внутри base_dir.
    Если try1 существует — возвращает try2, и т.д.
    """
    n = 1
    while True:
        try_dir = os.path.join(base_dir, "try{}".format(n))
        if not os.path.exists(try_dir):
            return try_dir
        n += 1


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

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("# {}\n".format(category_label))
        f.write("# Generated: {}\n".format(datetime.now().isoformat()))
        f.write("# Total: {}\n".format(len(rows)))
        f.write("# Format: path<TAB>hash\n")
        f.write("#\n")
        for path, hash_ in rows:
            f.write("{}\t{}\n".format(path, hash_))
    print(_ts() + "   Written {} entries -> {}".format(len(rows), output_path))


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
        with open('/proc/self/status', 'r') as f:
            status = f.read()
        def _get_kb(field):
            for line in status.splitlines():
                if line.startswith(field + ':'):
                    return int(line.split()[1])
            return 0
        vmrss  = _get_kb('VmRSS')
        vmvirt = _get_kb('VmSize')
        vmswap = _get_kb('VmSwap')
        print(_ts() + "   {}: RSS={:.1f} MB, VIRT={:.1f} MB, SWAP={:.1f} MB".format(
            label, vmrss/1024, vmvirt/1024, vmswap/1024))
    except Exception as e:
        print(_ts() + "   {}: could not read memory: {}".format(label, e))


SYSTEM_PATH_PREFIXES = (
    '/usr/lib/', '/usr/lib64/', '/lib/', '/lib64/',
    '/usr/include/', '/usr/local/lib/',
    '/etc/', '/proc/', '/sys/', '/dev/',
    '/usr/share/', '/var/',
)


def _is_system_path(path):
    return any(path.startswith(pfx) for pfx in SYSTEM_PATH_PREFIXES)


def _count_cmds(buildography_files):
    total = 0
    for path in buildography_files:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            data = json.load(f, strict=False)
        total += len(data.get('component_commands', []))
    return total


def _scan_pass(buildography_files, target_hashes, total_cmds, label):
    """
    Один проход по buildography.
    Для команд чьи выходы пересекаются с target_hashes —
    собираем их зависимости.
    
    Возвращает:
      out_to_deps : dict {out_hash_int -> [(dep_hash_int, dep_path, dep_hash_str)]}
      output_hashes_seen : set всех output хешей встреченных в этом проходе
      dep_hashes_seen    : set всех dep хешей встреченных в этом проходе
    """
    out_to_deps        = {}
    output_hashes_seen = set()
    dep_hashes_seen    = set()

    processed = 0
    for file_path in buildography_files:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            data = json.load(f, strict=False)
        cmds = data.get('component_commands', [])

        for cmd in cmds:
            processed += 1
            progress_log(label, processed, total_cmds)

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

            # Зависимости
            deps_raw = cmd.get('dependencies', {})
            dep_list = []
            if isinstance(deps_raw, dict):
                for path, h in deps_raw.items():
                    h = h.strip() if h else ''
                    hi = _hash_to_int(h)
                    if hi is not None:
                        dep_hashes_seen.add(hi)
                        dep_list.append((hi, path, h))
            elif isinstance(deps_raw, list):
                for dep in deps_raw:
                    if isinstance(dep, dict):
                        path = dep.get('path', '')
                        h = dep.get('hash', '').strip()
                        hi = _hash_to_int(h)
                        if hi is not None:
                            dep_hashes_seen.add(hi)
                            dep_list.append((hi, path, h))

            # Индексируем если выход в target_hashes
            relevant = out_ints & target_hashes
            if relevant and dep_list:
                for out_hi in relevant:
                    existing = out_to_deps.get(out_hi, [])
                    out_to_deps[out_hi] = existing + dep_list

        del data, cmds
        gc.collect()

    return out_to_deps, output_hashes_seen, dep_hashes_seen


def analyze_pass4(bin_entries, src_hashes, buildography_files, script_dir):
    """
    Проверяет происхождение бинарей дистрибутива.
    Итеративно расширяет граф только для нужных цепочек.
    """
    _log_memory("Pass 4 start")

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

    # Максимальная глубина итераций — покрывает сценарии:
    # iter1: бинарь ← прямые зависимости
    # iter2: .o/.a ← их зависимости (компиляция из скачанных исходников)
    # iter3: .so ← их зависимости (редкий случай)
    MAX_ITERATIONS = 3
    # Максимальный размер frontier — защита от "жирных" команд
    # (линковщики с сотнями тысяч зависимостей)
    MAX_FRONTIER = 10000

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

        out_to_deps, output_hashes_seen, dep_hashes_seen = _scan_pass(
            buildography_files, frontier, total_cmds,
            "Pass 4 iter{}".format(iteration)
        )

        # Объединяем с общим графом
        for out_hi, deps in out_to_deps.items():
            existing = full_out_to_deps.get(out_hi, [])
            full_out_to_deps[out_hi] = existing + deps

        all_output_hashes.update(output_hashes_seen)
        all_dep_hashes.update(dep_hashes_seen)

        _log_memory("Pass 4 iter {} after scan".format(iteration))
        print(_ts() + "   Pass 4 iter {}: found {} commands, "
              "output_hashes={}, dep_hashes={}".format(
              iteration, len(out_to_deps),
              len(all_output_hashes), len(all_dep_hashes)))

        # Новый frontier — промежуточные артефакты
        new_frontier = set()
        for deps in out_to_deps.values():
            for (dep_hi, dep_path, _) in deps:
                if (dep_hi in all_output_hashes and
                        dep_hi not in full_out_to_deps and
                        not _is_system_path(dep_path)):
                    new_frontier.add(dep_hi)

        # Ограничиваем размер frontier
        if len(new_frontier) > MAX_FRONTIER:
            print(_ts() + "   Pass 4 iter {}: frontier truncated {} → {} (MAX_FRONTIER)".format(
                iteration, len(new_frontier), MAX_FRONTIER))
            new_frontier = set(list(new_frontier)[:MAX_FRONTIER])

        frontier = new_frontier
        print(_ts() + "   Pass 4 iter {}: new frontier size: {}".format(
            iteration, len(frontier)))

        del out_to_deps, output_hashes_seen, dep_hashes_seen
        gc.collect()

    print(_ts() + "   Pass 4: graph expansion done after {} iterations".format(iteration))
    print(_ts() + "   Pass 4: full_out_to_deps={}, all_output_hashes={}, "
          "all_dep_hashes={}".format(
          len(full_out_to_deps), len(all_output_hashes), len(all_dep_hashes)))
    _log_memory("after graph expansion")

    # ==========================================================================
    # Классификация бинарей
    # ==========================================================================
    system_binaries  = []  # путь в дистрибутиве — системный (usr/lib, lib и т.д.)
    compiled_from_src = [] # сценарий 1: собран из src.json, трассировщик подтверждает
    binaries_from_src = [] # сценарий 2: хеш в src.json, скопирован напрямую
    untraced_from_src = [] # сценарий 6: хеш в src.json, трассировщик не видит
    external_built   = []  # сценарий 5: собран из внешних исходников
    external_prebuilt = [] # сценарий 3: готовый бинарь извне, трассировщик видит
    untraced_external = [] # сценарий 4: не в src.json, трассировщик не видит

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

    def _is_distrib_system_path(path):
        """Проверяем путь бинаря внутри дистрибутива."""
        # Убираем префикс типа KTDL.00554-01/bin/ или bin/
        p = path
        # Убираем PROJECT/bin/ или bin/
        for prefix in ('bin/', ):
            idx = p.find(prefix)
            if idx >= 0:
                p = p[idx + len(prefix):]
                break
        # Убираем архивные суффиксы типа foo.iso/bar.deb/
        # и смотрим на финальный путь
        parts = p.split('/')
        # Ищем первую часть которая не выглядит как архив
        clean_parts = []
        for part in parts:
            if any(part.endswith(ext) for ext in
                   ('.iso', '.deb', '.rpm', '.tar', '.tgz', '.zip', '.gz')):
                clean_parts = []  # сбрасываем — начинаем после архива
            else:
                clean_parts.append(part)
        clean_path = '/'.join(clean_parts)
        return any(clean_path.startswith(pfx) for pfx in DISTRIB_SYSTEM_PREFIXES)

    total_bin = len(bin_entries)
    print(_ts() + "   Pass 4: classifying {} binaries...".format(total_bin))

    def _get_ext_deps(hi, visited=None):
        """Рекурсивно находит внешние листья в цепочке зависимостей hi."""
        if visited is None:
            visited = set()
        if hi in visited:
            return []
        visited.add(hi)
        ext = []
        for (dep_hi, dep_path, dep_h_str) in full_out_to_deps.get(hi, []):
            if _is_system_path(dep_path):
                continue
            if dep_hi in src_hashes_int:
                continue
            if dep_hi in all_output_hashes:
                ext.extend(_get_ext_deps(dep_hi, visited))
            else:
                ext.append({'hash': dep_h_str, 'path': dep_path})
        return ext

    for i, entry in enumerate(bin_entries):
        progress_log("Pass 4 classifying", i + 1, total_bin)
        path  = entry.get('path', '')
        h_str = entry.get('hash', '').strip()
        hi    = _hash_to_int(h_str)

        # Первый фильтр — системный путь в дистрибутиве
        if _is_distrib_system_path(path):
            system_binaries.append({'path': path, 'hash': h_str})
            continue

        if hi is None:
            untraced_external.append({'path': path, 'hash': h_str})
            continue

        if hi not in all_output_hashes and hi not in all_dep_hashes:
            # Нет в трассировщике — проверяем есть ли в src.json
            if h_str in src_hashes:
                untraced_from_src.append({'path': path, 'hash': h_str})
            else:
                untraced_external.append({'path': path, 'hash': h_str})
            continue

        if hi not in all_output_hashes and hi in all_dep_hashes:
            # Готовый бинарь — трассировщик видит его как зависимость
            # Проверяем есть ли в src.json
            if h_str in src_hashes:
                binaries_from_src.append({'path': path, 'hash': h_str})
            else:
                external_prebuilt.append({'path': path, 'hash': h_str})
            continue

        # Собран — проверяем цепочку зависимостей
        ext_deps = _get_ext_deps(hi)

        if ext_deps:
            external_built.append({
                'path': path,
                'hash': h_str,
                'external_deps': ext_deps
            })
        else:
            # Все зависимости из src.json — проверяем есть ли сам в src.json
            if h_str in src_hashes:
                binaries_from_src.append({'path': path, 'hash': h_str})
            else:
                compiled_from_src.append({'path': path, 'hash': h_str})

    del full_out_to_deps, all_output_hashes, all_dep_hashes
    del src_hashes_int, bin_hashes_int
    gc.collect()
    _log_memory("Pass 4 done")

    print(_ts() + "   Pass 4 done: "
          "compiled_from_src={}, binaries_from_src={}, untraced_from_src={}, "
          "external_built={}, external_prebuilt={}, untraced_external={}, "
          "system_binaries={}".format(
          len(compiled_from_src), len(binaries_from_src), len(untraced_from_src),
          len(external_built), len(external_prebuilt), len(untraced_external),
          len(system_binaries)))

    return (compiled_from_src, binaries_from_src, untraced_from_src,
            external_built, external_prebuilt, untraced_external, system_binaries)



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
def process_project(project_name, compiler_basenames, interpreter_basenames, by_disk=False):
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
        print(_ts() + "     {}".format(os.path.basename(f)))

    sources_dir = os.path.join(RESULTS_DIR, project_name, "sources")
    signatures_pattern = os.path.join(sources_dir, "*_src.json")
    signatures_files = sorted(glob.glob(signatures_pattern))
    if not signatures_files:
        print(_ts() + "   No *_src.json found: {}".format(signatures_pattern))
        return False

    print(_ts() + "   Source signature files found: {}".format(len(signatures_files)))
    for f in signatures_files:
        print(_ts() + "     {}".format(os.path.basename(f)))

    output_dir = os.path.join(RESULTS_DIR, project_name, "izb")
    os.makedirs(output_dir, exist_ok=True)

    try:
        signatures = load_signatures(signatures_files)
        buildography_hashes, raw_cmds = load_buildography_data(buildography_files)
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
    pass4_ran   = False  # флаг успешного выполнения Pass 4
    total_bin_count = 0  # будем хранить количество бинарных файлов для статистики
    bin_json_path = os.path.join(RESULTS_DIR, project_name, "sources",
                                 "{}_bin.json".format(project_name))
    binaries_in_bin_path = os.path.join(RESULTS_DIR, project_name, "ext",
                                        "binaries_in_bin.txt")

    if os.path.isfile(bin_json_path) and os.path.isfile(binaries_in_bin_path):
        try:
            # Читаем bin.json — строим индекс path -> hash
            with open(bin_json_path, 'r', encoding='utf-8') as f:
                bin_data = json.load(f)
            if isinstance(bin_data, list):
                raw_files = bin_data
            elif 'signatures' in bin_data:
                raw_files = bin_data['signatures']
            else:
                raw_files = bin_data.get('files', [])

            # Строим индекс по нормализованному пути
            path_to_hash = {}
            for item in raw_files:
                p = item.get('path', '').strip()
                h = item.get('hash', '').strip()
                if p and h:
                    # Нормализуем путь — убираем ведущий слеш если есть
                    p_norm = p.lstrip('/')
                    path_to_hash[p_norm] = h
                    path_to_hash[p] = h  # также оригинальный путь

            print(_ts() + "   bin.json loaded: {} entries".format(len(path_to_hash)))

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
                        bin_entries.append({'path': fpath_norm, 'hash': h})
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

    # buildography_hashes больше не нужен
    del buildography_hashes
    gc.collect()
    print(_ts() + "   Pass 1 done. Memory freed: buildography_hashes")

    # --- Проход 2 (компиляторы) ---
    if compiler_basenames:
        print(_ts() + "   Starting pass 2 (transitive closure from bin using compilers)...")
        good_compiler_input_keys = build_good_compiler_inputs(
            raw_cmds, compiler_basenames, bin_hashes, bin_paths
        )
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
        executed, compiled_used, compiled_unused, copied, izb = analyze_interpreted(
            signatures, input_files, output_files, bin_hashes, bin_paths, raw_cmds
        )
        del input_files, output_files
        gc.collect()
        print(_ts() + "   Pass 3 done. Memory freed: input_files, output_files")
    else:
        print(_ts() + "   Pass 3 skipped (no interpreter list)")
        executed = compiled_used = compiled_unused = copied = izb = []

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
        p4_system_binaries = analyze_pass4(
            bin_entries, src_hashes, buildography_files, script_dir
        )
        del bin_entries, src_hashes
        gc.collect()
        pass4_ran = True
        print(_ts() + "   Pass 4 done. Memory freed: bin_entries, src_hashes")
    else:
        print(_ts() + "   Pass 4 skipped (no bin entries)")
        p4_compiled_from_src = p4_binaries_from_src = p4_untraced_from_src = \
        p4_external_built = p4_external_prebuilt = p4_untraced_external = \
        p4_system_binaries = []
        pass4_ran = False
        del src_hashes
        gc.collect()
        pass4_ran = False

    # Создаём папку try{N} и подпапки для каждого прохода
    izb_base = os.path.join(RESULTS_DIR, project_name, "izb")
    try_dir  = get_try_dir(izb_base)
    os.makedirs(try_dir, exist_ok=True)
    print(_ts() + "   Results directory: {}".format(try_dir))

    pass1_dir = os.path.join(try_dir, "pass1")
    pass2_dir = os.path.join(try_dir, "pass2")
    pass3_dir = os.path.join(try_dir, "pass3")
    pass4_dir = os.path.join(try_dir, "pass4")
    for d in [pass1_dir, pass2_dir, pass3_dir, pass4_dir]:
        os.makedirs(d, exist_ok=True)

    def jt(folder, name, category, entries):
        """Записывает JSON и TXT файлы для категории."""
        base = os.path.join(folder, "{}_{}".format(project_name, name))
        write_json_result(base + ".json", category, entries)
        write_txt_result(base + ".txt", category, entries)

    # --- Pass 1 ---
    print(_ts() + "   Writing pass 1 results...")
    jt(pass1_dir, "direct",           "direct",    direct)
    jt(pass1_dir, "parent",           "parent",    parent)
    jt(pass1_dir, "redundant-by-hash","redundant", redundant)

    # --- Pass 2 ---
    print(_ts() + "   Writing pass 2 results...")
    jt(pass2_dir, "not_compiled", "not_compiled", not_compiled)

    # --- Pass 3 ---
    print(_ts() + "   Writing pass 3 results...")
    jt(pass3_dir, "executed",        "interpreted_executed",        executed)
    jt(pass3_dir, "compiled_used",   "interpreted_compiled_used",   compiled_used)
    jt(pass3_dir, "compiled_unused", "interpreted_compiled_unused", compiled_unused)
    jt(pass3_dir, "copied",          "interpreted_copied",          copied)
    jt(pass3_dir, "not_used",        "interpreted_not_used",        izb)

    # --- Pass 4 ---
    if pass4_ran:
        print(_ts() + "   Writing pass 4 results...")
        jt(pass4_dir, "compiled_from_src",  "compiled_from_src",  p4_compiled_from_src)
        jt(pass4_dir, "binaries_from_src",  "binaries_from_src",  p4_binaries_from_src)
        jt(pass4_dir, "untraced_from_src",  "untraced_from_src",  p4_untraced_from_src)
        jt(pass4_dir, "external_built",     "external_built",     p4_external_built)
        jt(pass4_dir, "external_prebuilt",  "external_prebuilt",  p4_external_prebuilt)
        jt(pass4_dir, "untraced_external",  "untraced_external",  p4_untraced_external)
        jt(pass4_dir, "system_binaries",    "system_binaries",    p4_system_binaries)

    # --- Статистика ---
    def pct(n, total):
        return (n / total * 100) if total else 0

    # Компилируемые исходники
    total_source   = len(direct) + len(parent) + len(redundant)
    # not_compiled — подмножество redundant (файлы перемещённые из direct/parent)
    # redundant уже включает not_compiled, поэтому избыточные = redundant
    n_redundant    = len(redundant)

    # Интерпретируемые файлы
    total_interp   = len(executed) + len(compiled_used) + len(compiled_unused) + len(copied) + len(izb)

    # Итого
    total_all      = total_source + total_interp
    total_izb      = n_redundant + len(compiled_unused) + len(izb)

    sep = "  " + "-" * 48

    print("\n  --- Results for {} ---".format(project_name))

    print("\n  Компилируемые исходники ({} файлов)".format(total_source))
    print(sep)
    print("  Direct (используются напрямую)                     : {:>7}  ({:.1f}%)".format(len(direct),       pct(len(direct),       total_source)))
    print("  Parent (через архив)                               : {:>7}  ({:.1f}%)".format(len(parent),       pct(len(parent),       total_source)))
    print("  Not compiled (компилировались, результат не в bin) : {:>7}  ({:.1f}%)".format(n_redundant,       pct(n_redundant,       total_source)))

    print("\n  Интерпретируемые файлы ({} файлов)".format(total_interp))
    print(sep)
    print("  Executed (запускаются)                             : {:>7}  ({:.1f}%)".format(len(executed),        pct(len(executed),        total_interp)))
    print("  Compiled used (скомпилированы, результат в bin)    : {:>7}  ({:.1f}%)".format(len(compiled_used),   pct(len(compiled_used),   total_interp)))
    print("  Compiled unused (скомпилированы, результат не в bin): {:>7}  ({:.1f}%)".format(len(compiled_unused), pct(len(compiled_unused), total_interp)))
    print("  Copied (есть в дистрибутиве)                       : {:>7}  ({:.1f}%)".format(len(copied),          pct(len(copied),          total_interp)))
    print("  Not used (не используются нигде)                   : {:>7}  ({:.1f}%)".format(len(izb),             pct(len(izb),             total_interp)))

    print("\n  Итого ({} файлов)".format(total_all))
    print(sep)
    print("  Используются                                       : {:>7}  ({:.1f}%)".format(total_all - total_izb, pct(total_all - total_izb, total_all)))
    print("  Избыточные (not_compiled + compiled_unused + not_used): {:>7}  ({:.1f}%)".format(total_izb,             pct(total_izb,             total_all)))

    if pass4_ran:
        total_bin = (len(p4_compiled_from_src) + len(p4_binaries_from_src) +
                     len(p4_untraced_from_src) + len(p4_external_built) +
                     len(p4_external_prebuilt) + len(p4_untraced_external) +
                     len(p4_system_binaries))
        print("\n  Происхождение бинарей дистрибутива ({} файлов)".format(total_bin))
        print(sep)
        print("  Compiled from src  (собран из src.json)            : {:>7}  ({:.1f}%)".format(
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
        ("not_compiled",      "not_compiled",       not_compiled),
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
        description='Анализ избыточных файлов исходных текстов'
    )
    parser.add_argument(
        '--single-project',
        metavar='NAME',
        help='Обработать только один указанный проект'
    )
    parser.add_argument(
        '--by-disk',
        action='store_true',
        default=False,
        help='Pass 5: разбить избыточные файлы по дискам (pass5/src/ и pass5/bin/)'
    )
    args = parser.parse_args()

    if not os.path.isdir(BUILDOGRAPHY_DIR):
        print(_ts() + " Buildography directory not found: {}".format(BUILDOGRAPHY_DIR))
        sys.exit(1)

    if not os.path.isdir(RESULTS_DIR):
        print(_ts() + " Results directory not found: {}".format(RESULTS_DIR))
        sys.exit(1)

    compiler_basenames, interpreter_basenames = load_utilities_lists(UTILITIES_FILE)

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
    print(_ts() + " Compilers: {}, Interpreters: {}".format(len(compiler_basenames), len(interpreter_basenames)))

    start_time = datetime.now()
    results = {}

    for project_name in projects:
        results[project_name] = process_project(
            project_name, compiler_basenames, interpreter_basenames,
            by_disk=args.by_disk
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