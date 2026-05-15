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

  --- Проход 1: анализ хешей исходников ---

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

  --- Проход 2: компилируемые языки ---

  {project}_not_compiled.json
      Подмножество redundant: файлы компилируемых языков (.c, .cpp, .rs, .go и др.),
      которые компилировались, но результат компиляции не попал в дистрибутив.
      Поля: path, hash, source (direct|parent)

  {project}_redundant.txt
      Объединение redundant.json + not_compiled.json (без дублей).
      Формат: путь<TAB>хеш. Заголовок содержит алгоритм хеширования.

  --- Проход 3: интерпретируемые языки ---

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

  {project}_interpreted_copied.json
      Интерпретируемые файлы, присутствующие в bin.json напрямую,
      но не вошедшие в executed/compiled_used. Поля: path, hash
      (TXT не создаётся)

  {project}_interpreted_izb.json
      Остальные избыточные интерпретируемые файлы —
      не запускались, не компилировались, не в дистрибутиве. Поля: path, hash

  {project}_interpreted_izb.txt
      Текстовый вариант interpreted_izb.json.
      Формат: путь<TAB>хеш

  --- Проход 4: проверка происхождения файлов дистрибутива ---

  {project}_pass4_untraced.json
      Файлы дистрибутива (bin.json), чей хеш вообще не встречается
      в трассировщике — ни в output, ни в dependencies.
      Происхождение неизвестно. Поля: path, hash

  {project}_pass4_untraced.txt
      Текстовый вариант pass4_untraced.json.
      Формат: путь<TAB>хеш

  {project}_pass4_external_unbuilt.json
      Файлы дистрибутива, которые присутствуют в dependencies
      трассировщика, но НЕ в output ни одной команды — то есть
      не были собраны в этой сборке, пришли готовыми извне.
      Поля: path, hash

  {project}_pass4_external_unbuilt.txt
      Текстовый вариант pass4_external_unbuilt.json.
      Формат: путь<TAB>хеш

  {project}_pass4_external_sources.json
      Файлы дистрибутива, которые есть в output трассировщика
      (т.е. были собраны), но в цепочке их зависимостей (транзитивно)
      обнаружены файлы, отсутствующие в src.json — чужие исходники.
      Поля: path, hash, external_deps (список {path, hash} чужих исходников)

  {project}_pass4_external_sources.txt
      Текстовый вариант pass4_external_sources.json.
      Формат: путь<TAB>хеш (только сам файл дистрибутива, без external_deps)

  {project}_pass4_traced.json
      Файлы дистрибутива с полностью подтверждённым происхождением:
      собраны в этой сборке и все исходники в цепочке из src.json.
      Поля: path, hash

  {project}_pass4_traced.txt
      Текстовый вариант pass4_traced.json.
      Формат: путь<TAB>хеш

=============================================================================
"""

import json
import sys
import os
import argparse
import glob
from pathlib import Path
from datetime import datetime

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
        print("  [INFO] redundant.txt: generate script not found: {}".format(
            os.path.basename(script_path)))
        print("  [INFO] redundant.txt: hash_algorithm field will be empty")
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
                        print("  [INFO] redundant.txt: HASH_CMD={} (from {})".format(
                            value, os.path.basename(script_path)))
                        return value
        print("  [INFO] redundant.txt: HASH_CMD not found in {}".format(
            os.path.basename(script_path)))
        return ''
    except Exception as e:
        print("  [WARN] redundant.txt: failed to read HASH_CMD: {}".format(e))
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
        print("[WARN] utilities.yaml not found: {}".format(utilities_path))
        return compilers, interpreters
    try:
        import yaml
        with open(utilities_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        utilities = data.get('utilities', {})
        compilers = set(utilities.get('compilers', []))
        interpreters = set(utilities.get('interpreters', []))
        print("[INFO] Loaded {} compilers, {} interpreters".format(len(compilers), len(interpreters)))
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
            print("[INFO] Loaded {} compilers, {} interpreters (fallback)".format(len(compilers), len(interpreters)))
            return set(compilers), set(interpreters)
        except Exception as e:
            print("[WARN] Failed to parse utilities.yaml: {}".format(e))
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
        print("  [INFO]   {} {}/{} ({}%)".format(label, current, total, pct))


# =============================================================================
# ЗАГРУЗКА ДАННЫХ (с нормализацией путей)
# =============================================================================
def load_signatures(paths):
    all_signatures = []
    for path in paths:
        print("  [INFO] Loading signatures: {}".format(os.path.basename(path)))
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            data = json.load(f)
        sigs = data.get('signatures', [])
        for s in sigs:
            # Добавляем нормализованный путь
            s['path_norm'] = os.path.normpath(s.get('path', ''))
        print("  [INFO] Signatures in {}: {}".format(os.path.basename(path), len(sigs)))
        all_signatures.extend(sigs)
    print("  [INFO] Total signatures (merged): {}".format(len(all_signatures)))
    return all_signatures


def load_bin_signatures(project_name):
    bin_path = os.path.join(RESULTS_DIR, project_name, "sources", "{}_bin.json".format(project_name))
    if not os.path.exists(bin_path):
        print("  [WARN] bin.json not found: {}".format(bin_path))
        return set(), set()
    print("  [INFO] Loading bin signatures: {}".format(os.path.basename(bin_path)))
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
        print("  [INFO] Loading buildography: {}".format(os.path.basename(path)))
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
        print("  [INFO] Hashes from {}: {} (total pool: {})".format(
            os.path.basename(path), added, len(hashes)))
    print("  [INFO] Total buildography hashes (merged): {}".format(len(hashes)))
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
    print("  [INFO] Pass 2: indexing {} commands...".format(total_cmds))

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

    print("  [INFO] Pass 2: seeding BFS from bin outputs...")
    for idx in range(total_cmds):
        if (any(p in bin_paths_set  for p in cmd_output_paths[idx]) or
            any(h in bin_hashes_set for h in cmd_output_hashes[idx])):
            good_cmds.add(idx)
            queue.append(idx)

    print("  [INFO] Pass 2: BFS start — seed size: {}".format(len(good_cmds)))

    processed = 0
    while queue:
        idx = queue.popleft()
        processed += 1
        if processed % 10000 == 0:
            print("  [INFO] Pass 2: BFS processed {}, good so far: {}, queue: {}".format(
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

    print("  [INFO] Pass 2: BFS done — total good commands: {}".format(len(good_cmds)))
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

    print("  [INFO] Pass 2 done: moved to not_compiled={}".format(moved_count))
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
    print("  [INFO] Pass 3: scanning {} commands for interpreter calls...".format(total_cmds))
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
    print("  [INFO] Pass 3: classifying {} interpreted files...".format(total_interp))

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
        p_norm = entry.get('path_norm', '')

        # --- Проверка на executed (только Python) ---
        if is_python_extension(path):
            was_executed = False
            if h and h in input_hashes:
                was_executed = True
            elif p_norm and p_norm in input_paths_norm:
                was_executed = True

            if was_executed:
                # Собираем команды
                cmd_indices = input_by_hash.get(h, set()) if h else input_by_path.get(p_norm, set())
                commands = []
                for idx in cmd_indices:
                    cmd_str = cmd_idx_to_command.get(idx)
                    if cmd_str and cmd_str not in commands:
                        commands.append(cmd_str)
                executed.append({
                    'path': path,
                    'hash': h,
                    'commands': commands
                })
                continue

        # --- Проверка на выходной файл интерпретатора ---
        is_output = False
        if h and h in output_hashes:
            is_output = True
        elif p_norm and p_norm in output_paths_norm:
            is_output = True

        if is_output:
            # Проверяем попал ли сам этот файл в дистрибутив
            in_bin = (h and h in bin_hashes_set) or (p_norm and p_norm in bin_paths_set)
            if in_bin:
                compiled_used.append({'path': path, 'hash': h})
            else:
                compiled_unused.append({'path': path, 'hash': h})
            added_compiled_paths.add(p_norm)
            continue

        # --- Проверка на leads_to_bin (входной файл, чей выход попал в bin) ---
        leads_to_bin = False
        was_input = False
        if h and h in input_hashes:
            was_input = True
            cmd_indices = input_by_hash.get(h, set())
        elif p_norm and p_norm in input_paths_norm:
            was_input = True
            cmd_indices = input_by_path.get(p_norm, set())
        else:
            cmd_indices = set()

        if was_input:
            for cmd_idx in cmd_indices:
                if cmd_has_bin_output[cmd_idx]:
                    leads_to_bin = True
                    break

        if leads_to_bin:
            compiled_used.append({'path': path, 'hash': h})
            added_compiled_paths.add(p_norm)
            continue

        # --- Проверка наличия в дистрибутиве (copied) ---
        in_bin = False
        if h and h in bin_hashes_set:
            in_bin = True
        elif p_norm and p_norm in bin_paths_set:
            in_bin = True

        if in_bin:
            copied.append({'path': path, 'hash': h})
        else:
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
            print("  [INFO] Pass 1: analyzed {} source files...".format(processed))
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
    print("  [INFO] Pass 1 done: direct={}, parent={}, redundant={}".format(
        len(direct), len(parent), len(redundant)))
    return direct, parent, redundant


# =============================================================================
# ЗАПИСЬ РЕЗУЛЬТАТОВ (JSON и текстовые)
# =============================================================================
def write_json_result(output_path, category, files):
    versioned_path = get_versioned_filepath(output_path)
    if versioned_path != output_path:
        print("  [WARN] File exists, writing to: {}".format(os.path.basename(versioned_path)))
    result = {
        'category': category,
        'total': len(files),
        'generated_at': datetime.now().isoformat(),
        'files': files,
    }
    with open(versioned_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    print("  [INFO] Written {} entries -> {}".format(len(files), versioned_path))


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
        print("  [WARN] File exists, writing to: {}".format(os.path.basename(versioned_path)))

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
    print("  [INFO] Written {} entries -> {}".format(len(rows), versioned_path))
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
        print("  [WARN] File exists, writing to: {}".format(os.path.basename(versioned_path)))

    with open(versioned_path, 'w', encoding='utf-8') as f:
        f.write("# Interpreted redundant files (izb)\n")
        f.write("# Generated: {}\n".format(datetime.now().isoformat()))
        f.write("# Total: {}\n".format(len(rows)))
        f.write("# Format: path<TAB>hash\n")
        f.write("#\n")
        for path, hash_ in rows:
            f.write("{}\t{}\n".format(path, hash_))
    print("  [INFO] Written {} entries -> {}".format(len(rows), versioned_path))


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
        print("  [WARN] File exists, writing to: {}".format(os.path.basename(versioned_path)))

    with open(versioned_path, 'w', encoding='utf-8') as f:
        f.write("# Interpreted executed Python files\n")
        f.write("# Generated: {}\n".format(datetime.now().isoformat()))
        f.write("# Total: {}\n".format(len(rows)))
        f.write("# Format: path<TAB>hash\n")
        f.write("#\n")
        for path, hash_ in rows:
            f.write("{}\t{}\n".format(path, hash_))
    print("  [INFO] Written {} entries -> {}".format(len(rows), versioned_path))


# =============================================================================
# АНАЛИЗ — ПРОХОД 4: проверка происхождения файлов дистрибутива
# =============================================================================

def build_pass4_indexes(raw_cmds):
    """
    Строит индексы по хешу из raw_cmds:
      - output_hashes      : хеши всех выходных файлов (файл был собран в этой сборке)
      - dep_hashes         : хеши всех входных файлов (файл участвовал как зависимость)
      - output_to_deps     : hash_output -> set(hash_dep) — прямые зависимости выхода
      - dep_to_outputs     : hash_dep -> set(hash_output) — в каких выходах участвует dep
    Хеши без значения (пустые строки) игнорируются.
    """
    output_hashes = set()
    dep_hashes = set()
    # hash выхода -> множество хешей его прямых зависимостей
    output_to_deps = {}
    # hash зависимости -> множество хешей выходов где она участвует
    dep_to_outputs = {}

    total_cmds = len(raw_cmds)
    print("  [INFO] Pass 4: indexing {} commands...".format(total_cmds))
    for i, cmd in enumerate(raw_cmds):
        progress_log("Pass 4 indexing commands", i + 1, total_cmds)
        # Собираем хеши выходов этой команды
        cmd_out_hashes = set()
        outputs = cmd.get('output', {})
        items = outputs.items() if isinstance(outputs, dict) else (
            ((o.get('path', ''), o.get('hash', '')) if isinstance(o, dict) else (str(o), ''))
            for o in (outputs if isinstance(outputs, list) else [])
        )
        for path, h in (outputs.items() if isinstance(outputs, dict) else []):
            h = h.strip() if h else ''
            if h:
                output_hashes.add(h)
                cmd_out_hashes.add(h)

        if isinstance(outputs, list):
            for out in outputs:
                if isinstance(out, dict):
                    h = out.get('hash', '').strip()
                else:
                    h = ''
                if h:
                    output_hashes.add(h)
                    cmd_out_hashes.add(h)

        # Собираем хеши зависимостей этой команды
        cmd_dep_hashes = set()
        deps = cmd.get('dependencies', {})
        if isinstance(deps, dict):
            for path, h in deps.items():
                h = h.strip() if h else ''
                if h:
                    dep_hashes.add(h)
                    cmd_dep_hashes.add(h)
        elif isinstance(deps, list):
            for dep in deps:
                if isinstance(dep, dict):
                    h = dep.get('hash', '').strip()
                else:
                    h = ''
                if h:
                    dep_hashes.add(h)
                    cmd_dep_hashes.add(h)

        # Связываем выходы с зависимостями
        for out_h in cmd_out_hashes:
            output_to_deps.setdefault(out_h, set()).update(cmd_dep_hashes)
        for dep_h in cmd_dep_hashes:
            dep_to_outputs.setdefault(dep_h, set()).update(cmd_out_hashes)

    return output_hashes, dep_hashes, output_to_deps, dep_to_outputs


def build_leaf_cache(output_to_deps, output_hashes):
    """
    Вычисляет для каждого хеша из output_hashes множество его листовых зависимостей
    (транзитивно). Лист — хеш которого нет в output_hashes (исходник или внешний файл).

    Использует итеративный BFS вместо рекурсии — каждый хеш обходится ровно один раз.
    Возвращает словарь: hash -> frozenset(leaf_hashes).
    """
    from collections import deque

    # Топологический обход: сначала находим все хеши которые нужно обойти
    all_hashes = set(output_to_deps.keys())
    leaf_cache = {}   # hash -> set(leaf_hashes)

    total = len(all_hashes)
    print("  [INFO] Pass 4: building leaf cache for {} output hashes...".format(total))
    done = 0

    for start_hash in all_hashes:
        if start_hash in leaf_cache:
            done += 1
            continue

        # Итеративный DFS с постобработкой (bottom-up)
        stack = [start_hash]
        order = []          # порядок обработки (постфиксный)
        visited_local = set()

        while stack:
            h = stack.pop()
            if h in visited_local:
                continue
            visited_local.add(h)
            order.append(h)
            if h in output_hashes:
                for dep_h in output_to_deps.get(h, ()):
                    if dep_h not in visited_local and dep_h not in leaf_cache:
                        stack.append(dep_h)

        # Вычисляем leaf_cache в обратном порядке (листья раньше родителей)
        for h in reversed(order):
            if h not in output_hashes:
                # Лист
                leaf_cache[h] = frozenset({h})
            else:
                deps = output_to_deps.get(h, set())
                leaves = set()
                for dep_h in deps:
                    if dep_h not in output_hashes:
                        leaves.add(dep_h)
                    else:
                        leaves.update(leaf_cache.get(dep_h, frozenset()))
                leaf_cache[h] = frozenset(leaves)

        done += len(visited_local)
        progress_log("Pass 4 leaf cache", min(done, total), total)

    print("  [INFO] Pass 4: leaf cache built, {} entries".format(len(leaf_cache)))
    return leaf_cache


def analyze_pass4(bin_entries, src_hashes, raw_cmds):
    """
    Классифицирует файлы дистрибутива (bin.json) по происхождению:
      - untraced          : хеш не найден ни в output, ни в dependencies трассировщика
      - external_unbuilt  : хеш есть в dependencies, но не в output (пришёл готовым извне)
      - external_sources  : собран (есть в output), но в цепочке deps есть хеши не из src.json
      - traced            : собран и вся цепочка восходит к src.json

    bin_entries : список {'path': ..., 'hash': ...} из bin.json
    src_hashes  : множество хешей из src.json
    raw_cmds    : команды трассировщика

    Возвращает (untraced, external_unbuilt, external_sources, traced).
    """
    print("  [INFO] Pass 4: building tracer indexes...")
    output_hashes, dep_hashes, output_to_deps, dep_to_outputs = build_pass4_indexes(raw_cmds)
    print("  [INFO] Pass 4: output_hashes={}, dep_hashes={}".format(
        len(output_hashes), len(dep_hashes)))

    # Строим кеш листовых зависимостей один раз для всех хешей
    leaf_cache = build_leaf_cache(output_to_deps, output_hashes)

    untraced = []
    external_unbuilt = []
    external_sources = []
    traced = []

    total_bin = len(bin_entries)
    print("  [INFO] Pass 4: checking origin of {} distrib files...".format(total_bin))
    for i, entry in enumerate(bin_entries):
        progress_log("Pass 4 checking files", i + 1, total_bin)
        path = entry.get('path', '')
        h = entry.get('hash', '').strip()
        if not h:
            untraced.append({'path': path, 'hash': h})
            continue

        # Категория 1: вообще нет в трассировщике
        if h not in output_hashes and h not in dep_hashes:
            untraced.append({'path': path, 'hash': h})
            continue

        # Категория 2: есть в зависимостях, но не был собран
        if h not in output_hashes and h in dep_hashes:
            external_unbuilt.append({'path': path, 'hash': h})
            continue

        # Файл есть в output — был собран. Берём листья из кеша.
        leaf_hashes = leaf_cache.get(h, frozenset())

        # Листья которых нет в src.json — чужие исходники
        external_leaf_hashes = leaf_hashes - src_hashes

        if external_leaf_hashes:
            ext_deps = [{'hash': eh} for eh in external_leaf_hashes]
            external_sources.append({
                'path': path,
                'hash': h,
                'external_deps': ext_deps
            })
        else:
            traced.append({'path': path, 'hash': h})

    print("  [INFO] Pass 4 done: untraced={}, external_unbuilt={}, external_sources={}, traced={}".format(
        len(untraced), len(external_unbuilt), len(external_sources), len(traced)))

    return untraced, external_unbuilt, external_sources, traced


def enrich_external_sources_with_paths(external_sources, raw_cmds):
    """
    Дополняет external_deps в external_sources записями path
    используя обратный индекс hash->path из raw_cmds.
    Вызывать после analyze_pass4.
    """
    # Строим индекс hash -> path по всем зависимостям трассировщика
    hash_to_path = {}
    for cmd in raw_cmds:
        deps = cmd.get('dependencies', {})
        if isinstance(deps, dict):
            for path, h in deps.items():
                h = h.strip() if h else ''
                if h and h not in hash_to_path:
                    hash_to_path[h] = path.strip()
        elif isinstance(deps, list):
            for dep in deps:
                if isinstance(dep, dict):
                    h = dep.get('hash', '').strip()
                    p = dep.get('path', '').strip()
                    if h and h not in hash_to_path:
                        hash_to_path[h] = p

    for entry in external_sources:
        for dep in entry.get('external_deps', []):
            h = dep.get('hash', '')
            if h and 'path' not in dep:
                dep['path'] = hash_to_path.get(h, '')

    return external_sources


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
        print("  [WARN] File exists, writing to: {}".format(os.path.basename(versioned_path)))

    with open(versioned_path, 'w', encoding='utf-8') as f:
        f.write("# Pass 4: {}\n".format(category_label))
        f.write("# Generated: {}\n".format(datetime.now().isoformat()))
        f.write("# Total: {}\n".format(len(rows)))
        f.write("# Format: path<TAB>hash\n")
        f.write("#\n")
        for path, h in rows:
            f.write("{}\t{}\n".format(path, h))
    print("  [INFO] Written {} entries -> {}".format(len(rows), versioned_path))


# =============================================================================
# ОБРАБОТКА ПРОЕКТА
# =============================================================================
def process_project(project_name, compiler_basenames, interpreter_basenames):
    print("\n" + "=" * 50)
    print("Processing project: {}".format(project_name))
    print("=" * 50)

    buildography_pattern = os.path.join(BUILDOGRAPHY_DIR, project_name, "*.json")
    buildography_files = sorted(glob.glob(buildography_pattern))
    if not buildography_files:
        print("  [ERROR] No buildography JSON found: {}".format(buildography_pattern))
        return False

    print("  [INFO] Buildography files found: {}".format(len(buildography_files)))
    for f in buildography_files:
        print("  [INFO]   {}".format(os.path.basename(f)))

    sources_dir = os.path.join(RESULTS_DIR, project_name, "sources")
    signatures_pattern = os.path.join(sources_dir, "*_src.json")
    signatures_files = sorted(glob.glob(signatures_pattern))
    if not signatures_files:
        print("  [ERROR] No *_src.json found: {}".format(signatures_pattern))
        return False

    print("  [INFO] Source signature files found: {}".format(len(signatures_files)))
    for f in signatures_files:
        print("  [INFO]   {}".format(os.path.basename(f)))

    output_dir = os.path.join(RESULTS_DIR, project_name, "izb")
    os.makedirs(output_dir, exist_ok=True)

    try:
        signatures = load_signatures(signatures_files)
        buildography_hashes, raw_cmds = load_buildography_data(buildography_files)
        bin_hashes, bin_paths = load_bin_signatures(project_name)
    except Exception as e:
        print("  [ERROR] Failed to load data: {}".format(e))
        import traceback
        traceback.print_exc()
        return False

    # Загружаем bin_entries (path + hash) для Прохода 4
    bin_json_path = os.path.join(RESULTS_DIR, project_name, "sources",
                                 "{}_bin.json".format(project_name))
    bin_entries = []
    if os.path.isfile(bin_json_path):
        try:
            with open(bin_json_path, 'r', encoding='utf-8') as f:
                bin_data = json.load(f)
            raw_files = bin_data if isinstance(bin_data, list) else bin_data.get('files', [])
            for item in raw_files:
                p = item.get('path', '').strip()
                h = item.get('hash', '').strip()
                if p or h:
                    bin_entries.append({'path': p, 'hash': h})
            print("  [INFO] bin.json loaded: {} entries".format(len(bin_entries)))
        except Exception as e:
            print("  [WARN] Could not load bin.json: {}".format(e))
    else:
        print("  [WARN] bin.json not found: {} — Pass 4 will be skipped".format(bin_json_path))

    # src_hashes — множество хешей из src.json (все загруженные signatures)
    src_hashes = {
        entry.get('hash', '').strip()
        for entry in signatures
        if entry.get('hash', '').strip()
    }

    # --- Проход 1 ---
    print("  [INFO] Starting pass 1 (hash analysis)...")
    direct, parent, redundant = analyze_pass1(signatures, buildography_hashes)

    # --- Проход 2 (компиляторы) ---
    if compiler_basenames:
        print("  [INFO] Starting pass 2 (transitive closure from bin using compilers)...")
        good_compiler_input_keys = build_good_compiler_inputs(
            raw_cmds, compiler_basenames, bin_hashes, bin_paths
        )
        print("  [INFO] Good compiler input keys: {}".format(len(good_compiler_input_keys)))
        direct, parent, redundant, not_compiled = analyze_pass2(
            direct, parent, redundant, good_compiler_input_keys
        )
    else:
        print("  [WARN] Pass 2 skipped (no compiler list)")
        not_compiled = []

    # --- Проход 3 (интерпретаторы) ---
    if interpreter_basenames:
        print("  [INFO] Starting pass 3 (interpreted languages)...")
        input_files, output_files = build_interpreted_files_with_cmds(raw_cmds, interpreter_basenames)
        print("  [INFO] Interpreted input files: {}, output files: {}".format(len(input_files), len(output_files)))
        executed, compiled_used, compiled_unused, copied, izb = analyze_interpreted(
            signatures, input_files, output_files, bin_hashes, bin_paths, raw_cmds
        )
    else:
        print("  [WARN] Pass 3 skipped (no interpreter list)")
        executed = compiled_used = compiled_unused = copied = izb = []

    # --- Проход 4 (происхождение файлов дистрибутива) ---
    if bin_entries:
        print("  [INFO] Starting pass 4 (distrib origin check)...")
        p4_untraced, p4_ext_unbuilt, p4_ext_sources, p4_traced = analyze_pass4(
            bin_entries, src_hashes, raw_cmds
        )
        p4_ext_sources = enrich_external_sources_with_paths(p4_ext_sources, raw_cmds)
    else:
        print("  [WARN] Pass 4 skipped (no bin entries)")
        p4_untraced = p4_ext_unbuilt = p4_ext_sources = p4_traced = []

    # --- Запись JSON результатов ---
    print("  [INFO] Writing JSON results...")
    write_json_result(os.path.join(output_dir, "{}_direct.json".format(project_name)), 'direct', direct)
    write_json_result(os.path.join(output_dir, "{}_parent.json".format(project_name)), 'parent', parent)
    write_json_result(os.path.join(output_dir, "{}_redundant.json".format(project_name)), 'redundant', redundant)
    write_json_result(os.path.join(output_dir, "{}_not_compiled.json".format(project_name)), 'not_compiled', not_compiled)
    write_json_result(os.path.join(output_dir, "{}_interpreted_executed.json".format(project_name)), 'interpreted_executed', executed)
    write_json_result(os.path.join(output_dir, "{}_interpreted_compiled_used.json".format(project_name)), 'interpreted_compiled_used', compiled_used)
    write_json_result(os.path.join(output_dir, "{}_interpreted_compiled_unused.json".format(project_name)), 'interpreted_compiled_unused', compiled_unused)
    write_json_result(os.path.join(output_dir, "{}_interpreted_copied.json".format(project_name)), 'interpreted_copied', copied)
    write_json_result(os.path.join(output_dir, "{}_interpreted_izb.json".format(project_name)), 'interpreted_izb', izb)

    # --- Запись JSON результатов Прохода 4 ---
    if bin_entries:
        print("  [INFO] Writing pass 4 JSON results...")
        write_json_result(os.path.join(output_dir, "{}_pass4_untraced.json".format(project_name)),
                          'pass4_untraced', p4_untraced)
        write_json_result(os.path.join(output_dir, "{}_pass4_external_unbuilt.json".format(project_name)),
                          'pass4_external_unbuilt', p4_ext_unbuilt)
        write_json_result(os.path.join(output_dir, "{}_pass4_external_sources.json".format(project_name)),
                          'pass4_external_sources', p4_ext_sources)
        write_json_result(os.path.join(output_dir, "{}_pass4_traced.json".format(project_name)),
                          'pass4_traced', p4_traced)

    # --- Запись текстовых файлов ---
    print("  [INFO] Writing text results...")
    hash_algorithm = read_hash_cmd(GENERATE_JSON_SCRIPT)
    txt_path = os.path.join(output_dir, "{}_redundant.txt".format(project_name))
    write_redundant_txt(txt_path, project_name, redundant, not_compiled, hash_algorithm)

    izb_txt_path = os.path.join(output_dir, "{}_interpreted_izb.txt".format(project_name))
    write_interpreted_izb_txt(izb_txt_path, izb)

    compiled_unused_txt = os.path.join(output_dir, "{}_interpreted_compiled_unused.txt".format(project_name))
    write_interpreted_izb_txt(compiled_unused_txt, compiled_unused)

    executed_txt_path = os.path.join(output_dir, "{}_interpreted_executed.txt".format(project_name))
    write_interpreted_executed_txt(executed_txt_path, executed)

    # --- Запись текстовых файлов Прохода 4 ---
    if bin_entries:
        print("  [INFO] Writing pass 4 text results...")
        write_pass4_txt(
            os.path.join(output_dir, "{}_pass4_untraced.txt".format(project_name)),
            'untraced (not found in tracer at all)', p4_untraced)
        write_pass4_txt(
            os.path.join(output_dir, "{}_pass4_external_unbuilt.txt".format(project_name)),
            'external_unbuilt (in deps but not built in this build)', p4_ext_unbuilt)
        write_pass4_txt(
            os.path.join(output_dir, "{}_pass4_external_sources.txt".format(project_name)),
            'external_sources (built but deps not in src.json)', p4_ext_sources)
        write_pass4_txt(
            os.path.join(output_dir, "{}_pass4_traced.txt".format(project_name)),
            'traced (fully verified origin)', p4_traced)

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
    print("  Избыточные (izb)                                   : {:>7}  ({:.1f}%)".format(len(izb),             pct(len(izb),             total_interp)))

    print("\n  Итого ({} файлов)".format(total_all))
    print(sep)
    print("  Используются                                       : {:>7}  ({:.1f}%)".format(total_all - total_izb, pct(total_all - total_izb, total_all)))
    print("  Избыточные (not_compiled + compiled_unused + izb)  : {:>7}  ({:.1f}%)".format(total_izb,             pct(total_izb,             total_all)))

    if bin_entries:
        total_bin = len(bin_entries)
        print("\n  Происхождение файлов дистрибутива ({} файлов)".format(total_bin))
        print(sep)
        print("  Traced (подтверждённое)          : {:>7}  ({:.1f}%)".format(
            len(p4_traced),     pct(len(p4_traced),     total_bin)))
        print("  Untraced (нет в трассировщике)   : {:>7}  ({:.1f}%)".format(
            len(p4_untraced),   pct(len(p4_untraced),   total_bin)))
        print("  External unbuilt (готовый извне) : {:>7}  ({:.1f}%)".format(
            len(p4_ext_unbuilt), pct(len(p4_ext_unbuilt), total_bin)))
        print("  External sources (чужие исходники): {:>7}  ({:.1f}%)".format(
            len(p4_ext_sources), pct(len(p4_ext_sources), total_bin)))

    return True


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
    args = parser.parse_args()

    if not os.path.isdir(BUILDOGRAPHY_DIR):
        print("[ERROR] Buildography directory not found: {}".format(BUILDOGRAPHY_DIR))
        sys.exit(1)

    if not os.path.isdir(RESULTS_DIR):
        print("[ERROR] Results directory not found: {}".format(RESULTS_DIR))
        sys.exit(1)

    compiler_basenames, interpreter_basenames = load_utilities_lists(UTILITIES_FILE)

    if args.single_project:
        project_dir = os.path.join(BUILDOGRAPHY_DIR, args.single_project)
        if not os.path.isdir(project_dir):
            print("[ERROR] Project not found: {}".format(project_dir))
            sys.exit(1)
        projects = [args.single_project]
    else:
        projects = get_all_projects()
        if not projects:
            print("[ERROR] No projects found in: {}".format(BUILDOGRAPHY_DIR))
            sys.exit(1)

    print("[INFO] Projects to analyze: {}".format(len(projects)))
    print("[INFO] Projects: {}".format(', '.join(projects)))
    print("[INFO] UTILITIES_FILE: {}".format(UTILITIES_FILE))
    print("[INFO] Compilers: {}, Interpreters: {}".format(len(compiler_basenames), len(interpreter_basenames)))

    start_time = datetime.now()
    results = {}

    for project_name in projects:
        results[project_name] = process_project(project_name, compiler_basenames, interpreter_basenames)

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