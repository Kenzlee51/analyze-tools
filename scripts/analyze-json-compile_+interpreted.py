#!/usr/bin/env python3
"""
=============================================================================
analyze-json.py - Скрипт анализа избыточных файлов исходных текстов
=============================================================================

ОПИСАНИЕ:
    Сравнивает JSON сигнатур исходных текстов с JSON buildography и
    распределяет файлы по категориям. Добавлена поддержка Python-файлов,
    которые были выполнены (interpreted_executed) с выводом команд запуска.

    Категории для интерпретируемых языков:
      - interpreted_executed : только Python-файлы, которые были входными для команд интерпретаторов
                               (содержат поле "commands" — список полных команд)
      - interpreted_compiled : файлы, сгенерированные интерпретаторами, или входные,
                               чьи выходы попали в дистрибутив
      - interpreted_copied   : файлы, присутствующие в bin.json, но не вошедшие в первые две
      - interpreted_izb      : остальные (избыточные) интерпретируемые файлы

    Для компилируемых языков:
      - Избыточными считаются те файлы, которые были входами команд компиляторов/линковщиков,
        но ни один выходной файл этих команд не попал в bin.json.
      - Файлы, вообще не бывшие входами компиляторов, также считаются избыточными.

    Выходные текстовые файлы:
      - {project}_redundant.txt              : путь<TAB>хеш (объединение redundant + not_compiled)
      - {project}_interpreted_izb.txt        : путь<TAB>хеш (избыточные интерпретируемые)
      - {project}_interpreted_executed.txt   : путь<TAB>хеш (выполненные Python-файлы, без команд)

ИСПОЛЬЗОВАНИЕ:
    python3 analyze-json.py [OPTIONS]

ОПЦИИ:
    --single-project NAME   Обработать только один указанный проект
    -h, --help              Показать справку

ПРИМЕРЫ:
    python3 analyze-json.py
    python3 analyze-json.py --single-project proj1
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
      - хотя бы один её выход является входом для другой "хорошей" команды.
    Проверка ведётся как по нормализованным путям, так и по хешам.
    """
    bin_hashes_set = set(bin_hashes)
    bin_paths_set = set(bin_paths)

    # Отображения: выход (путь или хеш) -> список индексов команд, которые используют его как зависимость
    output_path_to_consumer_cmds = {}
    output_hash_to_consumer_cmds = {}

    # Для каждой команды запомним выходы (пути и хеши) и входы (пути и хеши)
    cmd_output_paths = []   # список множеств норм. путей выходов
    cmd_output_hashes = []  # список множеств хешей выходов
    cmd_input_paths = []    # список множеств норм. путей входов
    cmd_input_hashes = []   # список множеств хешей входов

    for idx, cmd in enumerate(raw_cmds):
        out_paths = set()
        out_hashes = set()
        in_paths = set()
        in_hashes = set()

        # --- Выходы (output) ---
        outputs = cmd.get('output', {})
        if isinstance(outputs, dict):
            for path, h in outputs.items():
                if path:
                    norm_path = os.path.normpath(path)
                    out_paths.add(norm_path)
                if h:
                    out_hashes.add(h.strip())
        elif isinstance(outputs, list):
            for out in outputs:
                if isinstance(out, dict):
                    path = out.get('path', '')
                    h = out.get('hash', '')
                else:
                    path = str(out)
                    h = ''
                if path:
                    norm_path = os.path.normpath(path)
                    out_paths.add(norm_path)
                if h:
                    out_hashes.add(h.strip())

        # --- Входы (dependencies) ---
        deps = cmd.get('dependencies', {})
        if isinstance(deps, dict):
            for path, h in deps.items():
                if path:
                    norm_path = os.path.normpath(path)
                    in_paths.add(norm_path)
                if h:
                    in_hashes.add(h.strip())
        elif isinstance(deps, list):
            for dep in deps:
                if isinstance(dep, dict):
                    path = dep.get('path', '')
                    h = dep.get('hash', '')
                else:
                    path = str(dep)
                    h = ''
                if path:
                    norm_path = os.path.normpath(path)
                    in_paths.add(norm_path)
                if h:
                    in_hashes.add(h.strip())

        cmd_output_paths.append(out_paths)
        cmd_output_hashes.append(out_hashes)
        cmd_input_paths.append(in_paths)
        cmd_input_hashes.append(in_hashes)

        # Заполняем обратные отображения для выходов
        for p in out_paths:
            output_path_to_consumer_cmds.setdefault(p, set()).add(idx)
        for h in out_hashes:
            output_hash_to_consumer_cmds.setdefault(h, set()).add(idx)

    # Инициализируем "хорошие" команды: те, у которых хотя бы один выход напрямую в bin
    good_cmds = set()
    for idx in range(len(raw_cmds)):
        # Проверка по путям
        if any(p in bin_paths_set for p in cmd_output_paths[idx]):
            good_cmds.add(idx)
            continue
        # Проверка по хешам
        if any(h in bin_hashes_set for h in cmd_output_hashes[idx]):
            good_cmds.add(idx)

    # Распространяем "хорошесть" назад: если команда использует вход,
    # который является выходом уже "хорошей" команды, то текущая команда тоже хорошая.
    changed = True
    while changed:
        changed = False
        for idx in range(len(raw_cmds)):
            if idx in good_cmds:
                continue
            # Проверяем входные пути: есть ли среди них выход какой-либо хорошей команды?
            for in_path in cmd_input_paths[idx]:
                if in_path in output_path_to_consumer_cmds:
                    # Команды-производители этого пути
                    for producer_idx in output_path_to_consumer_cmds[in_path]:
                        if producer_idx in good_cmds:
                            good_cmds.add(idx)
                            changed = True
                            break
                if changed:
                    break
            if changed:
                continue
            # Проверяем входные хеши
            for in_hash in cmd_input_hashes[idx]:
                if in_hash in output_hash_to_consumer_cmds:
                    for producer_idx in output_hash_to_consumer_cmds[in_hash]:
                        if producer_idx in good_cmds:
                            good_cmds.add(idx)
                            changed = True
                            break
                if changed:
                    break

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

    for cmd_idx, cmd in enumerate(raw_cmds):
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
    # В signatures уже есть path_norm
    interpreted_entries = [s for s in signatures if is_interpreted_extension(s.get('path', ''))]

    executed = []
    compiled = []
    copied = []
    izb = []
    added_compiled_paths = set()

    # Цикл по интерпретируемым файлам
    for entry in interpreted_entries:
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

        # --- Проверка на выходной файл (compiled) ---
        is_output = False
        if h and h in output_hashes:
            is_output = True
        elif p_norm and p_norm in output_paths_norm:
            is_output = True

        if is_output:
            compiled.append({'path': path, 'hash': h})
            added_compiled_paths.add(p_norm)
            continue

        # --- Проверка на leads_to_bin (входной файл, чей выход попал в bin) ---
        leads_to_bin = False
        # Определяем, был ли файл входным для какой-либо команды интерпретатора
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
            compiled.append({'path': path, 'hash': h})
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

    # Добавляем в compiled все выходные файлы, которых нет в signatures
    for out in output_files:
        path = out.get('path', '')
        p_norm = out.get('path_norm', '')
        if not p_norm:
            continue
        if p_norm not in added_compiled_paths:
            compiled.append({'path': path, 'hash': out.get('hash', '')})
            added_compiled_paths.add(p_norm)

    return executed, compiled, copied, izb


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
        executed, compiled, copied, izb = analyze_interpreted(
            signatures, input_files, output_files, bin_hashes, bin_paths, raw_cmds
        )
    else:
        print("  [WARN] Pass 3 skipped (no interpreter list)")
        executed = compiled = copied = izb = []

    # --- Запись JSON результатов ---
    print("  [INFO] Writing JSON results...")
    write_json_result(os.path.join(output_dir, "{}_direct.json".format(project_name)), 'direct', direct)
    write_json_result(os.path.join(output_dir, "{}_parent.json".format(project_name)), 'parent', parent)
    write_json_result(os.path.join(output_dir, "{}_redundant.json".format(project_name)), 'redundant', redundant)
    write_json_result(os.path.join(output_dir, "{}_not_compiled.json".format(project_name)), 'not_compiled', not_compiled)
    write_json_result(os.path.join(output_dir, "{}_interpreted_executed.json".format(project_name)), 'interpreted_executed', executed)
    write_json_result(os.path.join(output_dir, "{}_interpreted_compiled.json".format(project_name)), 'interpreted_compiled', compiled)
    write_json_result(os.path.join(output_dir, "{}_interpreted_copied.json".format(project_name)), 'interpreted_copied', copied)
    write_json_result(os.path.join(output_dir, "{}_interpreted_izb.json".format(project_name)), 'interpreted_izb', izb)

    # --- Запись текстовых файлов ---
    print("  [INFO] Writing text results...")
    hash_algorithm = read_hash_cmd(GENERATE_JSON_SCRIPT)
    txt_path = os.path.join(output_dir, "{}_redundant.txt".format(project_name))
    write_redundant_txt(txt_path, project_name, redundant, not_compiled, hash_algorithm)

    izb_txt_path = os.path.join(output_dir, "{}_interpreted_izb.txt".format(project_name))
    write_interpreted_izb_txt(izb_txt_path, izb)

    executed_txt_path = os.path.join(output_dir, "{}_interpreted_executed.txt".format(project_name))
    write_interpreted_executed_txt(executed_txt_path, executed)

    # --- Статистика ---
    total_source = len(direct) + len(parent) + len(redundant)
    redundant_pct = (len(redundant) / total_source * 100) if total_source else 0

    print("\n  --- Results for {} ---".format(project_name))
    print("  Source files analyzed      : {}".format(total_source))
    print("  Direct (used)              : {}".format(len(direct)))
    print("  Parent (via archive)       : {}".format(len(parent)))
    print("  Redundant                  : {}".format(len(redundant)))
    print("  Not compiled               : {}".format(len(not_compiled)))
    print("  Interpreted executed (Python only): {}".format(len(executed)))
    print("  Interpreted compiled       : {}".format(len(compiled)))
    print("  Interpreted copied         : {}".format(len(copied)))
    print("  Interpreted izb            : {}".format(len(izb)))
    print("  Redundancy rate            : {:.1f}%".format(redundant_pct))

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