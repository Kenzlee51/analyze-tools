#!/usr/bin/env python3
"""
=============================================================================
analyze-json.py - Скрипт анализа избыточных файлов исходных текстов
=============================================================================

ОПИСАНИЕ:
    Сравнивает JSON сигнатур исходных текстов с JSON buildography и
    распределяет файлы по категориям:
      - PROJ_direct.json      - файлы найденные напрямую по хэшу
      - PROJ_parent.json      - файлы найденные через родительский архив
      - PROJ_redundant.json   - избыточные файлы (не используются при сборке)
      - PROJ_not_compiled.json - файлы компилируемых языков из direct/parent
                                 которые не прошли через компилятор
      - PROJ_redundant.txt    - объединённый список path+hash из redundant
                                 и not_compiled без дублирования по (path,hash)

    Логика анализа (два прохода):
      Проход 1: стандартный — direct / parent / redundant по хэшам buildography
      Проход 2: из direct и parent берём файлы компилируемых расширений,
                проверяем по utilities.yaml был ли файл входом компилятора.
                Не прошедшие — удаляются из direct/parent,
                добавляются в redundant и not_compiled.

ИСПОЛЬЗОВАНИЕ:
    python3 analyze-json.py [OPTIONS]

ОПЦИИ:
    --single-project NAME   Обработать только один указанный проект
    -h, --help              Показать справку

ПРИМЕРЫ:
    python3 analyze-json.py
    python3 analyze-json.py --single-project proj1

ОЖИДАЕМАЯ СТРУКТУРА:
    BASE_DIR/
    |-- scripts/
    |   +-- analyze-json.py
    |-- lib/
    |   +-- utilities.yaml           <- список компиляторов
    |-- buildography/
    |   +-- builds/
    |       |-- proj1/
    |       |   +-- *.json           <- один или несколько buildography JSON
    |       +-- proj2/
    |           +-- *.json
    |-- results/
        |-- proj1/
        |   |-- sources/
        |   |   +-- *_src.json       <- один или несколько, объединяются
        |   +-- izb/                 <- результаты анализа (авто)
        |       |-- proj1_direct.json
        |       |-- proj1_parent.json
        |       |-- proj1_redundant.json
        |       |-- proj1_not_compiled.json
        |       +-- proj1_redundant.txt   <- объединённый список (авто)
        +-- proj2/

ЗАВИСИМОСТИ:
    Python 3.4+ (только стандартная библиотека + PyYAML)
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

# Путь к bash скрипту из которого читается HASH_CMD
GENERATE_JSON_SCRIPT = os.path.join(BASE_DIR, "scripts", "generate_json_v2_test.sh")
# =============================================================================


# =============================================================================
# ЧТЕНИЕ HASH_CMD ИЗ BASH СКРИПТА
# Ищет строку вида: HASH_CMD="..."  или  HASH_CMD='...'  или  HASH_CMD=...
# Возвращает строку с командой или пустую строку если не найдено.
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

# --- Расширения исходных текстов ---
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

# Расширения компилируемых языков — для них применяем проход 2
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


# =============================================================================
# UTILITIES.YAML — загрузка списка компиляторов
# =============================================================================

def load_compiler_basenames(utilities_path):
    if not os.path.exists(utilities_path):
        print("[WARN] utilities.yaml not found: {}".format(utilities_path))
        print("[WARN] Compiler check (pass 2) will be SKIPPED")
        return None

    try:
        import yaml
        with open(utilities_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        compilers = data.get('utilities', {}).get('compilers', [])
        result = set(compilers)
        print("[INFO] Loaded {} compilers from utilities.yaml".format(len(result)))
        return result
    except ImportError:
        pass

    try:
        compilers = []
        in_compilers = False
        with open(utilities_path, 'r', encoding='utf-8') as f:
            for line in f:
                stripped = line.rstrip()
                if stripped.strip() == 'compilers:':
                    in_compilers = True
                    continue
                if in_compilers:
                    if stripped and not stripped.startswith(' ') and not stripped.startswith('\t'):
                        break
                    val = stripped.strip()
                    if val.startswith('- '):
                        compilers.append(val[2:].strip())
        result = set(compilers)
        print("[INFO] Loaded {} compilers from utilities.yaml (no PyYAML)".format(len(result)))
        return result
    except Exception as e:
        print("[WARN] Failed to parse utilities.yaml: {}".format(e))
        print("[WARN] Compiler check (pass 2) will be SKIPPED")
        return None


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
    ext = os.path.splitext(path)[1]
    return ext in COMPILED_EXTENSIONS or ext.lower() in COMPILED_EXTENSIONS


def get_versioned_filename(filepath):
    if not os.path.exists(filepath):
        return filepath
    base = filepath[:-5]
    version = 1
    while os.path.exists("{}_v{}.json".format(base, version)):
        version += 1
    return "{}_v{}.json".format(base, version)


# =============================================================================
# ЗАГРУЗКА ДАННЫХ
# =============================================================================

def load_signatures(paths):
    all_signatures = []
    for path in paths:
        print("  [INFO] Loading signatures: {}".format(os.path.basename(path)))
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            data = json.load(f)
        sigs = data.get('signatures', [])
        print("  [INFO] Signatures in {}: {}".format(os.path.basename(path), len(sigs)))
        all_signatures.extend(sigs)
    print("  [INFO] Total signatures (merged): {}".format(len(all_signatures)))
    return all_signatures


def load_buildography_data(paths):
    hashes   = set()
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


def build_compiler_inputs(raw_cmds, compiler_basenames):
    compiler_input_hashes = set()
    compiler_input_paths  = set()

    for cmd in raw_cmds:
        cmd_list = cmd.get('command', [])
        if not cmd_list:
            continue
        if os.path.basename(cmd_list[0]) not in compiler_basenames:
            continue

        deps = cmd.get('dependencies', {})
        if isinstance(deps, dict):
            for path, h in deps.items():
                h = h.strip()
                if h:
                    compiler_input_hashes.add(h)
                if path:
                    compiler_input_paths.add(os.path.normpath(path))
        elif isinstance(deps, list):
            for dep in deps:
                h = dep.get('hash', '').strip()
                if h:
                    compiler_input_hashes.add(h)
                p = dep.get('path', '')
                if p:
                    compiler_input_paths.add(os.path.normpath(p))

    return compiler_input_hashes, compiler_input_paths


# =============================================================================
# АНАЛИЗ — ПРОХОД 1
# =============================================================================

def analyze_pass1(signatures, buildography_hashes):
    direct    = []
    parent    = []
    redundant = []
    processed = 0

    for entry in signatures:
        path          = entry.get('path', '')
        file_hash     = entry.get('hash', '')
        parents_chain = entry.get('parents_chain', [])

        if not is_source_file(path):
            continue

        processed += 1
        if processed % 5000 == 0:
            print("  [INFO] Pass 1: analyzed {} source files...".format(processed))

        if file_hash in buildography_hashes:
            direct.append({'path': path, 'hash': file_hash})
            continue

        found_parent = None
        for ph in parents_chain:
            if ph in buildography_hashes:
                found_parent = ph
                break

        if found_parent:
            parent.append({'path': path, 'hash': file_hash, 'parent_hash': found_parent})
            continue

        redundant.append({'path': path, 'hash': file_hash})

    print("  [INFO] Pass 1 done: direct={}, parent={}, redundant={}".format(
        len(direct), len(parent), len(redundant)))
    return direct, parent, redundant


# =============================================================================
# АНАЛИЗ — ПРОХОД 2
# =============================================================================

def analyze_pass2(direct, parent, redundant, compiler_input_hashes, compiler_input_paths):
    direct_out    = []
    parent_out    = []
    not_compiled  = []
    moved_count   = 0

    def was_compiled(entry):
        h = entry.get('hash', '')
        p = entry.get('path', '')

        if h and h in compiler_input_hashes:
            return True

        if p:
            norm = os.path.normpath(p)
            if norm in compiler_input_paths:
                return True
            basename = os.path.basename(norm)
            for cp in compiler_input_paths:
                if os.path.basename(cp) == basename and (
                    cp.endswith(os.sep + norm) or norm.endswith(os.sep + cp)
                ):
                    return True

        return False

    for entry in direct:
        if is_compiled_extension(entry.get('path', '')):
            if not was_compiled(entry):
                not_compiled.append({
                    'path':   entry['path'],
                    'hash':   entry['hash'],
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
                    'path':        entry['path'],
                    'hash':        entry['hash'],
                    'parent_hash': entry.get('parent_hash', ''),
                    'source':      'parent',
                })
                redundant.append({'path': entry['path'], 'hash': entry['hash']})
                moved_count += 1
                continue
        parent_out.append(entry)

    print("  [INFO] Pass 2 done: moved to not_compiled={}".format(moved_count))
    print("  [INFO] Final: direct={}, parent={}, redundant={}, not_compiled={}".format(
        len(direct_out), len(parent_out), len(redundant), len(not_compiled)))

    return direct_out, parent_out, redundant, not_compiled


# =============================================================================
# ЗАПИСЬ РЕЗУЛЬТАТА JSON
# =============================================================================

def write_result(output_path, category, files):
    versioned_path = get_versioned_filename(output_path)
    if versioned_path != output_path:
        print("  [WARN] File exists, writing to: {}".format(
            os.path.basename(versioned_path)))

    result = {
        'category':     category,
        'total':        len(files),
        'generated_at': datetime.now().isoformat(),
        'files':        files,
    }
    with open(versioned_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    print("  [INFO] Written {} entries -> {}".format(len(files), versioned_path))


# =============================================================================
# ЗАПИСЬ ОБЪЕДИНЁННОГО TXT
# Объединяет redundant + not_compiled без дублирования по паре (path, hash).
# Если path разный — обе строки попадают.
# Если path одинаковый и hash одинаковый — одна строка.
# Если path одинаковый но hash разный — обе строки.
# =============================================================================

def write_redundant_txt(output_path, project_name, redundant, not_compiled, hash_algorithm=''):
    seen = set()   # пары (path, hash)
    rows = []

    for entry in redundant + not_compiled:
        path  = entry.get('path', '').strip()
        hash_ = entry.get('hash', '').strip()
        if not path or not hash_:
            continue
        key = (path, hash_)
        if key in seen:
            continue
        seen.add(key)
        rows.append((path, hash_))

    # Сортируем по пути для читаемости
    rows.sort(key=lambda x: x[0])

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("# Redundant files report: {}\n".format(project_name))
        f.write("# Generated: {}\n".format(datetime.now().isoformat()))
        f.write("# Total: {}\n".format(len(rows)))
        f.write("# Hash algorithm: {}\n".format(hash_algorithm if hash_algorithm else '(unknown)'))
        f.write("# Sources: redundant.json + not_compiled.json\n")
        f.write("# Format: path<TAB>hash\n")
        f.write("#\n")
        for path, hash_ in rows:
            f.write("{}\t{}\n".format(path, hash_))

    print("  [INFO] Written {} entries -> {}".format(len(rows), output_path))
    return len(rows)


# =============================================================================
# ОБРАБОТКА ОДНОГО ПРОЕКТА
# =============================================================================

def process_project(project_name, compiler_basenames):
    print("\n" + "=" * 50)
    print("Processing project: {}".format(project_name))
    print("=" * 50)

    buildography_pattern = os.path.join(BUILDOGRAPHY_DIR, project_name, "*.json")
    buildography_files   = sorted(glob.glob(buildography_pattern))

    if not buildography_files:
        print("  [ERROR] No buildography JSON found: {}".format(buildography_pattern))
        return False

    print("  [INFO] Buildography files found: {}".format(len(buildography_files)))
    for f in buildography_files:
        print("  [INFO]   {}".format(os.path.basename(f)))

    sources_dir        = os.path.join(RESULTS_DIR, project_name, "sources")
    signatures_pattern = os.path.join(sources_dir, "*_src.json")
    signatures_files   = sorted(glob.glob(signatures_pattern))

    if not signatures_files:
        print("  [ERROR] No *_src.json found: {}".format(signatures_pattern))
        return False

    print("  [INFO] Source signature files found: {}".format(len(signatures_files)))
    for f in signatures_files:
        print("  [INFO]   {}".format(os.path.basename(f)))

    output_dir = os.path.join(RESULTS_DIR, project_name, "izb")
    os.makedirs(output_dir, exist_ok=True)

    try:
        signatures                    = load_signatures(signatures_files)
        buildography_hashes, raw_cmds = load_buildography_data(buildography_files)
    except Exception as e:
        print("  [ERROR] Failed to load data: {}".format(e))
        import traceback
        print(traceback.format_exc())
        return False

    # --- Проход 1 ---
    print("  [INFO] Starting pass 1 (hash analysis)...")
    direct, parent, redundant = analyze_pass1(signatures, buildography_hashes)

    # --- Проход 2 ---
    if compiler_basenames is not None:
        print("  [INFO] Starting pass 2 (compiler check)...")
        compiler_input_hashes, compiler_input_paths = build_compiler_inputs(
            raw_cmds, compiler_basenames
        )
        print("  [INFO] Compiler input hashes: {}, paths: {}".format(
            len(compiler_input_hashes), len(compiler_input_paths)))
        direct, parent, redundant, not_compiled = analyze_pass2(
            direct, parent, redundant,
            compiler_input_hashes, compiler_input_paths
        )
    else:
        print("  [WARN] Pass 2 skipped (utilities.yaml not loaded)")
        not_compiled = []

    # --- Запись JSON ---
    print("  [INFO] Writing results...")
    write_result(
        os.path.join(output_dir, "{}_direct.json".format(project_name)),
        'direct', direct
    )
    write_result(
        os.path.join(output_dir, "{}_parent.json".format(project_name)),
        'parent', parent
    )
    write_result(
        os.path.join(output_dir, "{}_redundant.json".format(project_name)),
        'redundant', redundant
    )
    write_result(
        os.path.join(output_dir, "{}_not_compiled.json".format(project_name)),
        'not_compiled', not_compiled
    )

    # --- Запись объединённого TXT ---
    print("  [INFO] Writing redundant.txt...")
    hash_algorithm = read_hash_cmd(GENERATE_JSON_SCRIPT)
    txt_path  = os.path.join(output_dir, "{}_redundant.txt".format(project_name))
    txt_count = write_redundant_txt(txt_path, project_name, redundant, not_compiled, hash_algorithm)

    # --- Статистика ---
    total_source  = len(direct) + len(parent) + len(redundant)
    redundant_pct = (len(redundant) / total_source * 100) if total_source > 0 else 0

    print("\n  --- Results for {} ---".format(project_name))
    print("  Source files analyzed : {}".format(total_source))
    print("  Direct (used)         : {}".format(len(direct)))
    print("  Parent (via archive)  : {}".format(len(parent)))
    print("  Redundant             : {}".format(len(redundant)))
    print("  Not compiled          : {}".format(len(not_compiled)))
    print("  Redundancy rate       : {:.1f}%".format(redundant_pct))
    print("  redundant.txt entries : {}".format(txt_count))

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

    compiler_basenames = load_compiler_basenames(UTILITIES_FILE)

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
    print("[INFO] Compiler check: {}".format(
        "enabled ({} compilers)".format(len(compiler_basenames))
        if compiler_basenames else "DISABLED"
    ))

    start_time = datetime.now()
    results    = {}

    for project_name in projects:
        success = process_project(project_name, compiler_basenames)
        results[project_name] = success

    elapsed       = datetime.now() - start_time
    success_count = sum(1 for v in results.values() if v)
    fail_count    = len(results) - success_count

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
