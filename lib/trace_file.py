#!/usr/bin/env python3
"""
trace_file.py — диагностика происхождения файла из interpreted_compiled_unused

ИСПОЛЬЗОВАНИЕ:
    python3 trace_file.py --project PROJ --file foo.py
    python3 trace_file.py --project PROJ --hash d7cd04b5...
    python3 trace_file.py --project PROJ --file foo.py --bin-json results/PROJ/sources/PROJ_bin.json

ОПИСАНИЕ:
    Принимает имя файла или хеш из interpreted_compiled_unused.
    1. Находит файл в buildography — показывает кто его создал и кто читал.
    2. Ищет похожие файлы в bin.json по basename.
"""

import json
import os
import glob
import sys
import argparse
import re

# =============================================================================
# НАСТРАИВАЕМЫЕ ПУТИ
# =============================================================================
BASE_DIR         = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILDOGRAPHY_DIR = os.path.join(BASE_DIR, "buildography", "builds")
RESULTS_DIR      = os.path.join(BASE_DIR, "results")
# =============================================================================


def find_in_buildography(buildography_files, target_hash=None, target_basename=None):
    """
    Ищет файл в buildography по хешу или basename.
    Возвращает список совпадений: {cmd, section, path, hash}.
    """
    matches = []

    for fpath in buildography_files:
        try:
            with open(fpath, 'r', encoding='utf-8', errors='replace') as f:
                data = json.load(f, strict=False)
        except Exception as e:
            print(f"  [WARN] Could not read {os.path.basename(fpath)}: {e}")
            continue

        for cmd in data.get('component_commands', []):
            cl = cmd.get('command', [])
            tool = os.path.basename(str(cl[0])) if cl else '?'
            cmd_str = ' '.join(str(x) for x in cl[:6])
            if len(cl) > 6:
                cmd_str += ' ...'

            for section in ('output', 'dependencies', 'modified'):
                items = cmd.get(section) or []
                if isinstance(items, dict):
                    items = [{'path': p, 'hash': h} for p, h in items.items()]

                for item in items:
                    if not isinstance(item, dict):
                        continue
                    p = item.get('path', '')
                    h = item.get('hash', '').strip()
                    pre_h = item.get('pre_hash', '').strip()
                    bn = os.path.basename(p)

                    hit = False
                    if target_hash and (h == target_hash or pre_h == target_hash):
                        hit = True
                    if target_basename and _basename_matches(bn, target_basename):
                        hit = True

                    if hit:
                        matches.append({
                            'cmd_id':   cmd.get('id'),
                            'tool':     tool,
                            'cmd_str':  cmd_str,
                            'section':  section,
                            'path':     p,
                            'hash':     h,
                            'pre_hash': pre_h,
                        })

        del data

    return matches


def _basename_matches(bn, target_bn):
    """
    Сравнивает базовые имена файлов без расширения.
    foo.py == foo.cpython-36.pyc == foo.pyc == foo
    """
    def stem(name):
        # Убираем .pyc, .cpython-36m.pyc и аналоги
        name = re.sub(r'\.cpython-\d+[^.]*\.py[co]$', '', name)
        name = re.sub(r'\.py[co]$', '', name)
        name = re.sub(r'\.py$', '', name)
        return name.lower()

    return stem(bn) == stem(target_bn)


def find_in_bin_json(bin_json_path, target_basename):
    """
    Ищет похожие файлы в bin.json по basename.
    Возвращает список {path, hash}.
    """
    if not os.path.isfile(bin_json_path):
        return []

    try:
        with open(bin_json_path, 'r', encoding='utf-8', errors='replace') as f:
            data = json.load(f)
    except Exception as e:
        print(f"  [WARN] Could not read bin.json: {e}")
        return []

    sigs = data if isinstance(data, list) else \
           data.get('signatures', data.get('files', []))

    results = []
    for e in sigs:
        p = e.get('path', '')
        h = e.get('hash', '').strip()
        bn = os.path.basename(p)
        if _basename_matches(bn, target_basename):
            results.append({'path': p, 'hash': h})

    return sorted(results, key=lambda x: x['path'])


def find_in_unused_report(project_name, target_file=None, target_hash=None):
    """
    Ищет запись в interpreted_compiled_unused.txt.
    Возвращает (path, hash) или (None, None).
    """
    # Ищем в try1, try2... берём последний
    pattern = os.path.join(RESULTS_DIR, project_name, "izb", "try*",
                           "pass3", f"*_compiled_unused.txt")
    files = sorted(glob.glob(pattern))
    if not files:
        return None, None

    report = files[-1]
    with open(report, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split('\t')
            if len(parts) < 2:
                continue
            path, hash_ = parts[0], parts[1]
            bn = os.path.basename(path)
            if target_hash and hash_.strip() == target_hash:
                return path, hash_.strip()
            if target_file and _basename_matches(bn, target_file):
                return path, hash_.strip()

    return None, None


def print_section(title):
    print()
    print('=' * 60)
    print(f'  {title}')
    print('=' * 60)


def main():
    parser = argparse.ArgumentParser(
        description='Диагностика происхождения файла из interpreted_compiled_unused'
    )
    parser.add_argument('-p', '--project', required=True,
                        help='Имя проекта (как в buildography/builds/)')
    parser.add_argument('-f', '--file',
                        help='Имя файла (basename или полный путь)')
    parser.add_argument('--hash',
                        help='Хеш файла из отчёта')
    parser.add_argument('--bin-json',
                        help='Путь к bin.json (по умолчанию: results/PROJ/sources/PROJ_bin.json)')
    args = parser.parse_args()

    if not args.file and not args.hash:
        parser.error('Укажите --file или --hash')

    project = args.project
    target_file = os.path.basename(args.file) if args.file else None
    target_hash = args.hash.strip().lower() if args.hash else None

    # --- Пути ---
    buildography_dir = os.path.join(BUILDOGRAPHY_DIR, project)
    if not os.path.isdir(buildography_dir):
        print(f'[ERROR] Project not found: {buildography_dir}')
        sys.exit(1)

    buildography_files = sorted(glob.glob(os.path.join(buildography_dir, '*.json')))
    if not buildography_files:
        print(f'[ERROR] No buildography JSON in: {buildography_dir}')
        sys.exit(1)

    bin_json_path = args.bin_json or os.path.join(
        RESULTS_DIR, project, "sources", f"{project}_bin.json")

    # --- Шаг 0: ищем в отчёте ---
    print_section('0. Поиск в interpreted_compiled_unused')
    rep_path, rep_hash = find_in_unused_report(project, target_file, target_hash)
    if rep_path:
        print(f'  Найден в отчёте:')
        print(f'    path : {rep_path}')
        print(f'    hash : {rep_hash}')
        # Если передали только имя — берём хеш из отчёта
        if not target_hash:
            target_hash = rep_hash
        if not target_file:
            target_file = os.path.basename(rep_path)
    else:
        print(f'  Не найден в отчёте (ищем по {"хешу" if target_hash else "имени"})')

    # --- Шаг 1: buildography ---
    print_section('1. Buildography — где встречается файл')
    print(f'  Файлов buildography: {len(buildography_files)}')
    print(f'  Ищем: hash={target_hash or "—"}  basename={target_file or "—"}')

    matches = find_in_buildography(buildography_files, target_hash, target_file)

    if not matches:
        print('  Не найден ни в одной команде buildography.')
    else:
        # Группируем по cmd_id
        by_cmd = {}
        for m in matches:
            by_cmd.setdefault(m['cmd_id'], []).append(m)

        print(f'  Найдено совпадений: {len(matches)} в {len(by_cmd)} командах\n')
        for cmd_id, entries in sorted(by_cmd.items()):
            tool = entries[0]['tool']
            cmd_str = entries[0]['cmd_str']
            print(f'  Команда id={cmd_id}  tool={tool}')
            print(f'    {cmd_str}')
            for e in entries:
                pre = f'  pre_hash={e["pre_hash"][:16]}...' if e['pre_hash'] else ''
                print(f'    [{e["section"]:12s}] {e["path"]}')
                print(f'                  hash={e["hash"][:16]}...{pre}')

    # --- Шаг 2: bin.json ---
    print_section('2. bin.json — похожие файлы в дистрибутиве')
    if not target_file:
        print('  Basename не известен — пропускаем.')
    elif not os.path.isfile(bin_json_path):
        print(f'  bin.json не найден: {bin_json_path}')
    else:
        similar = find_in_bin_json(bin_json_path, target_file)
        if not similar:
            print(f'  Похожих файлов не найдено (basename ~ {target_file})')
        else:
            print(f'  Найдено похожих: {len(similar)}\n')
            for e in similar:
                match_mark = ' ✓' if target_hash and e['hash'] == target_hash else ''
                print(f'  {e["path"]}{match_mark}')
                print(f'    hash={e["hash"][:32]}...')

    print()


if __name__ == '__main__':
    main()
