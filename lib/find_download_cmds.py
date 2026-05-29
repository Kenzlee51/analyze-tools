#!/usr/bin/env python3
"""
=============================================================================
find_download_cmds.py — Поиск команд скачивания/установки в buildography
=============================================================================

ОПИСАНИЕ:
    Два режима поиска в buildography:

    Режим 1 (--tools): ищет команды по имени инструмента
        apt, apt-get, pip, wget, curl, dpkg и т.д.

    Режим 2 (--packages): ищет имена пакетов/файлов в dependencies и output
        любых команд — показывает как трассировщик видит эти файлы
        (как вход или как выход, и в какой команде).

    Оба режима можно использовать одновременно.

ИСПОЛЬЗОВАНИЕ:
    python3 find_download_cmds.py [OPTIONS]

ОПЦИИ:
    --single-project NAME   Обработать только один проект
    --tools LIST            Инструменты через запятую: apt,pip,wget
                            (по умолчанию: все известные)
    --packages LIST         Имена пакетов/файлов через запятую: apache2,ffmpeg
                            Ищет подстроку в путях dependencies и output
    --no-tools              Отключить режим 1 (только --packages)
    -h, --help              Показать справку

ПРИМЕРЫ:
    python3 find_download_cmds.py --single-project KTDL.00554-01
    python3 find_download_cmds.py --packages apache2,ffmpeg,librabbitmq4
    python3 find_download_cmds.py --no-tools --packages apache2,ffmpeg

=============================================================================
"""

import json
import os
import sys
import glob
import argparse

BASE_DIR         = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILDOGRAPHY_DIR = os.path.join(BASE_DIR, "buildography", "builds")

# Инструменты которые ищем по умолчанию (режим 1)
DEFAULT_TOOLS = {
    'apt', 'apt-get', 'apt-cache', 'apt-download', 'dpkg', 'dpkg-deb',
    'pip', 'pip2', 'pip3', 'pip3.5', 'pip3.6', 'pip3.7', 'pip3.8',
    'pip3.9', 'pip3.10', 'pip3.11',
    'wget', 'curl',
    'npm', 'yarn', 'npx',
    'gem', 'bundle', 'bundler',
    'cargo',
    'mvn', 'gradle', 'gradlew',
}


def _extract_paths(field):
    """Извлекает список путей из поля output или dependencies."""
    if isinstance(field, dict):
        return list(field.keys())
    elif isinstance(field, list):
        result = []
        for item in field:
            if isinstance(item, dict):
                p = item.get('path', '')
                if p:
                    result.append(p)
            else:
                result.append(str(item))
        return result
    return []


def _extract_hashes(field):
    """Извлекает dict {path: hash} из поля output или dependencies."""
    if isinstance(field, dict):
        return field
    elif isinstance(field, list):
        result = {}
        for item in field:
            if isinstance(item, dict):
                p = item.get('path', '')
                h = item.get('hash', '')
                if p:
                    result[p] = h
        return result
    return {}


def search_project_tools(project_name, buildography_dir, search_tools):
    """Режим 1: ищет команды по имени инструмента."""
    pattern = os.path.join(buildography_dir, project_name, "*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        return [], 0

    found = []
    total_cmds = 0

    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                data = json.load(f, strict=False)
        except Exception as e:
            print("  [ERROR] Failed to read {}: {}".format(file_path, e))
            continue

        cmds = data.get('component_commands', [])
        total_cmds += len(cmds)

        for cmd in cmds:
            cmd_list = cmd.get('command', [])
            if not cmd_list:
                continue
            tool = os.path.basename(str(cmd_list[0]))

            matched = tool in search_tools
            if not matched:
                for t in search_tools:
                    if t in tool.lower():
                        matched = True
                        break
            if not matched:
                continue

            out_paths = _extract_paths(cmd.get('output', {}))[:5]
            dep_paths = _extract_paths(cmd.get('dependencies', {}))[:3]

            found.append({
                'tool':    tool,
                'command': cmd_list[:6],
                'outputs': out_paths,
                'deps':    dep_paths,
                'file':    os.path.basename(file_path),
            })

        del data, cmds

    return found, total_cmds


def search_project_packages(project_name, buildography_dir, package_patterns):
    """
    Режим 2: ищет имена пакетов в dependencies и output всех команд.
    Для каждого найденного вхождения показывает:
      - роль файла: INPUT (зависимость) или OUTPUT (выход)
      - инструмент команды
      - полный путь файла
    """
    pattern = os.path.join(buildography_dir, project_name, "*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        return [], 0

    found = []
    total_cmds = 0

    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                data = json.load(f, strict=False)
        except Exception as e:
            print("  [ERROR] Failed to read {}: {}".format(file_path, e))
            continue

        cmds = data.get('component_commands', [])
        total_cmds += len(cmds)

        for cmd in cmds:
            cmd_list = cmd.get('command', [])
            tool = os.path.basename(str(cmd_list[0])) if cmd_list else '?'

            out_map = _extract_hashes(cmd.get('output', {}))
            dep_map = _extract_hashes(cmd.get('dependencies', {}))

            for pkg in package_patterns:
                pkg_lower = pkg.lower()

                # Ищем в выходах
                for path, h in out_map.items():
                    if pkg_lower in path.lower():
                        found.append({
                            'package': pkg,
                            'role':    'OUTPUT',
                            'tool':    tool,
                            'command': cmd_list[:4],
                            'path':    path,
                            'hash':    h,
                            'file':    os.path.basename(file_path),
                        })

                # Ищем в зависимостях
                for path, h in dep_map.items():
                    if pkg_lower in path.lower():
                        found.append({
                            'package': pkg,
                            'role':    'INPUT',
                            'tool':    tool,
                            'command': cmd_list[:4],
                            'path':    path,
                            'hash':    h,
                            'file':    os.path.basename(file_path),
                        })

        del data, cmds

    return found, total_cmds


def print_tools_results(results):
    if not results:
        print("  No download/install commands found.")
        return
    print("  Found {} matching command(s):".format(len(results)))
    for r in results:
        print()
        print("  Tool   : {}".format(r['tool']))
        print("  Command: {}".format(' '.join(str(x) for x in r['command'])))
        if r['outputs']:
            print("  Outputs:")
            for o in r['outputs']:
                print("    {}".format(o))
        if r['deps']:
            print("  Deps:")
            for d in r['deps']:
                print("    {}".format(d))


def print_packages_results(results):
    if not results:
        print("  Packages not found in any command dependencies or outputs.")
        print("  => Tracer does not see these files at all => untraced_external")
        return

    # Группируем по пакету
    by_pkg = {}
    for r in results:
        by_pkg.setdefault(r['package'], []).append(r)

    for pkg, entries in sorted(by_pkg.items()):
        print()
        print("  Package pattern: '{}'  ({} occurrence(s))".format(pkg, len(entries)))

        # Группируем по роли
        inputs  = [e for e in entries if e['role'] == 'INPUT']
        outputs = [e for e in entries if e['role'] == 'OUTPUT']

        if outputs:
            print("  As OUTPUT of commands (трассировщик видит как результат):")
            # Уникальные инструменты
            tools_seen = {}
            for e in outputs:
                tools_seen.setdefault(e['tool'], []).append(e['path'])
            for tool, paths in sorted(tools_seen.items()):
                print("    tool={} : {} path(s)".format(tool, len(paths)))
                for p in paths[:3]:
                    print("      {}".format(p))
                if len(paths) > 3:
                    print("      ... and {} more".format(len(paths) - 3))

        if inputs:
            print("  As INPUT of commands (трассировщик видит как зависимость):")
            tools_seen = {}
            for e in inputs:
                tools_seen.setdefault(e['tool'], []).append(e['path'])
            for tool, paths in sorted(tools_seen.items()):
                print("    tool={} : {} path(s)".format(tool, len(paths)))
                for p in paths[:3]:
                    print("      {}".format(p))
                if len(paths) > 3:
                    print("      ... and {} more".format(len(paths) - 3))

        # Вывод о категории в analyze-json
        if outputs and not inputs:
            print("  => В buildography есть как OUTPUT, нет как INPUT")
            print("     Вероятная категория: external_prebuilt или untraced_from_src")
        elif inputs and not outputs:
            print("  => Только как INPUT (зависимость)")
            print("     Вероятная категория: external_prebuilt")
        elif outputs and inputs:
            print("  => Есть и как OUTPUT и как INPUT")
            print("     Вероятная категория: external_built или compiled_from_src")
        else:
            print("  => Не найден ни как INPUT ни как OUTPUT => untraced_external")


def main():
    parser = argparse.ArgumentParser(
        description='Поиск команд скачивания и пакетов в buildography'
    )
    parser.add_argument('--single-project', metavar='NAME',
                        help='Обработать только один проект')
    parser.add_argument('--tools', metavar='LIST',
                        help='Инструменты через запятую (по умолчанию: все)')
    parser.add_argument('--no-tools', action='store_true',
                        help='Отключить режим 1 (поиск по инструменту)')
    parser.add_argument('--packages', metavar='LIST',
                        help='Имена пакетов через запятую: apache2,ffmpeg')
    args = parser.parse_args()

    if not os.path.isdir(BUILDOGRAPHY_DIR):
        print("[ERROR] Buildography directory not found: {}".format(BUILDOGRAPHY_DIR))
        sys.exit(1)

    # Режим 1
    run_tools = not args.no_tools
    if run_tools:
        if args.tools:
            search_tools = set(t.strip() for t in args.tools.split(',') if t.strip())
        else:
            search_tools = DEFAULT_TOOLS
    else:
        search_tools = set()

    # Режим 2
    if args.packages:
        package_patterns = [p.strip() for p in args.packages.split(',') if p.strip()]
    else:
        package_patterns = []

    if not run_tools and not package_patterns:
        print("[ERROR] Nothing to search. Use --tools or --packages.")
        sys.exit(1)

    # Определяем проекты
    if args.single_project:
        proj_dir = os.path.join(BUILDOGRAPHY_DIR, args.single_project)
        if not os.path.isdir(proj_dir):
            print("[ERROR] Project not found: {}".format(proj_dir))
            sys.exit(1)
        projects = [args.single_project]
    else:
        projects = sorted([
            e for e in os.listdir(BUILDOGRAPHY_DIR)
            if os.path.isdir(os.path.join(BUILDOGRAPHY_DIR, e))
        ])
        if not projects:
            print("[ERROR] No projects found in: {}".format(BUILDOGRAPHY_DIR))
            sys.exit(1)

    if run_tools:
        print("Mode 1 — searching for tools: {}".format(
            ', '.join(sorted(search_tools))))
    if package_patterns:
        print("Mode 2 — searching for packages: {}".format(
            ', '.join(package_patterns)))
    print("Buildography: {}".format(BUILDOGRAPHY_DIR))
    print("=" * 60)

    all_tools_found = []
    all_pkgs_found  = []
    total_cmds_all  = 0

    for project_name in projects:
        print("\n[{}]".format(project_name))
        total_cmds = 0

        if run_tools:
            t_found, t_cmds = search_project_tools(
                project_name, BUILDOGRAPHY_DIR, search_tools)
            total_cmds = max(total_cmds, t_cmds)
            print("  --- Mode 1: tool search ---")
            print_tools_results(t_found)
            all_tools_found.extend(t_found)

        if package_patterns:
            p_found, p_cmds = search_project_packages(
                project_name, BUILDOGRAPHY_DIR, package_patterns)
            total_cmds = max(total_cmds, p_cmds)
            print("  --- Mode 2: package search ---")
            print_packages_results(p_found)
            all_pkgs_found.extend(p_found)

        total_cmds_all += total_cmds
        print("\n  Total commands scanned: {}".format(total_cmds))

    print("\n" + "=" * 60)
    print("Summary:")
    print("  Projects scanned : {}".format(len(projects)))
    print("  Total commands   : {}".format(total_cmds_all))

    if run_tools:
        print("  Tool matches     : {}".format(len(all_tools_found)))
        if all_tools_found:
            by_tool = {}
            for r in all_tools_found:
                by_tool.setdefault(r['tool'], 0)
                by_tool[r['tool']] += 1
            print("  By tool:")
            for tool, count in sorted(by_tool.items(), key=lambda x: -x[1]):
                print("    {:20s} : {}".format(tool, count))
        else:
            print("  => Tracer does not capture download/install commands.")
            print("     Packages from apt/pip/wget will appear as untraced_external.")

    if package_patterns:
        print("  Package matches  : {}".format(len(all_pkgs_found)))
        if not all_pkgs_found:
            print("  => None of the requested packages found in buildography.")
            print("     They will appear as untraced_external in the report.")

    print("=" * 60)


if __name__ == '__main__':
    main()
