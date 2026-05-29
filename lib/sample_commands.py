#!/usr/bin/env python3
"""
=============================================================================
sample_commands.py — Просмотр примеров команд из buildography
=============================================================================

ОПИСАНИЕ:
    Показывает примеры структуры команд из buildography.
    Помогает понять формат поля 'command' и других полей.

ИСПОЛЬЗОВАНИЕ:
    python3 sample_commands.py [OPTIONS]

ОПЦИИ:
    --single-project NAME   Обработать только один проект
    --tools LIST            Фильтр по инструментам через запятую: gcc,ld,cp
                            Если не задан — показывает все уникальные инструменты
    --n N                   Количество примеров на инструмент (default: 3)
    --full                  Показывать полную структуру команды (все поля)
    --tools-only            Только список уникальных инструментов без примеров
    -h, --help              Показать справку

ПРИМЕРЫ:
    python3 sample_commands.py --single-project KTDL.00554-01 --tools-only
    python3 sample_commands.py --single-project KTDL.00554-01 --tools gcc,ld,cc
    python3 sample_commands.py --single-project KTDL.00554-01 --n 1 --full

=============================================================================
"""

import json
import os
import sys
import glob
import argparse

BASE_DIR         = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILDOGRAPHY_DIR = os.path.join(BASE_DIR, "buildography", "builds")


def _get_tool(cmd_list):
    """Извлекает имя инструмента из списка команды."""
    if not cmd_list:
        return None
    return os.path.basename(str(cmd_list[0]))


def _extract_paths(field, limit=3):
    """Извлекает первые N путей из output или dependencies."""
    if isinstance(field, dict):
        return list(field.keys())[:limit]
    elif isinstance(field, list):
        result = []
        for item in field:
            if isinstance(item, dict):
                p = item.get('path', '')
                if p:
                    result.append(p)
            else:
                result.append(str(item))
            if len(result) >= limit:
                break
        return result
    return []


def sample_project(project_name, buildography_dir, filter_tools,
                   n_per_tool, full, tools_only):
    pattern = os.path.join(buildography_dir, project_name, "*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        print("  [WARN] No buildography files: {}".format(pattern))
        return

    # tool -> list of sample commands (ограничиваем до n_per_tool)
    tool_samples = {}   # tool -> [cmd, ...]
    tool_counts  = {}   # tool -> total count
    total_cmds   = 0
    no_cmd_count = 0

    for file_path in files:
        print("  Reading: {}".format(os.path.basename(file_path)))
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                data = json.load(f, strict=False)
        except Exception as e:
            print("  [ERROR] {}: {}".format(file_path, e))
            continue

        for cmd in data.get('component_commands', []):
            total_cmds += 1
            cmd_list = cmd.get('command', [])
            if not cmd_list:
                no_cmd_count += 1
                tool = '__no_command__'
            else:
                tool = _get_tool(cmd_list)

            tool_counts[tool] = tool_counts.get(tool, 0) + 1

            # Применяем фильтр если задан
            if filter_tools and tool not in filter_tools:
                continue

            # Сохраняем сэмпл если ещё не набрали n_per_tool
            samples = tool_samples.setdefault(tool, [])
            if len(samples) < n_per_tool:
                samples.append(cmd)

        del data

    print()
    print("  Total commands : {}".format(total_cmds))
    print("  No-command     : {}".format(no_cmd_count))
    print("  Unique tools   : {}".format(len(tool_counts)))
    print()

    if tools_only:
        # Только список инструментов с количеством
        print("  {:5s}  {}".format("COUNT", "TOOL"))
        print("  " + "-" * 50)
        for tool, count in sorted(tool_counts.items(),
                                  key=lambda x: -x[1]):
            print("  {:5d}  {}".format(count, tool))
        return

    # Показываем сэмплы
    if filter_tools:
        tools_to_show = [t for t in sorted(tool_samples.keys())
                         if t in filter_tools]
    else:
        tools_to_show = sorted(tool_samples.keys())

    for tool in tools_to_show:
        samples = tool_samples.get(tool, [])
        count   = tool_counts.get(tool, 0)
        print("  " + "=" * 56)
        print("  Tool: {}  (total occurrences: {})".format(tool, count))
        print("  " + "=" * 56)

        for i, cmd in enumerate(samples, 1):
            print()
            print("  --- Sample {}/{} ---".format(i, len(samples)))

            cmd_list = cmd.get('command', [])
            if cmd_list:
                print("  command[0]  : {}".format(cmd_list[0]))
                print("  basename    : {}".format(
                    os.path.basename(str(cmd_list[0]))))
                if len(cmd_list) > 1:
                    print("  full command: {}".format(
                        ' '.join(str(x) for x in cmd_list[:8])))
                    if len(cmd_list) > 8:
                        print("                ... ({} args total)".format(
                            len(cmd_list)))
            else:
                print("  command     : (empty)")

            if full:
                # Полная структура — все поля
                print("  fields      : {}".format(
                    ', '.join(k for k in cmd.keys() if k != 'command')))

                for field in ('output', 'dependencies'):
                    val = cmd.get(field)
                    if val is None:
                        continue
                    paths = _extract_paths(val, limit=5)
                    total = (len(val) if isinstance(val, (dict, list))
                             else 0)
                    print("  {} ({} items):".format(field, total))
                    for p in paths:
                        print("    {}".format(p))
                    if total > 5:
                        print("    ... and {} more".format(total - 5))

                # Остальные поля кроме command/output/dependencies
                for key, val in cmd.items():
                    if key in ('command', 'output', 'dependencies'):
                        continue
                    if isinstance(val, (str, int, float, bool)):
                        print("  {:12s}: {}".format(key, val))
                    elif isinstance(val, list):
                        print("  {:12s}: {} items".format(key, len(val)))
                    elif isinstance(val, dict):
                        print("  {:12s}: {} keys".format(key, len(val)))
            else:
                # Краткий режим — только выходы и входы
                out_paths = _extract_paths(cmd.get('output', {}), limit=3)
                dep_paths = _extract_paths(cmd.get('dependencies', {}),
                                           limit=3)
                out_total = len(cmd.get('output', {})) if isinstance(
                    cmd.get('output'), (dict, list)) else 0
                dep_total = len(cmd.get('dependencies', {})) if isinstance(
                    cmd.get('dependencies'), (dict, list)) else 0

                if out_paths:
                    print("  outputs ({}) :".format(out_total))
                    for p in out_paths:
                        print("    {}".format(p))
                    if out_total > 3:
                        print("    ... and {} more".format(out_total - 3))

                if dep_paths:
                    print("  deps    ({}) :".format(dep_total))
                    for p in dep_paths:
                        print("    {}".format(p))
                    if dep_total > 3:
                        print("    ... and {} more".format(dep_total - 3))

        print()


def main():
    parser = argparse.ArgumentParser(
        description='Просмотр примеров команд из buildography'
    )
    parser.add_argument('--single-project', metavar='NAME',
                        help='Обработать только один проект')
    parser.add_argument('--tools', metavar='LIST',
                        help='Фильтр по инструментам: gcc,ld,cp')
    parser.add_argument('--n', type=int, default=3, metavar='N',
                        help='Примеров на инструмент (default: 3)')
    parser.add_argument('--full', action='store_true',
                        help='Показывать полную структуру команды')
    parser.add_argument('--tools-only', action='store_true',
                        help='Только список инструментов без примеров')
    args = parser.parse_args()

    if not os.path.isdir(BUILDOGRAPHY_DIR):
        print("[ERROR] Buildography not found: {}".format(BUILDOGRAPHY_DIR))
        sys.exit(1)

    filter_tools = set()
    if args.tools:
        filter_tools = set(t.strip() for t in args.tools.split(',')
                           if t.strip())

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
            print("[ERROR] No projects found")
            sys.exit(1)

    mode = ('tools-only' if args.tools_only
            else 'full' if args.full else 'brief')
    print("Buildography : {}".format(BUILDOGRAPHY_DIR))
    print("Mode         : {}".format(mode))
    if filter_tools:
        print("Filter tools : {}".format(', '.join(sorted(filter_tools))))
    print("Samples/tool : {}".format(args.n))
    print("=" * 60)

    for project_name in projects:
        print("\n[{}]".format(project_name))
        sample_project(
            project_name, BUILDOGRAPHY_DIR,
            filter_tools, args.n, args.full, args.tools_only
        )

    print("=" * 60)


if __name__ == '__main__':
    main()
