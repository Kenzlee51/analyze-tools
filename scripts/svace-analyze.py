#!/usr/bin/env python3
"""
=============================================================================
svace-analyze.py — Анализ проектов через Svace
=============================================================================

ОПИСАНИЕ:
    Последовательно (один за другим) прогоняет полный локальный анализ
    Svace для проектов из unpacked/PROJ/src:

        svace init  <svace_dir>
        svace build --svace-dir <svace_dir> [--python SRC] [--javascript SRC]
        svace analyze --svace-dir <svace_dir>

    Языки Python и JavaScript определяются автоматически по наличию файлов
    в исходниках проекта. По умолчанию анализируются оба языка сразу (если
    файлы найдены) и все проекты, найденные в unpacked/.

    Если на каком-то шаге для проекта происходит ошибка — она пишется в
    лог-файл этого проекта, и скрипт переходит к следующему проекту
    (весь прогон не прерывается).

ИСПОЛЬЗОВАНИЕ:
    python3 svace-analyze.py [OPTIONS]

ОПЦИИ:
    --project NAME       Анализировать только указанный проект.
                          Можно указать несколько раз: --project A --project B
                          (по умолчанию: все проекты, найденные в unpacked/)
    --only-python         Собирать/анализировать только Python
                          (даже если в проекте есть JS/TS файлы)
    --only-javascript      Собирать/анализировать только JavaScript/TypeScript
                          (даже если в проекте есть Python файлы)
    --svace-bin PATH      Путь/имя исполняемого файла svace
                          (по умолчанию: переменная окружения SVACE_BIN,
                          иначе системный "svace" из PATH)
    -h, --help            Показать справку

ПРИМЕРЫ:
    python3 svace-analyze.py
    python3 svace-analyze.py --project my-project
    python3 svace-analyze.py --project proj1 --project proj2
    python3 svace-analyze.py --only-python
    python3 svace-analyze.py --svace-bin /opt/svace/bin/svace

ОЖИДАЕМАЯ СТРУКТУРА (скрипт лежит в scripts/, запускается оттуда же):
    analyze-tools/
    ├── scripts/
    │   └── svace-analyze.py
    ├── unpacked/
    │   └── PROJ/
    │       └── src/                  ← анализируемые исходники
    ├── svace/
    │   └── PROJ/                     ← рабочая директория svace (.svace-dir)
    └── logs/
        └── svace/
            └── PROJ/                 ← лог проекта
                └── PROJ.log

ЗАВИСИМОСТИ:
    Python 3.6+, svace (init/build/analyze) доступен в PATH или через
    --svace-bin / переменную окружения SVACE_BIN
=============================================================================
"""

import os
import sys
import shutil
import argparse
import subprocess
from datetime import datetime

# =============================================================================
# НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
# =============================================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

UNPACKED_DIR   = os.path.join(BASE_DIR, "unpacked")
SVACE_WORK_DIR = os.path.join(BASE_DIR, "svace")
LOG_DIR        = os.path.join(BASE_DIR, "logs", "svace")

DEFAULT_SVACE_BIN = os.environ.get("SVACE_BIN", "svace")

# Расширения файлов для автоопределения языка проекта
PYTHON_EXTENSIONS = ('.py', '.pyw')
JS_EXTENSIONS      = ('.js', '.jsx', '.ts', '.tsx', '.mjs', '.cjs')

# =============================================================================


# =============================================================================
# ЛОГИРОВАНИЕ (один файл на проект, дописывается)
# =============================================================================
class ProjectLogger(object):
    def __init__(self, project_name):
        self.project_name = project_name
        # ИЗМЕНЕНИЕ: лог сохраняется в подпапке проекта
        self.path = os.path.join(LOG_DIR, project_name, "{}.log".format(project_name))
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

    def _write(self, text):
        try:
            with open(self.path, 'a', encoding='utf-8') as f:
                f.write(text + '\n')
        except Exception as e:
            print("[LOG ERROR] {}: {}".format(self.path, e))

    def start_run(self):
        self._write("")
        self._write("=" * 70)
        self._write("RUN START: {}".format(datetime.now().isoformat(timespec='seconds')))
        self._write("=" * 70)

    def end_run(self, status):
        self._write("-" * 70)
        self._write("RUN END: {}  status={}".format(
            datetime.now().isoformat(timespec='seconds'), status))
        self._write("=" * 70)

    def info(self, message):
        line = "[{}] [INFO] {}".format(datetime.now().strftime('%H:%M:%S'), message)
        print("    {}".format(message))
        self._write(line)

    def error(self, message):
        line = "[{}] [ERROR] {}".format(datetime.now().strftime('%H:%M:%S'), message)
        print("    \u274c {}".format(message))
        self._write(line)

    def raw(self, text):
        self._write(text)


# =============================================================================
# ЗАПУСК КОМАНДЫ SVACE
# =============================================================================
def run_cmd(svace_bin, cmd, cwd, log, step_name):
    log.info("Запуск: {}".format(' '.join(cmd)))
    log.raw("--- {} ---".format(step_name))
    log.raw("CMD: {}".format(' '.join(cmd)))
    log.raw("CWD: {}".format(cwd))

    try:
        result = subprocess.run(
            cmd, cwd=cwd,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        log.error("{}: бинарь не найден: {}".format(step_name, cmd[0]))
        log.raw("Проверьте --svace-bin / переменную окружения SVACE_BIN")
        return False
    except Exception as e:
        log.error("{}: непредвиденная ошибка запуска: {}".format(step_name, e))
        return False

    stdout = result.stdout.decode('utf-8', errors='replace')
    stderr = result.stderr.decode('utf-8', errors='replace')

    if stdout.strip():
        log.raw("--- stdout ---")
        log.raw(stdout.rstrip())
    if stderr.strip():
        log.raw("--- stderr ---")
        log.raw(stderr.rstrip())
    log.raw("--- код возврата: {} ---".format(result.returncode))

    if result.returncode == 0:
        log.info("{}: OK".format(step_name))
        return True
    else:
        log.error("{}: завершился с кодом {}".format(step_name, result.returncode))
        return False


def check_svace_available(svace_bin, log_print=print):
    try:
        r = subprocess.run([svace_bin, '--version'],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        version = r.stdout.decode('utf-8', errors='replace').strip()
        log_print("\u2705 svace доступен: {}".format(version or "версия неизвестна"))
        return True
    except FileNotFoundError:
        log_print("\u274c svace не найден: '{}'".format(svace_bin))
        log_print("   Укажите --svace-bin PATH или переменную окружения SVACE_BIN")
        return False


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЯЗЫКОВ ПРОЕКТА
# =============================================================================
def has_files_with_extensions(root_dir, extensions):
    # ИЗМЕНЕНИЕ: убрана фильтрация служебных каталогов
    for root, dirs, files in os.walk(root_dir):
        for f in files:
            if f.endswith(extensions):
                return True
    return False


def detect_languages(src_dir, only_python, only_javascript):
    languages = []
    if not only_javascript and has_files_with_extensions(src_dir, PYTHON_EXTENSIONS):
        languages.append('python')
    if not only_python and has_files_with_extensions(src_dir, JS_EXTENSIONS):
        languages.append('javascript')
    return languages


# =============================================================================
# ОБРАБОТКА ОДНОГО ПРОЕКТА: init -> build -> analyze
# =============================================================================
def process_project(project_name, svace_bin, only_python, only_javascript):
    log = ProjectLogger(project_name)
    log.start_run()

    src_dir   = os.path.join(UNPACKED_DIR, project_name, "src")
    svace_dir = os.path.join(SVACE_WORK_DIR, project_name)

    print("\n{}".format("=" * 60))
    print("\U0001F3AF Проект: {}".format(project_name))
    log.info("Источник        : {}".format(src_dir))
    log.info("Рабочая папка svace: {}".format(svace_dir))

    if not os.path.isdir(src_dir):
        log.error("Директория исходников не найдена: {}".format(src_dir))
        log.end_run("FAILED (нет src)")
        return "failed"

    languages = detect_languages(src_dir, only_python, only_javascript)
    if not languages:
        if only_python:
            reason = "не найдено файлов Python ({})".format(', '.join(PYTHON_EXTENSIONS))
        elif only_javascript:
            reason = "не найдено файлов JavaScript/TypeScript ({})".format(', '.join(JS_EXTENSIONS))
        else:
            reason = "не найдено файлов Python или JavaScript/TypeScript"
        log.error("Пропуск проекта: {}".format(reason))
        log.end_run("SKIPPED")
        return "skipped"

    log.info("Обнаруженные языки: {}".format(', '.join(languages)))

    # Очищаем рабочую директорию svace перед прогоном
    if os.path.isdir(svace_dir):
        log.info("Очистка рабочей директории: {}".format(svace_dir))
        try:
            shutil.rmtree(svace_dir)
        except Exception as e:
            log.error("Не удалось очистить {}: {}".format(svace_dir, e))
            log.end_run("FAILED (очистка рабочей директории)")
            return "failed"
    os.makedirs(svace_dir, exist_ok=True)

    # 1. svace init
    if not run_cmd(svace_bin, [svace_bin, 'init', svace_dir], src_dir, log, "svace init"):
        log.end_run("FAILED (init)")
        return "failed"

    # 2. svace build
    build_cmd = [svace_bin, 'build', '--svace-dir', svace_dir]
    if 'python' in languages:
        build_cmd += ['--python', src_dir]
    if 'javascript' in languages:
        build_cmd += ['--javascript', src_dir]

    if not run_cmd(svace_bin, build_cmd, src_dir, log, "svace build"):
        log.end_run("FAILED (build)")
        return "failed"

    # 3. svace analyze
    analyze_cmd = [svace_bin, 'analyze', '--svace-dir', svace_dir]
    if not run_cmd(svace_bin, analyze_cmd, src_dir, log, "svace analyze"):
        log.end_run("FAILED (analyze)")
        return "failed"

    log.end_run("SUCCESS")
    return "success"


# =============================================================================
# ОСНОВНОЙ ЦИКЛ
# =============================================================================
def discover_all_projects():
    if not os.path.isdir(UNPACKED_DIR):
        return []
    return sorted([
        p for p in os.listdir(UNPACKED_DIR)
        if os.path.isdir(os.path.join(UNPACKED_DIR, p, "src"))
    ])


def main():
    parser = argparse.ArgumentParser(
        description='Последовательный анализ проектов через Svace (init + build + analyze)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--project', metavar='NAME', action='append', default=None,
                         help='Проект для анализа (можно указывать несколько раз). '
                              'По умолчанию — все проекты из unpacked/')
    parser.add_argument('--only-python', action='store_true',
                         help='Анализировать только Python, даже если есть JS/TS')
    parser.add_argument('--only-javascript', action='store_true',
                         help='Анализировать только JavaScript/TypeScript, даже если есть Python')
    parser.add_argument('--svace-bin', metavar='PATH', default=DEFAULT_SVACE_BIN,
                         help='Путь/имя исполняемого файла svace '
                              '(по умолчанию: $SVACE_BIN или системный "svace")')
    args = parser.parse_args()

    if args.only_python and args.only_javascript:
        parser.error("--only-python и --only-javascript нельзя использовать одновременно")

    print("Svace analyze")
    print("BASE_DIR : {}".format(BASE_DIR))
    print("SVACE_BIN: {}".format(args.svace_bin))

    if not check_svace_available(args.svace_bin):
        sys.exit(1)

    # --- Список проектов ---
    if args.project:
        projects = args.project
        missing = [p for p in projects
                   if not os.path.isdir(os.path.join(UNPACKED_DIR, p, "src"))]
        if missing:
            print("\u274c Не найдены исходники для проектов: {}".format(', '.join(missing)))
            print("   Ожидался путь: {}".format(
                os.path.join(UNPACKED_DIR, "<PROJECT>", "src")))
            sys.exit(1)
    else:
        projects = discover_all_projects()
        if not projects:
            print("\u2139\ufe0f Не найдено проектов с src/ в {}".format(UNPACKED_DIR))
            sys.exit(1)

    print("Проектов к анализу: {}".format(len(projects)))
    print("Список: {}".format(', '.join(projects)))
    if args.only_python:
        print("Режим: только Python")
    elif args.only_javascript:
        print("Режим: только JavaScript/TypeScript")
    else:
        print("Режим: Python + JavaScript/TypeScript (авто)")

    start_time = datetime.now()
    results = {}

    for project_name in projects:
        results[project_name] = process_project(
            project_name, args.svace_bin, args.only_python, args.only_javascript
        )

    elapsed = datetime.now() - start_time
    success = [p for p, s in results.items() if s == "success"]
    failed  = [p for p, s in results.items() if s == "failed"]
    skipped = [p for p, s in results.items() if s == "skipped"]

    print("\n{}".format("=" * 60))
    print("Анализ завершён!")
    print("Всего проектов : {}".format(len(projects)))
    print("Успешно        : {}".format(len(success)))
    print("Ошибка         : {}".format(len(failed)))
    print("Пропущено      : {}".format(len(skipped)))
    print("Время          : {}".format(elapsed))
    print("Логи           : {}".format(LOG_DIR))

    if failed:
        print("\nПроекты с ошибкой (см. лог каждого проекта в {}):".format(LOG_DIR))
        for p in failed:
            print("  - {}  ->  {}".format(p, os.path.join(LOG_DIR, p, "{}.log".format(p))))

    if skipped:
        print("\nПропущенные проекты (нет подходящих файлов):")
        for p in skipped:
            print("  - {}".format(p))

    sys.exit(0 if not failed else 1)


if __name__ == '__main__':
    main()