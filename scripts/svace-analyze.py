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

    Каждый режим анализа кладёт результат в свою подпапку:

        (без флагов)        -> svace/PROJ/OB/   Python + JavaScript вместе
        --only-python       -> svace/PROJ/PY/   только Python
        --only-javascript   -> svace/PROJ/JS/   только JavaScript/TypeScript
        --separate          -> svace/PROJ/PY/, затем svace/PROJ/JS/
                               (для каждого проекта: сперва PY, потом JS)

    Языки определяются автоматически по наличию файлов в исходниках.
    Если в проекте нет файлов нужного языка — режим для проекта пропускается.

    Перед прогоном очищается только подпапка текущего режима
    (например, svace/PROJ/PY/), результаты других режимов не трогаются.

    Если на каком-то шаге происходит ошибка — она пишется в лог-файл
    проекта/режима, и скрипт переходит к следующему (прогон не прерывается).

ИСПОЛЬЗОВАНИЕ:
    python3 svace-analyze.py [OPTIONS]

ОПЦИИ:
    --project NAME        Анализировать только указанный проект.
                          Можно указать несколько раз: --project A --project B
                          (по умолчанию: все проекты, найденные в unpacked/)
    --only-python         Только Python          -> svace/PROJ/PY/
    --only-javascript,
    --only-js             Только JavaScript/TS   -> svace/PROJ/JS/
    --separate            Раздельно: сперва PY, затем JS для каждого проекта
    --svace-bin PATH      Путь/имя исполняемого файла svace
                          (по умолчанию: переменная окружения SVACE_BIN,
                          иначе системный "svace" из PATH)
    -h, --help            Показать справку

    --only-python, --only-javascript и --separate взаимоисключающие.

ПРИМЕРЫ:
    python3 svace-analyze.py
    python3 svace-analyze.py --project my-project
    python3 svace-analyze.py --project proj1 --project proj2 --separate
    python3 svace-analyze.py --only-python
    python3 svace-analyze.py --only-js --svace-bin /opt/svace/bin/svace

ОЖИДАЕМАЯ СТРУКТУРА (скрипт лежит в scripts/):
    analyze-tools/
    ├── scripts/
    │   └── svace-analyze.py
    ├── unpacked/
    │   └── PROJ/
    │       └── src/                  ← анализируемые исходники
    ├── svace/
    │   └── PROJ/
    │       ├── OB/                   ← Python + JS вместе (.svace-dir)
    │       ├── PY/                   ← только Python      (.svace-dir)
    │       └── JS/                   ← только JS/TS       (.svace-dir)
    └── logs/
        └── svace/
            └── PROJ/
                ├── PROJ-OB.log
                ├── PROJ-PY.log
                └── PROJ-JS.log

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
JS_EXTENSIONS     = ('.js', '.jsx', '.ts', '.tsx', '.mjs', '.cjs')

# Режимы анализа: имя подпапки -> языки, которые собираются в этом режиме
MODE_PY = "PY"
MODE_JS = "JS"
MODE_OB = "OB"

MODE_LANGUAGES = {
    MODE_PY: ['python'],
    MODE_JS: ['javascript'],
    MODE_OB: ['python', 'javascript'],
}

MODE_TITLES = {
    MODE_PY: "только Python",
    MODE_JS: "только JavaScript/TypeScript",
    MODE_OB: "Python + JavaScript/TypeScript",
}

# =============================================================================


# =============================================================================
# ЛОГИРОВАНИЕ (один файл на проект + режим, дописывается)
# =============================================================================
def log_path_for(project_name, mode):
    return os.path.join(LOG_DIR, project_name, "{}-{}.log".format(project_name, mode))


class ProjectLogger(object):
    def __init__(self, project_name, mode):
        self.project_name = project_name
        self.mode = mode
        self.path = log_path_for(project_name, mode)
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
        self._write("RUN START: {}  mode={}".format(
            datetime.now().isoformat(timespec='seconds'), self.mode))
        self._write("=" * 70)

    def end_run(self, status):
        self._write("-" * 70)
        self._write("RUN END: {}  mode={}  status={}".format(
            datetime.now().isoformat(timespec='seconds'), self.mode, status))
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
def run_cmd(cmd, cwd, log, step_name):
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
# ОПРЕДЕЛЕНИЕ ЯЗЫКОВ ПРОЕКТА (один проход по дереву)
# =============================================================================
def detect_available_languages(src_dir):
    """Возвращает множество языков, файлы которых есть в src_dir."""
    found = set()
    for _root, _dirs, files in os.walk(src_dir):
        for f in files:
            if 'python' not in found and f.endswith(PYTHON_EXTENSIONS):
                found.add('python')
            elif 'javascript' not in found and f.endswith(JS_EXTENSIONS):
                found.add('javascript')
        if len(found) == 2:
            break
    return found


def skip_reason(mode):
    if mode == MODE_PY:
        return "не найдено файлов Python ({})".format(', '.join(PYTHON_EXTENSIONS))
    if mode == MODE_JS:
        return "не найдено файлов JavaScript/TypeScript ({})".format(', '.join(JS_EXTENSIONS))
    return "не найдено файлов Python или JavaScript/TypeScript"


# =============================================================================
# ОБРАБОТКА ОДНОГО ПРОЕКТА В ОДНОМ РЕЖИМЕ: init -> build -> analyze
# =============================================================================
def process_project(project_name, mode, svace_bin, available_languages):
    log = ProjectLogger(project_name, mode)
    log.start_run()

    src_dir   = os.path.join(UNPACKED_DIR, project_name, "src")
    svace_dir = os.path.join(SVACE_WORK_DIR, project_name, mode)

    print("\n{}".format("=" * 60))
    print("\U0001F3AF Проект: {}  [{}: {}]".format(project_name, mode, MODE_TITLES[mode]))
    log.info("Источник           : {}".format(src_dir))
    log.info("Рабочая папка svace: {}".format(svace_dir))

    if not os.path.isdir(src_dir):
        log.error("Директория исходников не найдена: {}".format(src_dir))
        log.end_run("FAILED (нет src)")
        return "failed"

    languages = [lang for lang in MODE_LANGUAGES[mode] if lang in available_languages]
    if not languages:
        log.error("Пропуск: {}".format(skip_reason(mode)))
        log.end_run("SKIPPED")
        return "skipped"

    log.info("Языки для сборки: {}".format(', '.join(languages)))

    # Очищаем только подпапку текущего режима (PY / JS / OB)
    if os.path.isdir(svace_dir):
        log.info("Очистка рабочей директории: {}".format(svace_dir))
        try:
            shutil.rmtree(svace_dir)
        except Exception as e:
            log.error("Не удалось очистить {}: {}".format(svace_dir, e))
            log.end_run("FAILED (очистка рабочей директории)")
            return "failed"
    try:
        os.makedirs(svace_dir, exist_ok=True)
    except Exception as e:
        log.error("Не удалось создать {}: {}".format(svace_dir, e))
        log.end_run("FAILED (создание рабочей директории)")
        return "failed"

    # 1. svace init
    if not run_cmd([svace_bin, 'init', svace_dir], src_dir, log, "svace init"):
        log.end_run("FAILED (init)")
        return "failed"

    # 2. svace build
    build_cmd = [svace_bin, 'build', '--svace-dir', svace_dir]
    if 'python' in languages:
        build_cmd += ['--python', src_dir]
    if 'javascript' in languages:
        build_cmd += ['--javascript', src_dir]

    if not run_cmd(build_cmd, src_dir, log, "svace build"):
        log.end_run("FAILED (build)")
        return "failed"

    # 3. svace analyze
    analyze_cmd = [svace_bin, 'analyze', '--svace-dir', svace_dir]
    if not run_cmd(analyze_cmd, src_dir, log, "svace analyze"):
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


def select_modes(args):
    if args.only_python:
        return [MODE_PY]
    if args.only_javascript:
        return [MODE_JS]
    if args.separate:
        return [MODE_PY, MODE_JS]
    return [MODE_OB]


def main():
    parser = argparse.ArgumentParser(
        description='Последовательный анализ проектов через Svace (init + build + analyze)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--project', metavar='NAME', action='append', default=None,
                        help='Проект для анализа (можно указывать несколько раз). '
                             'По умолчанию — все проекты из unpacked/')

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--only-python', action='store_true',
                            help='Только Python -> svace/PROJ/PY/')
    mode_group.add_argument('--only-javascript', '--only-js', dest='only_javascript',
                            action='store_true',
                            help='Только JavaScript/TypeScript -> svace/PROJ/JS/')
    mode_group.add_argument('--separate', action='store_true',
                            help='Раздельно: сперва Python (PY), затем JavaScript (JS) '
                                 'для каждого проекта')

    parser.add_argument('--svace-bin', metavar='PATH', default=DEFAULT_SVACE_BIN,
                        help='Путь/имя исполняемого файла svace '
                             '(по умолчанию: $SVACE_BIN или системный "svace")')
    args = parser.parse_args()

    modes = select_modes(args)

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
    print("Режим: {}".format(
        " -> ".join("{} ({})".format(m, MODE_TITLES[m]) for m in modes)))

    start_time = datetime.now()
    results = []  # список (project, mode, status)

    for project_name in projects:
        src_dir = os.path.join(UNPACKED_DIR, project_name, "src")
        available = detect_available_languages(src_dir) if os.path.isdir(src_dir) else set()

        for mode in modes:
            status = process_project(project_name, mode, args.svace_bin, available)
            results.append((project_name, mode, status))

    elapsed = datetime.now() - start_time
    success = [(p, m) for p, m, s in results if s == "success"]
    failed  = [(p, m) for p, m, s in results if s == "failed"]
    skipped = [(p, m) for p, m, s in results if s == "skipped"]

    print("\n{}".format("=" * 60))
    print("Анализ завершён!")
    print("Проектов       : {}".format(len(projects)))
    print("Прогонов       : {}".format(len(results)))
    print("Успешно        : {}".format(len(success)))
    print("Ошибка         : {}".format(len(failed)))
    print("Пропущено      : {}".format(len(skipped)))
    print("Время          : {}".format(elapsed))
    print("Результаты     : {}".format(SVACE_WORK_DIR))
    print("Логи           : {}".format(LOG_DIR))

    if success:
        print("\nУспешные прогоны:")
        for p, m in success:
            print("  - {} [{}]  ->  {}".format(p, m, os.path.join(SVACE_WORK_DIR, p, m)))

    if failed:
        print("\nПрогоны с ошибкой:")
        for p, m in failed:
            print("  - {} [{}]  ->  {}".format(p, m, log_path_for(p, m)))

    if skipped:
        print("\nПропущенные прогоны (нет подходящих файлов):")
        for p, m in skipped:
            print("  - {} [{}]".format(p, m))

    sys.exit(0 if not failed else 1)


if __name__ == '__main__':
    main()
