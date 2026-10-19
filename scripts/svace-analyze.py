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
        svace remote --host H --port P --login L --password P --path ID \
              analyze --svace-dir <svace_dir> -n NAME

    По умолчанию этап анализа выполняется УДАЛЁННО на сервере Svace
    (параметры REMOTE_* в начале скрипта). С флагом --local анализ
    выполняется локально обычной командой:

        svace analyze --svace-dir <svace_dir>

    Этапы init и build всегда локальные.

    --path ID — каталог проекта на сервере анализа. Относительное значение
    сервер разворачивает в свой каталог проектов (например, ID 'PROJ' ->
    /home/user/svace-server/dirs/PROJ). Если --path не передать, svace берёт
    имя текущей папки (у нас это 'src') и анализ падает с кодом 2:

        Remote server error: Cannot connect to the project with given
        identifier [src]: No project at path .../dirs/src

    Шаблон идентификатора задаётся REMOTE_PATH_TEMPLATE и поддерживает
    подстановки {project} и {mode} (например, '{project}' или
    '{project}_{mode}').

    -n NAME — имя, с которым связываются файлы результатов (<NAME>.svres и
    прочие). По умолчанию svace берёт имя папки, содержащей .svace-dir, то
    есть 'OB'/'PY'/'JS'; скрипт передаёт имя изделия, чтобы результаты
    назывались как раньше.

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
    --local               Локальный анализ (svace analyze) вместо удалённого
    --remote-host HOST    Сервер удалённого анализа   ($SVACE_REMOTE_HOST)
    --remote-port PORT    Порт удалённого анализа     ($SVACE_REMOTE_PORT)
    --remote-login NAME   Логин удалённого анализа    ($SVACE_REMOTE_LOGIN)
    --remote-password PW  Пароль удалённого анализа   ($SVACE_REMOTE_PASSWORD)
    --remote-path ID      Каталог проекта на сервере анализа: готовый
                          идентификатор либо шаблон с {project} и {mode}
                          ($SVACE_REMOTE_PATH, по умолчанию "{project}")
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
    python3 svace-analyze.py --local
    python3 svace-analyze.py --remote-host 192.168.25.183 --remote-port 55135
    python3 svace-analyze.py --remote-path '{project}_{mode}'
    python3 svace-analyze.py --project RTPG.34369-01 --remote-path RTPG.34369-01

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
    Python 3.6+, svace (init/build/analyze/remote) доступен в PATH или через
    --svace-bin / переменную окружения SVACE_BIN.
    Для удалённого анализа нужен доступ к серверу Svace (REMOTE_*).
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

# --- Удалённый анализ (svace remote ... analyze) ---
# Используются по умолчанию; переопределяются флагами --remote-* .
# С флагом --local анализ выполняется локально и эти параметры не нужны.
REMOTE_HOST     = os.environ.get("SVACE_REMOTE_HOST",     "192.168.25.183")
REMOTE_PORT     = os.environ.get("SVACE_REMOTE_PORT",     "55135")
REMOTE_LOGIN    = os.environ.get("SVACE_REMOTE_LOGIN",    "main")
REMOTE_PASSWORD = os.environ.get("SVACE_REMOTE_PASSWORD", "12345678")

# Каталог проекта на сервере анализа (svace remote --path).
# Поддерживает подстановки {project} и {mode}; можно указать и абсолютный путь.
REMOTE_PATH_TEMPLATE = os.environ.get("SVACE_REMOTE_PATH", "{project}")

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
def mask_secrets(text, secrets):
    for value in secrets or ():
        if value:
            text = text.replace(value, '***')
    return text


def run_cmd(cmd, cwd, log, step_name, secrets=()):
    shown = mask_secrets(' '.join(cmd), secrets)
    log.info("Запуск: {}".format(shown))
    log.raw("--- {} ---".format(step_name))
    log.raw("CMD: {}".format(shown))
    log.raw("CWD: {}".format(cwd))

    try:
        result = subprocess.run(
            cmd, cwd=cwd,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        log.error("{}: бинарь не найден: {}".format(step_name, cmd[0]))
        log.raw("Проверьте --svace-bin / переменную окружения SVACE_BIN")
        return False, ""
    except Exception as e:
        log.error("{}: непредвиденная ошибка запуска: {}".format(step_name, e))
        return False, ""

    stdout = result.stdout.decode('utf-8', errors='replace')
    stderr = result.stderr.decode('utf-8', errors='replace')

    if stdout.strip():
        log.raw("--- stdout ---")
        log.raw(mask_secrets(stdout.rstrip(), secrets))
    if stderr.strip():
        log.raw("--- stderr ---")
        log.raw(mask_secrets(stderr.rstrip(), secrets))
    log.raw("--- код возврата: {} ---".format(result.returncode))

    output = stdout + stderr
    if result.returncode == 0:
        log.info("{}: OK".format(step_name))
        return True, output
    else:
        log.error("{}: завершился с кодом {}".format(step_name, result.returncode))
        return False, output


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
# КОМАНДА АНАЛИЗА (локальная или удалённая)
# =============================================================================
def remote_project_path(template, project_name, mode):
    """Идентификатор каталога проекта на сервере анализа (svace remote --path)."""
    try:
        return template.format(project=project_name, mode=mode)
    except (KeyError, IndexError) as e:
        raise ValueError("недопустимый шаблон --remote-path {!r}: {} "
                         "(допустимы только {{project}} и {{mode}})".format(template, e))


def build_analyze_cmd(svace_bin, svace_dir, remote, project_name, mode):
    """remote = None -> локальный анализ, иначе dict с параметрами сервера."""
    if remote is None:
        return [svace_bin, 'analyze', '--svace-dir', svace_dir]
    return [svace_bin, 'remote',
            '--host', str(remote['host']),
            '--port', str(remote['port']),
            '--login', str(remote['login']),
            '--password', str(remote['password']),
            '--path', remote_project_path(remote['path'], project_name, mode),
            'analyze', '--svace-dir', svace_dir,
            '-n', project_name]


# =============================================================================
# ОБРАБОТКА ОДНОГО ПРОЕКТА В ОДНОМ РЕЖИМЕ: init -> build -> analyze
# =============================================================================
def process_project(project_name, mode, svace_bin, available_languages, remote):
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
    ok, _ = run_cmd([svace_bin, 'init', svace_dir], src_dir, log, "svace init")
    if not ok:
        log.end_run("FAILED (init)")
        return "failed"

    # 2. svace build
    build_cmd = [svace_bin, 'build', '--svace-dir', svace_dir]
    if 'python' in languages:
        build_cmd += ['--python', src_dir]
    if 'javascript' in languages:
        build_cmd += ['--javascript', src_dir]

    ok, _ = run_cmd(build_cmd, src_dir, log, "svace build")
    if not ok:
        log.end_run("FAILED (build)")
        return "failed"

    # 3. svace analyze (локально или удалённо)
    step_name = "svace analyze" if remote is None else "svace remote analyze"
    secrets = () if remote is None else (remote['password'],)
    try:
        analyze_cmd = build_analyze_cmd(svace_bin, svace_dir, remote, project_name, mode)
    except ValueError as e:
        log.error("{}: {}".format(step_name, e))
        log.end_run("FAILED ({})".format(step_name))
        return "failed"
    if remote is not None:
        log.info("Каталог проекта на сервере анализа: {}".format(
            remote_project_path(remote['path'], project_name, mode)))
    ok, out = run_cmd(analyze_cmd, src_dir, log, step_name, secrets)
    if not ok:
        if 'No project at path' in out or 'given identifier' in out:
            hint = ("На сервере анализа нет каталога проекта. Создайте его на {} "
                    "(каталог проектов сервера, см. путь в сообщении выше) или укажите "
                    "существующий через --remote-path / SVACE_REMOTE_PATH."
                    .format(remote['host'] if remote else '?'))
            log.error(hint)
        log.end_run("FAILED ({})".format(step_name))
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

    parser.add_argument('--local', action='store_true',
                        help='Локальный анализ (svace analyze) вместо удалённого '
                             '(svace remote ... analyze)')
    parser.add_argument('--remote-host', metavar='HOST', default=REMOTE_HOST,
                        help='Сервер удалённого анализа (по умолчанию: {})'.format(REMOTE_HOST))
    parser.add_argument('--remote-port', metavar='PORT', default=REMOTE_PORT,
                        help='Порт удалённого анализа (по умолчанию: {})'.format(REMOTE_PORT))
    parser.add_argument('--remote-login', metavar='NAME', default=REMOTE_LOGIN,
                        help='Логин удалённого анализа (по умолчанию: {})'.format(REMOTE_LOGIN))
    parser.add_argument('--remote-password', metavar='PW', default=REMOTE_PASSWORD,
                        help='Пароль удалённого анализа')
    parser.add_argument('--remote-path', metavar='ID', default=REMOTE_PATH_TEMPLATE,
                        help='Каталог проекта на сервере анализа: идентификатор или '
                             'шаблон с {{project}} и {{mode}} (по умолчанию: {})'.format(
                                 REMOTE_PATH_TEMPLATE))

    parser.add_argument('--svace-bin', metavar='PATH', default=DEFAULT_SVACE_BIN,
                        help='Путь/имя исполняемого файла svace '
                             '(по умолчанию: $SVACE_BIN или системный "svace")')
    args = parser.parse_args()

    modes = select_modes(args)

    if args.local:
        remote = None
    else:
        remote = {'host': args.remote_host, 'port': args.remote_port,
                  'login': args.remote_login, 'password': args.remote_password,
                  'path': args.remote_path}
        missing = [name for name in ('host', 'port', 'login', 'password', 'path')
                   if not remote[name]]
        if missing:
            print("\u274c Не заданы параметры удалённого анализа: {}".format(', '.join(missing)))
            print("   Укажите --remote-{} или переменные окружения SVACE_REMOTE_*, "
                  "либо запустите с --local".format('/--remote-'.join(missing)))
            sys.exit(1)

    print("Svace analyze")
    print("BASE_DIR : {}".format(BASE_DIR))
    print("SVACE_BIN: {}".format(args.svace_bin))
    if remote is None:
        print("Анализ   : локальный (svace analyze)")
    else:
        print("Анализ   : удалённый {}:{} (login {}, path {})".format(
            remote['host'], remote['port'], remote['login'], remote['path']))

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
            status = process_project(project_name, mode, args.svace_bin, available, remote)
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
