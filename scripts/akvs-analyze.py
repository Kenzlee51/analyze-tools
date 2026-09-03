#!/usr/bin/env python3
"""
=============================================================================
akvs-analyze.py — Анализ проектов через АК-ВС (REST-client v3)
=============================================================================

ОПИСАНИЕ:
    Последовательно (сервер держит только один проект за раз) прогоняет
    полный конвейер АК-ВС для проектов из unpacked/PROJ/src:

        analyze static   -> скачивание data
            -> dyn.py     (генерация trace.log из результатов статики)
        analyze dynamic  -> скачивание html + data
            -> end.py     (причёсывание отчёта)
        project delete    (проект на сервере удаляется ВСЕГДА)

    Итоговый распакованный отчёт с правками end.py кладётся в
    results/PROJ/akvs/.

    Если проект падает на каком-то шаге — с сервера скачивается лог
    (project + лог упавшей стадии) в logs/akvs/PROJ/serverlog/, ошибка
    пишется в лог проекта, и скрипт переходит к следующему проекту
    (весь прогон не прерывается). Проект на сервере удаляется в любом
    случае — и при успехе, и при ошибке.

ПРЕДВАРИТЕЛЬНЫЙ ШАГ (готовит unpacked/):
    ./scripts/unpack.sh --clean --filter cpp

ИСПОЛЬЗОВАНИЕ:
    python3 scripts/akvs-analyze.py [OPTIONS]

ОПЦИИ:
    --project NAME        Анализировать только указанный проект.
                          Можно указать несколько раз: --project A --project B
                          (по умолчанию: все проекты из unpacked/)
    --server VALUE        Адрес сервера (со схемой или без).  [см. SERVER]
    --port INT            Порт сервера.                        [см. PORT]
    --login VALUE         Имя пользователя.                    [см. LOGIN]
    --password VALUE      Пароль.                              [см. PASS]
    --timeout INT         Таймаут запроса, сек.                [см. TIMEOUT]
    --level INT           Уровень контроля статики (-l).       [см. LEVEL]
    --java-bin PATH       Путь/имя java (по умолчанию $JAVA_BIN или "java")
    --jar PATH            Путь к akvs-rest-client.jar
                          (по умолчанию lib/akvs/akvs-rest-client.jar)
    --keep-raw            Не удалять сырые выгрузки и рабочие файлы проекта
                          (по умолчанию они чистятся после каждого проекта)
    -h, --help            Показать справку

ПРИМЕРЫ:
    python3 scripts/akvs-analyze.py
    python3 scripts/akvs-analyze.py --project my-project
    python3 scripts/akvs-analyze.py --project a --project b --keep-raw
    python3 scripts/akvs-analyze.py --server 10.0.0.5 --port 11000
    python3 scripts/akvs-analyze.py --login user --password secret

ОЖИДАЕМАЯ СТРУКТУРА (скрипт лежит в scripts/, запускается оттуда же):
    analyze-tools/
    ├── scripts/
    │   └── akvs-analyze.py
    ├── lib/
    │   └── akvs/                     ← бинари АК-ВС (НАСТРАИВАЕТСЯ, см. LIB)
    │       ├── akvs-rest-client.jar
    │       ├── dyn.py
    │       ├── end.py
    │       └── dictionary.txt
    ├── unpacked/
    │   └── PROJ/
    │       └── src/                  ← анализируемые исходники
    ├── akvs/
    │   └── PROJ/                     ← рабочая папка (in/ out/ dyn/), временная
    ├── results/
    │   └── PROJ/
    │       └── akvs/                 ← ИТОГОВЫЙ распакованный отчёт
    └── logs/
        └── akvs/
            └── PROJ/
                ├── PROJ.log          ← лог прогона
                └── serverlog/        ← логи с сервера при ошибке

ЗАВИСИМОСТИ:
    Python 3.5+ (совместимо с Astra Linux 1.6), java (для .jar рекомендуется
    Java 8). dyn.py / end.py / dictionary.txt берутся из lib/akvs/ как есть.
    Архивация trace.log выполняется штатным zipfile (7z не требуется).
=============================================================================
"""

import os
import sys
import shutil
import zipfile
import argparse
import subprocess
from datetime import datetime

# =============================================================================
# ГЛОБАЛЬНЫЕ НАСТРОЙКИ (меняются здесь; каждую можно переопределить флагом)
# =============================================================================
SERVER  = "192.168.25.173"   # адрес сервера АК-ВС (--server); схема необязательна
PORT    = 11000              # порт сервера            (--port)
LOGIN   = "admin"            # логин                   (--login)
PASS    = "admin"            # пароль                  (--password)
TIMEOUT = 300                # таймаут запроса, сек    (--timeout)
LEVEL   = 2                  # уровень контроля статики(--level, -l)

# Где лежат бинари АК-ВС (jar, dyn.py, end.py, dictionary.txt)
LIB_SUBDIR = os.path.join("lib", "akvs")

# Что скачивать на каждом этапе (-sd, может быть несколько значений)
STATIC_DOWNLOAD  = ["data"]           # для dyn.py достаточно data/*.js
DYNAMIC_DOWNLOAD = ["html", "data"]   # html — база отчёта, data — за metrics.json

# =============================================================================

BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UNPACKED_DIR = os.path.join(BASE_DIR, "unpacked")
RESULTS_DIR  = os.path.join(BASE_DIR, "results")
LOG_DIR      = os.path.join(BASE_DIR, "logs", "akvs")
WORK_ROOT    = os.path.join(BASE_DIR, "akvs")
LIB_DIR      = os.path.join(BASE_DIR, LIB_SUBDIR)

DEFAULT_JAR      = os.path.join(LIB_DIR, "akvs-rest-client.jar")
DYN_PY           = os.path.join(LIB_DIR, "dyn.py")
END_PY           = os.path.join(LIB_DIR, "end.py")
DICTIONARY       = os.path.join(LIB_DIR, "dictionary.txt")
DEFAULT_JAVA_BIN = os.environ.get("JAVA_BIN", "java")


# =============================================================================
# ЛОГИРОВАНИЕ (один файл на проект, дописывается)
# =============================================================================
class ProjectLogger(object):
    def __init__(self, project_name):
        self.project_name = project_name
        self.dir = os.path.join(LOG_DIR, project_name)
        self.path = os.path.join(self.dir, "{}.log".format(project_name))
        os.makedirs(self.dir, exist_ok=True)

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
        self._write("[{}] [INFO] {}".format(datetime.now().strftime('%H:%M:%S'), message))
        print("    {}".format(message))

    def error(self, message):
        self._write("[{}] [ERROR] {}".format(datetime.now().strftime('%H:%M:%S'), message))
        print("    [ERROR] {}".format(message))

    def raw(self, text):
        self._write(text)


class StageError(Exception):
    """Ошибка на конкретной стадии конвейера (для выбора лога с сервера)."""
    def __init__(self, stage, message):
        super(StageError, self).__init__(message)
        self.stage = stage
        self.message = message


# =============================================================================
# ЗАПУСК ВНЕШНИХ КОМАНД
# =============================================================================
def _mask(cmd):
    """Прячем пароль в отображаемой команде."""
    shown = list(cmd)
    for i, tok in enumerate(shown):
        if tok in ("-w", "--password") and i + 1 < len(shown):
            shown[i + 1] = "***"
    return ' '.join(shown)


def run_cmd(cmd, cwd, log, step_name):
    log.info("Запуск: {}".format(_mask(cmd)))
    log.raw("--- {} ---".format(step_name))
    log.raw("CMD: {}".format(_mask(cmd)))
    log.raw("CWD: {}".format(cwd))

    try:
        result = subprocess.run(
            cmd, cwd=cwd,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        log.error("{}: исполняемый файл не найден: {}".format(step_name, cmd[0]))
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
    log.error("{}: завершился с кодом {}".format(step_name, result.returncode))
    return False


def check_java_available(java_bin, jar_path):
    problems = []
    try:
        subprocess.run([java_bin, '-version'],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError:
        problems.append("java не найден: '{}' (укажите --java-bin или $JAVA_BIN)".format(java_bin))
    for path, name in ((jar_path, "jar"), (DYN_PY, "dyn.py"),
                       (END_PY, "end.py"), (DICTIONARY, "dictionary.txt")):
        if not os.path.isfile(path):
            problems.append("не найден {}: {}".format(name, path))
    return problems


# =============================================================================
# ПОИСК ФАЙЛОВ/ПАПОК В ВЫГРУЗКАХ (имена папок плавающие: PROJ-N-data, PROJ-report)
# =============================================================================
def find_reports_data(root):
    """Папка .../reports/data со статистикой (fo_rel.js, fo.js, ...)."""
    for dirpath, dirnames, filenames in os.walk(root):
        if (os.path.basename(dirpath) == "data"
                and os.path.basename(os.path.dirname(dirpath)) == "reports"):
            return dirpath
    return None


def find_report_root(root):
    """Корень html-отчёта — папка, содержащая index.html."""
    for dirpath, dirnames, filenames in os.walk(root):
        if "index.html" in filenames:
            return dirpath
    return None


def find_file(root, name):
    for dirpath, dirnames, filenames in os.walk(root):
        if name in filenames:
            return os.path.join(dirpath, name)
    return None


def fresh_copytree(src, dst):
    """copytree без dirs_exist_ok (совместимо со старым Python): чистим dst."""
    if os.path.exists(dst):
        shutil.rmtree(dst)
    parent = os.path.dirname(dst)
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)
    shutil.copytree(src, dst)


# =============================================================================
# КОМАНДЫ АК-ВС
# =============================================================================
def auth_args(cfg):
    return ['-s', cfg['server'], '-p', str(cfg['port']),
            '-u', cfg['login'], '-w', cfg['password'], '-t', str(cfg['timeout'])]


def akvs_delete_project(cfg, project, log):
    """Удаление проекта на сервере (тихо — не роняет прогон)."""
    cmd = [cfg['java'], '-jar', cfg['jar'], 'project', 'delete'] \
        + auth_args(cfg) + ['-n', project]
    run_cmd(cmd, BASE_DIR, log, "project delete")


def akvs_download_server_log(cfg, project, stage, log):
    """Скачивание логов с сервера при ошибке -> logs/akvs/PROJ/serverlog/."""
    types_by_stage = {
        'static': ['p', 's'],
        'dyn.py': ['p', 's'],
        'dynamic': ['p', 'd'],
        'end.py': ['p', 's', 'd'],
    }
    wt = types_by_stage.get(stage, ['p'])

    out_dir = os.path.join(LOG_DIR, project, "serverlog")
    if os.path.exists(out_dir):          # -o should not exist
        shutil.rmtree(out_dir)

    cmd = [cfg['java'], '-jar', cfg['jar'], 'download', 'log', 'last-by-type'] \
        + auth_args(cfg) + ['-n', project, '-o', out_dir]
    for t in wt:
        cmd += ['-wt', t]

    log.info("Скачиваю логи с сервера ({}) в {}".format(",".join(wt), out_dir))
    ok = run_cmd(cmd, BASE_DIR, log, "download log")
    if not ok:
        # запасной вариант — только project-лог
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
        cmd = [cfg['java'], '-jar', cfg['jar'], 'download', 'log', 'last-by-type'] \
            + auth_args(cfg) + ['-n', project, '-o', out_dir, '-wt', 'p']
        run_cmd(cmd, BASE_DIR, log, "download log (fallback: project)")


# =============================================================================
# ОБРАБОТКА ОДНОГО ПРОЕКТА
# =============================================================================
def process_project(project, cfg, keep_raw):
    log = ProjectLogger(project)
    log.start_run()

    src_dir = os.path.join(UNPACKED_DIR, project, "src")
    work    = os.path.join(WORK_ROOT, project)
    downloads = os.path.join(work, "downloads")
    static_out  = os.path.join(downloads, "static")
    dynamic_out = os.path.join(downloads, "dynamic")
    in_data = os.path.join(work, "in", "data")
    out_dir = os.path.join(work, "out")
    dyn_dir = os.path.join(work, "dyn")

    print("\n{}".format("=" * 60))
    print("Проект: {}".format(project))
    log.info("Источник      : {}".format(src_dir))
    log.info("Рабочая папка  : {}".format(work))
    log.info("Сервер         : {}:{}".format(cfg['server'], cfg['port']))

    if not os.path.isdir(src_dir):
        log.error("Директория исходников не найдена: {}".format(src_dir))
        log.end_run("SKIPPED (нет src)")
        return "skipped"

    # Готовим чистую рабочую директорию
    if os.path.isdir(work):
        shutil.rmtree(work)
    os.makedirs(work)
    os.makedirs(os.path.join(work, "in"))
    os.makedirs(out_dir)
    shutil.copy2(DICTIONARY, os.path.join(work, "dictionary.txt"))

    status = "success"
    stage = None
    try:
        # Страховка: убираем возможный висящий проект прошлого прогона
        akvs_delete_project(cfg, project, log)

        # ---------- 1. СТАТИКА ----------
        stage = "static"
        cmd = [cfg['java'], '-jar', cfg['jar'], 'analyze', 'static'] \
            + auth_args(cfg) + ['-n', project, '-l', str(cfg['level']),
                                '-i', src_dir, '-o', static_out]
        for d in STATIC_DOWNLOAD:
            cmd += ['-sd', d]
        if not run_cmd(cmd, BASE_DIR, log, "analyze static"):
            raise StageError(stage, "analyze static завершился с ошибкой")

        st_data = find_reports_data(static_out)
        if not st_data:
            raise StageError(stage, "не найдена reports/data в выгрузке статики")
        log.info("Статика data  : {}".format(st_data))

        # ---------- 2. dyn.py -> out/trace.log ----------
        stage = "dyn.py"
        fresh_copytree(st_data, in_data)
        if not run_cmd([sys.executable, DYN_PY], work, log, "dyn.py"):
            raise StageError(stage, "dyn.py завершился с ошибкой")
        trace_log = os.path.join(out_dir, "trace.log")
        if not os.path.isfile(trace_log) or os.path.getsize(trace_log) == 0:
            raise StageError(stage, "dyn.py не создал непустой out/trace.log")

        # trace.zip (штатным zipfile, без 7z)
        trace_zip = os.path.join(out_dir, "trace.zip")
        with zipfile.ZipFile(trace_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(trace_log, arcname="trace.log")

        # ---------- 3. ДИНАМИКА ----------
        stage = "dynamic"
        cmd = [cfg['java'], '-jar', cfg['jar'], 'analyze', 'dynamic'] \
            + auth_args(cfg) + ['-n', project, '-i', trace_zip, '-o', dynamic_out]
        for d in DYNAMIC_DOWNLOAD:
            cmd += ['-sd', d]
        if not run_cmd(cmd, BASE_DIR, log, "analyze dynamic"):
            raise StageError(stage, "analyze dynamic завершился с ошибкой")

        # ---------- 4. Сборка отчёта + end.py ----------
        stage = "end.py"
        report_root = find_report_root(dynamic_out)
        if not report_root:
            raise StageError(stage, "не найден html-отчёт (index.html) в выгрузке динамики")
        log.info("HTML-отчёт    : {}".format(report_root))
        fresh_copytree(report_root, dyn_dir)

        # В html-отчёте нет metrics.json — берём из data-выгрузки (нужен end.py)
        metrics_json = find_file(dynamic_out, "metrics.json")
        if metrics_json:
            shutil.copy2(metrics_json, os.path.join(dyn_dir, "data", "metrics.json"))
            log.info("metrics.json  : добавлен из data-выгрузки")
        else:
            log.error("metrics.json не найден в выгрузке — end.py может упасть")

        if not run_cmd([sys.executable, END_PY], work, log, "end.py"):
            raise StageError(stage, "end.py завершился с ошибкой")

        # ---------- 5. Итоговый отчёт -> results/PROJ/akvs ----------
        result_dir = os.path.join(RESULTS_DIR, project, "akvs")
        fresh_copytree(dyn_dir, result_dir)
        log.info("Готовый отчёт : {}".format(result_dir))

    except StageError as e:
        status = "failed"
        log.error("Стадия '{}': {}".format(e.stage, e.message))
        try:
            akvs_download_server_log(cfg, project, e.stage, log)
        except Exception as le:
            log.error("Не удалось скачать логи с сервера: {}".format(le))
    except Exception as e:
        status = "failed"
        log.error("Непредвиденная ошибка на стадии '{}': {}".format(stage, e))
        try:
            akvs_download_server_log(cfg, project, stage or "static", log)
        except Exception as le:
            log.error("Не удалось скачать логи с сервера: {}".format(le))
    finally:
        # Проект на сервере удаляем ВСЕГДА (сервер держит только один проект)
        akvs_delete_project(cfg, project, log)
        # Чистка сырых выгрузок / рабочих файлов, если не --keep-raw
        if not keep_raw:
            if os.path.isdir(work):
                shutil.rmtree(work)
        else:
            log.info("--keep-raw: рабочие файлы сохранены в {}".format(work))

    log.end_run(status.upper())
    return status


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
        description="Последовательный анализ проектов через АК-ВС "
                    "(static -> dyn.py -> dynamic -> end.py -> delete)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Подробности и структуру каталогов см. в шапке файла.",
    )
    parser.add_argument('--project', metavar='NAME', action='append', default=None,
                        help='Проект для анализа (можно несколько раз). '
                             'По умолчанию — все проекты из unpacked/')
    parser.add_argument('--server',   default=SERVER,  help='Адрес сервера (default: {})'.format(SERVER))
    parser.add_argument('--port',     type=int, default=PORT,    help='Порт (default: {})'.format(PORT))
    parser.add_argument('--login',    default=LOGIN,   help='Логин (default: {})'.format(LOGIN))
    parser.add_argument('--password', default=PASS,    help='Пароль (default: {})'.format(PASS))
    parser.add_argument('--timeout',  type=int, default=TIMEOUT, help='Таймаут, сек (default: {})'.format(TIMEOUT))
    parser.add_argument('--level',    type=int, default=LEVEL,   help='Уровень контроля статики (default: {})'.format(LEVEL))
    parser.add_argument('--java-bin', default=DEFAULT_JAVA_BIN,  help='Путь/имя java (default: $JAVA_BIN или "java")')
    parser.add_argument('--jar',      default=DEFAULT_JAR,       help='Путь к akvs-rest-client.jar')
    parser.add_argument('--keep-raw', action='store_true',       help='Не удалять сырые выгрузки/рабочие файлы')
    args = parser.parse_args()

    cfg = {
        'server': args.server, 'port': args.port,
        'login': args.login, 'password': args.password,
        'timeout': args.timeout, 'level': args.level,
        'java': args.java_bin, 'jar': args.jar,
    }

    print("AK-VS analyze")
    print("BASE_DIR : {}".format(BASE_DIR))
    print("Сервер   : {}:{}  (логин: {})".format(cfg['server'], cfg['port'], cfg['login']))
    print("JAR      : {}".format(cfg['jar']))

    problems = check_java_available(cfg['java'], cfg['jar'])
    if problems:
        print("\n[ОШИБКА] Не выполнены предусловия запуска:")
        for p in problems:
            print("  - {}".format(p))
        sys.exit(1)

    # --- Список проектов ---
    if args.project:
        projects = args.project
        missing = [p for p in projects
                   if not os.path.isdir(os.path.join(UNPACKED_DIR, p, "src"))]
        if missing:
            print("[ОШИБКА] Не найдены исходники для проектов: {}".format(', '.join(missing)))
            print("   Ожидался путь: {}".format(os.path.join(UNPACKED_DIR, "<PROJECT>", "src")))
            sys.exit(1)
    else:
        projects = discover_all_projects()
        if not projects:
            print("[ИНФО] Не найдено проектов с src/ в {}".format(UNPACKED_DIR))
            print("       Сначала выполните: ./scripts/unpack.sh --clean --filter cpp")
            sys.exit(1)

    print("Проектов к анализу: {}".format(len(projects)))
    print("Список: {}".format(', '.join(projects)))

    start_time = datetime.now()
    results = {}
    for project in projects:
        results[project] = process_project(project, cfg, args.keep_raw)

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
    print("Отчёты         : {}".format(RESULTS_DIR))
    print("Логи           : {}".format(LOG_DIR))

    if failed:
        print("\nПроекты с ошибкой (лог + serverlog в logs/akvs/<project>/):")
        for p in failed:
            print("  - {}".format(p))
    if skipped:
        print("\nПропущенные проекты (нет src/):")
        for p in skipped:
            print("  - {}".format(p))

    sys.exit(0 if not failed else 1)


if __name__ == '__main__':
    main()
