#!/usr/bin/env python3
"""
=============================================================================
svace-upload.py — Скрипт загрузки результатов Svace на сервер
=============================================================================

ОПИСАНИЕ:
    Читает БД проектов из results/svace/svace-projects.json,
    для каждого проекта со статусом success выполняет:
      1. svacer import  — импорт результатов анализа
      2. svacer upload  — загрузка снапшета на сервер

    Имя проекта на сервере формируется как: PROJ_py_js_v1
    Суффикс задаётся переменной SNAPSHOT_SUFFIX.

    По умолчанию пропускает проекты у которых snapshot_status = "uploaded".
    Флаг --reupload принудительно перезагружает все проекты.

ИСПОЛЬЗОВАНИЕ:
    python3 svace-upload.py [OPTIONS]

ОПЦИИ:
    --single-project NAME   Загрузить только один указанный проект
    --reupload              Принудительно перезагрузить все проекты
    -h, --help              Показать справку

ПРИМЕРЫ:
    python3 svace-upload.py
    python3 svace-upload.py --single-project PROJ1
    python3 svace-upload.py --reupload
    python3 svace-upload.py --single-project PROJ1 --reupload

ОЖИДАЕМАЯ СТРУКТУРА:
    BASE_DIR/
    ├── scripts/
    │   └── svace-upload.py
    ├── logs/
    │   └── svace-upload/
    │       ├── run_YYYYMMDD_HHMMSS.log
    │       └── projects/
    │           └── PROJ1.log
    └── results/
        └── svace/
            ├── svace-projects.json   ← БД проектов (от svace-analyze.py)
            └── PROJ1/                ← директория с результатами анализа

ЗАВИСИМОСТИ:
    Python 3.6+, svacer
=============================================================================
"""

import os
import sys
import json
import argparse
import subprocess
from datetime import datetime

# =============================================================================
# НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
# =============================================================================
# Скрипт лежит в BASE_DIR/scripts/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Путь к svacer (если не в PATH — указать полный путь)
# Пример: "/opt/svace-3.4/bin/svacer"
SVACER_BIN = "svacer"

# Адрес сервера Svacer
SVACER_HOST = "localhost"
SVACER_PORT = "8080"

# Учётные данные для сервера
SVACER_USER     = "admin"
SVACER_PASSWORD = "admin"

# Суффикс имени проекта на сервере
# Итоговое имя: PROJ + SNAPSHOT_SUFFIX  →  PROJ_py_js_v1
SNAPSHOT_SUFFIX = "_py_js_v1"

# Таймаут на каждую команду svacer (секунды). 0 = без таймаута.
SVACER_TIMEOUT = 0

# Директории
RESULTS_DIR       = os.path.join(BASE_DIR, "results")
SVACE_DIR         = os.path.join(RESULTS_DIR, "svace")
LOG_DIR           = os.path.join(BASE_DIR, "logs", "svace-upload")
SVACE_PROJECTS_DB = os.path.join(SVACE_DIR, "svace-projects.json")
# =============================================================================


# =============================================================================
# ЛОГИРОВАНИЕ
# =============================================================================
class Logger(object):
    def __init__(self, run_log, project_log=None):
        self.run_log = run_log
        self.project_log = project_log

    def _write(self, message, log_path):
        try:
            with open(log_path, 'a', encoding='utf-8') as f:
                f.write(message + '\n')
        except Exception as e:
            print("[LOG ERROR] {}: {}".format(log_path, e))

    def _log(self, level, message):
        line = "[{}] [{}] {}".format(datetime.now().strftime('%H:%M:%S'), level, message)
        print(line)
        self._write(line, self.run_log)
        if self.project_log:
            self._write(line, self.project_log)

    def info(self, message):  self._log("INFO",  message)
    def warn(self, message):  self._log("WARN",  message)
    def error(self, message): self._log("ERROR", message)

    def raw(self, message):
        if self.project_log:
            self._write(message, self.project_log)

    def set_project_log(self, path):
        self.project_log = path


# =============================================================================
# БД ПРОЕКТОВ
# =============================================================================
def load_db():
    if not os.path.exists(SVACE_PROJECTS_DB):
        return {"projects": {}}
    try:
        with open(SVACE_PROJECTS_DB, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print("[WARN] Failed to load DB: {}".format(e))
        return {"projects": {}}


def save_db(db):
    db["updated_at"] = datetime.now().isoformat()
    try:
        with open(SVACE_PROJECTS_DB, 'w', encoding='utf-8') as f:
            json.dump(db, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print("[ERROR] Failed to save DB: {}".format(e))


def update_snapshot_status(db, project_name, status, snapshot_project_name=""):
    """Обновляет snapshot_status в БД проекта."""
    if project_name not in db["projects"]:
        return
    db["projects"][project_name]["snapshot_status"] = status
    db["projects"][project_name]["snapshot_uploaded_at"] = (
        datetime.now().isoformat() if status == "uploaded" else ""
    )
    if snapshot_project_name:
        db["projects"][project_name]["snapshot_project_name"] = snapshot_project_name
    save_db(db)


# =============================================================================
# ЗАПУСК КОМАНДЫ
# =============================================================================
def run_cmd(cmd, cwd, log, name, timeout=None):
    """
    Запускает команду в указанной директории.
    Возвращает (success, stdout).
    """
    log.info("Running: {}".format(' '.join(cmd)))
    log.raw("--- {} ---".format(name))
    log.raw("CMD: {}".format(' '.join(cmd)))
    log.raw("CWD: {}".format(cwd))

    try:
        kwargs = dict(
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if timeout:
            kwargs["timeout"] = timeout

        result = subprocess.run(cmd, **kwargs)

        stdout = result.stdout.decode('utf-8', errors='replace')
        stderr = result.stderr.decode('utf-8', errors='replace')

        if stdout:
            log.raw("--- stdout ---")
            log.raw(stdout)
        if stderr:
            log.raw("--- stderr ---")
            log.raw(stderr)
        log.raw("--- return code: {} ---".format(result.returncode))

        if result.returncode == 0:
            log.info("{}: OK (code 0)".format(name))
            return True, stdout
        else:
            log.error("{}: FAILED (code {})".format(name, result.returncode))
            return False, stdout

    except subprocess.TimeoutExpired:
        log.error("{}: TIMEOUT ({}s)".format(name, timeout))
        return False, ""
    except OSError:
        log.error("{}: binary not found: {}".format(name, cmd[0]))
        log.error("Set SVACER_BIN variable to correct path")
        return False, ""
    except Exception as e:
        log.error("{}: unexpected error: {}".format(name, e))
        return False, ""


# =============================================================================
# ПРОВЕРКА SVACER
# =============================================================================
def check_svacer(log):
    """Проверяет доступность svacer."""
    log.info("Checking svacer availability...")
    try:
        r = subprocess.run([SVACER_BIN, '--version'],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = r.stdout.decode('utf-8', errors='replace').strip()
        log.info("svacer: {}".format(stdout[:80] if stdout else "ok"))
        return True
    except OSError:
        log.error("svacer not found: {}".format(SVACER_BIN))
        return False


# =============================================================================
# ФОРМИРОВАНИЕ ИМЕНИ ПРОЕКТА НА СЕРВЕРЕ
# =============================================================================
def make_snapshot_project_name(project_name):
    """
    Формирует имя проекта на сервере Svacer.
    Пример: DShKG.00001-01 → DShKG.00001-01_py_js_v1
    """
    return "{}{}".format(project_name, SNAPSHOT_SUFFIX)


# =============================================================================
# ОБРАБОТКА ОДНОГО ПРОЕКТА
# =============================================================================
def process_project(project_name, project_info, db, args, log):
    """
    Выполняет import и upload для одного проекта.
    Возвращает True если успешно.
    """
    # --- Лог проекта ---
    projects_log_dir = os.path.join(LOG_DIR, "projects")
    os.makedirs(projects_log_dir, exist_ok=True)
    project_log_path = os.path.join(projects_log_dir, "{}.log".format(project_name))

    with open(project_log_path, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write("Project: {}\n".format(project_name))
        f.write("Started: {}\n".format(datetime.now().isoformat()))
        f.write("=" * 60 + "\n\n")

    log.set_project_log(project_log_path)

    log.info("=" * 50)
    log.info("Processing project: {}".format(project_name))
    log.info("=" * 50)

    svace_dir           = project_info.get("svace_dir", os.path.join(SVACE_DIR, project_name))
    snapshot_name       = make_snapshot_project_name(project_name)
    snapshot_status     = project_info.get("snapshot_status", "never")

    log.info("Svace dir     : {}".format(svace_dir))
    log.info("Snapshot name : {}".format(snapshot_name))
    log.info("Snapshot status: {}".format(snapshot_status))

    # --- Пропуск уже загруженных ---
    if snapshot_status == "uploaded" and not args.reupload:
        uploaded_at = project_info.get("snapshot_uploaded_at", "")
        log.info("Already uploaded at {}. Skipping (use --reupload to force)".format(uploaded_at))
        log.set_project_log(None)
        return True

    # --- Проверяем svace_dir ---
    if not os.path.isdir(svace_dir):
        log.error("Svace dir not found: {}".format(svace_dir))
        log.error("Run svace-analyze.py first")
        update_snapshot_status(db, project_name, "failed")
        log.set_project_log(None)
        return False

    timeout = SVACER_TIMEOUT if SVACER_TIMEOUT > 0 else None

    # --- svacer import ---
    # svacer import --host HOST --port PORT --user USER --password PASS
    #               --project SNAPSHOT_NAME --svace-dir SVACE_DIR
    import_cmd = [
        SVACER_BIN, 'import',
        '--host',     SVACER_HOST,
        '--port',     SVACER_PORT,
        '--user',     SVACER_USER,
        '--password', SVACER_PASSWORD,
        '--project',  snapshot_name,
        '--svace-dir', svace_dir,
    ]

    ok, _ = run_cmd(import_cmd, svace_dir, log, "svacer import", timeout)
    if not ok:
        update_snapshot_status(db, project_name, "failed")
        log.error("Project {} — DONE (failed at import)".format(project_name))
        log.set_project_log(None)
        return False

    # --- svacer upload ---
    # svacer upload --host HOST --port PORT --user USER --password PASS
    #               --project SNAPSHOT_NAME --svace-dir SVACE_DIR
    upload_cmd = [
        SVACER_BIN, 'upload',
        '--host',     SVACER_HOST,
        '--port',     SVACER_PORT,
        '--user',     SVACER_USER,
        '--password', SVACER_PASSWORD,
        '--project',  snapshot_name,
        '--svace-dir', svace_dir,
    ]

    ok, _ = run_cmd(upload_cmd, svace_dir, log, "svacer upload", timeout)
    if not ok:
        update_snapshot_status(db, project_name, "failed")
        log.error("Project {} — DONE (failed at upload)".format(project_name))
        log.set_project_log(None)
        return False

    # --- Успех ---
    update_snapshot_status(db, project_name, "uploaded", snapshot_name)
    log.info("Project {} — DONE (success)".format(project_name))
    log.info("Snapshot project name: {}".format(snapshot_name))
    log.set_project_log(None)
    return True


# =============================================================================
# ОСНОВНОЙ ЦИКЛ
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description='Svacer upload script')
    parser.add_argument('--single-project', metavar='NAME',
                        help='Upload only one project')
    parser.add_argument('--reupload', action='store_true',
                        help='Force re-upload even if already uploaded')
    args = parser.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(os.path.join(LOG_DIR, "projects"), exist_ok=True)

    run_log_path = os.path.join(
        LOG_DIR,
        "run_{}.log".format(datetime.now().strftime('%Y%m%d_%H%M%S'))
    )

    with open(run_log_path, 'w', encoding='utf-8') as f:
        f.write("Svacer upload run started: {}\n".format(datetime.now().isoformat()))
        f.write("BASE_DIR: {}\n".format(BASE_DIR))
        f.write("SVACER_BIN: {}\n".format(SVACER_BIN))
        f.write("SVACER_HOST: {}:{}\n".format(SVACER_HOST, SVACER_PORT))
        f.write("SVACER_USER: {}\n".format(SVACER_USER))
        f.write("SNAPSHOT_SUFFIX: {}\n".format(SNAPSHOT_SUFFIX))
        f.write("=" * 60 + "\n\n")

    log = Logger(run_log_path)

    log.info("BASE_DIR        : {}".format(BASE_DIR))
    log.info("SVACER_BIN      : {}".format(SVACER_BIN))
    log.info("SVACER_HOST     : {}:{}".format(SVACER_HOST, SVACER_PORT))
    log.info("SVACER_USER     : {}".format(SVACER_USER))
    log.info("SNAPSHOT_SUFFIX : {}".format(SNAPSHOT_SUFFIX))

    if not check_svacer(log):
        log.error("svacer not available. Aborting.")
        sys.exit(1)

    # --- Загружаем БД ---
    if not os.path.exists(SVACE_PROJECTS_DB):
        log.error("Projects DB not found: {}".format(SVACE_PROJECTS_DB))
        log.error("Run svace-analyze.py first")
        sys.exit(1)

    db = load_db()
    all_projects = db.get("projects", {})
    log.info("Loaded DB: {} projects total".format(len(all_projects)))

    # --- Только успешно проанализированные ---
    success_projects = {
        name: info for name, info in all_projects.items()
        if info.get("status") == "success"
    }
    log.info("Projects with status=success: {}".format(len(success_projects)))

    if not success_projects:
        log.warn("No projects with status=success found in DB")
        log.warn("Run svace-analyze.py first")
        sys.exit(0)

    # --- Выбираем проекты ---
    if args.single_project:
        if args.single_project not in success_projects:
            if args.single_project not in all_projects:
                log.error("Project not found in DB: {}".format(args.single_project))
            else:
                log.error("Project '{}' has status='{}', not 'success'".format(
                    args.single_project,
                    all_projects[args.single_project].get("status", "unknown")
                ))
            sys.exit(1)
        projects_to_process = {args.single_project: success_projects[args.single_project]}
    else:
        projects_to_process = success_projects

    log.info("Projects to upload: {}".format(len(projects_to_process)))
    log.info("Projects: {}".format(', '.join(sorted(projects_to_process.keys()))))
    if args.reupload:
        log.info("Mode: --reupload (force re-upload all)")

    start_time = datetime.now()
    results = {}

    for project_name, project_info in sorted(projects_to_process.items()):
        success = process_project(project_name, project_info, db, args, log)
        results[project_name] = success

    elapsed       = datetime.now() - start_time
    success_count = sum(1 for v in results.values() if v)
    fail_count    = len(results) - success_count
    skip_count    = sum(
        1 for name, info in projects_to_process.items()
        if not args.reupload and info.get("snapshot_status") == "uploaded"
    )

    log.info("")
    log.info("=" * 50)
    log.info("Upload complete!")
    log.info("=" * 50)
    log.info("Total projects : {}".format(len(projects_to_process)))
    log.info("Successful     : {}".format(success_count))
    log.info("Skipped        : {}".format(skip_count))
    log.info("Failed         : {}".format(fail_count))
    log.info("Time elapsed   : {}".format(elapsed))
    log.info("DB             : {}".format(SVACE_PROJECTS_DB))
    log.info("Run log        : {}".format(run_log_path))

    if fail_count > 0:
        log.info("")
        log.info("Failed projects:")
        for name, ok in results.items():
            if not ok:
                log.error("  - {}".format(name))

    sys.exit(0 if fail_count == 0 else 1)


if __name__ == '__main__':
    main()
