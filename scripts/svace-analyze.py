#!/usr/bin/env python3
"""
=============================================================================
svace-analyze.py — Анализ проектов через Svace
=============================================================================

ОПИСАНИЕ:
    Анализирует проекты из unpacked/PROJ/src с помощью Svace.
    Для каждого проекта создаётся рабочая директория svace/PROJ.
    Языки Python и JavaScript передаются всегда.

ИСПОЛЬЗОВАНИЕ:
    python3 svace-analyze.py [OPTIONS]

ОПЦИИ:
    --single-project NAME   Анализировать только указанный проект
    --fast                  Режим "build+analyze" последовательно для каждого проекта
                            (по умолчанию: сначала все build, затем все analyze)
    -h, --help              Показать справку

ПРИМЕРЫ:
    python3 svace-analyze.py
    python3 svace-analyze.py --single-project PROJ1
    python3 svace-analyze.py --fast

ОЖИДАЕМАЯ СТРУКТУРА:
    BASE_DIR/
    ├── scripts/
    │   └── svace-analyze.py
    ├── unpacked/
    │   └── PROJ/
    │       └── src/              ← анализируемые исходники
    ├── svace/
    │   └── PROJ/                 ← рабочая директория svace (.svace-dir)
    ├── logs/
    │   └── svace-analyze/
    │       ├── run_YYYYMMDD_HHMMSS.log
    │       └── projects/
    │           └── PROJ.log
    └── results/
        └── svace/
            └── svace-projects.json   ← БД проектов

ЗАВИСИМОСТИ:
    Python 3.6+, svace
=============================================================================
"""

import os
import sys
import json
import shutil
import argparse
import subprocess
from datetime import datetime

# =============================================================================
# НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
# =============================================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SVACE_BIN  = os.environ.get("SVACE_BIN", "svace")
SVACER_BIN = os.environ.get("SVACER_BIN", "svacer")

# Таймаут на каждую команду svace (секунды). 0 = без таймаута.
SVACE_TIMEOUT = 0

UNPACKED_DIR   = os.path.join(BASE_DIR, "unpacked")
SVACE_WORK_DIR = os.path.join(BASE_DIR, "svace")          # рабочая папка для svace
RESULTS_DIR    = os.path.join(BASE_DIR, "results")
SVACE_DB_DIR   = os.path.join(RESULTS_DIR, "svace")
SVACE_PROJECTS_DB = os.path.join(SVACE_DB_DIR, "svace-projects.json")
LOG_DIR        = os.path.join(BASE_DIR, "logs", "svace-analyze")
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
    os.makedirs(SVACE_DB_DIR, exist_ok=True)
    try:
        with open(SVACE_PROJECTS_DB, 'w', encoding='utf-8') as f:
            json.dump(db, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print("[ERROR] Failed to save DB: {}".format(e))


def update_db(db, project_name, source_path, svace_dir,
              status, error_message=None):
    """Обновляет запись проекта в БД."""
    existing = db["projects"].get(project_name, {})
    snapshot_status      = existing.get("snapshot_status", "never")
    snapshot_uploaded_at = existing.get("snapshot_uploaded_at", "")
    snapshot_project_name = existing.get("snapshot_project_name", "")

    db["projects"][project_name] = {
        "project_name":         project_name,
        "source_path":          source_path,
        "svace_dir":            svace_dir,
        "languages":            ["python", "javascript"],  # всегда оба
        "status":               status,
        "analyzed_at":          datetime.now().isoformat(),
        "error_message":        error_message or "",
        "snapshot_status":       snapshot_status,
        "snapshot_uploaded_at":  snapshot_uploaded_at,
        "snapshot_project_name": snapshot_project_name,
    }
    save_db(db)


# =============================================================================
# ЗАПУСК КОМАНДЫ
# =============================================================================
def run_cmd(cmd, cwd, log, name, timeout=None):
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
        log.error("Set SVACE_BIN/SVACER_BIN variables to correct paths")
        return False, ""
    except Exception as e:
        log.error("{}: unexpected error: {}".format(name, e))
        return False, ""


# =============================================================================
# ПРОВЕРКА SVACE
# =============================================================================
def check_svace(log):
    log.info("Checking svace availability...")
    try:
        r = subprocess.run([SVACE_BIN, '--version'],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = r.stdout.decode('utf-8', errors='replace').strip()
        log.info("svace: {}".format(stdout[:80] if stdout else "ok"))
    except OSError:
        log.error("svace not found: {}".format(SVACE_BIN))
        return False
    return True


# =============================================================================
# ОБРАБОТКА ОДНОГО ПРОЕКТА (build + analyze)
# =============================================================================
def process_project_build_and_analyze(project_name, db, log, fast_mode=False):
    """
    Выполняет build и analyze для одного проекта.
    Если fast_mode=False, только build.
    Если fast_mode=True, build+analyze сразу.
    Возвращает (build_ok, analyze_ok)
    """
    source_path = os.path.join(UNPACKED_DIR, project_name, "src")
    svace_dir   = os.path.join(SVACE_WORK_DIR, project_name)

    log.info("Source path: {}".format(source_path))
    log.info("Svace dir  : {}".format(svace_dir))

    if not os.path.isdir(source_path):
        log.error("Source directory not found: {}".format(source_path))
        update_db(db, project_name, source_path, svace_dir,
                  "failed", "Source directory not found")
        return False, False

    # Очищаем рабочую директорию svace
    os.makedirs(svace_dir, exist_ok=True)
    if os.listdir(svace_dir):
        log.info("Cleaning svace dir: {}".format(svace_dir))
        try:
            shutil.rmtree(svace_dir)
            os.makedirs(svace_dir, exist_ok=True)
        except Exception as e:
            log.error("Failed to clean svace dir: {}".format(e))
            update_db(db, project_name, source_path, svace_dir,
                      "failed", str(e))
            return False, False

    timeout = SVACE_TIMEOUT if SVACE_TIMEOUT > 0 else None

    # --- svace init ---
    ok, _ = run_cmd(
        [SVACE_BIN, 'init', '--svace-dir', svace_dir],
        source_path, log, "svace init", timeout
    )
    if not ok:
        update_db(db, project_name, source_path, svace_dir,
                  "failed", "svace init failed")
        return False, False

    # --- svace build (всегда оба языка) ---
    build_cmd = [SVACE_BIN, 'build', '--svace-dir', svace_dir,
                 '--python', source_path,
                 '--javascript', source_path]
    ok, _ = run_cmd(build_cmd, source_path, log, "svace build", timeout)
    if not ok:
        update_db(db, project_name, source_path, svace_dir,
                  "failed", "svace build failed")
        return False, False

    if not fast_mode:
        # Только build, analyze будет позже
        update_db(db, project_name, source_path, svace_dir,
                  "build_done")
        return True, False

    # --- svace analyze (fast mode) ---
    ok, _ = run_cmd(
        [SVACE_BIN, 'analyze', '--svace-dir', svace_dir],
        source_path, log, "svace analyze", timeout
    )
    if ok:
        update_db(db, project_name, source_path, svace_dir, "success")
        return True, True
    else:
        update_db(db, project_name, source_path, svace_dir,
                  "failed", "svace analyze failed")
        return True, False


def process_project_analyze(project_name, db, log):
    """Запускает analyze для уже построенного проекта."""
    source_path = os.path.join(UNPACKED_DIR, project_name, "src")
    svace_dir   = os.path.join(SVACE_WORK_DIR, project_name)

    if not os.path.isdir(svace_dir):
        log.error("Svace dir not found (build probably skipped): {}".format(svace_dir))
        update_db(db, project_name, source_path, svace_dir,
                  "failed", "Svace dir missing before analyze")
        return False

    timeout = SVACE_TIMEOUT if SVACE_TIMEOUT > 0 else None
    ok, _ = run_cmd(
        [SVACE_BIN, 'analyze', '--svace-dir', svace_dir],
        source_path, log, "svace analyze", timeout
    )
    if ok:
        update_db(db, project_name, source_path, svace_dir, "success")
        return True
    else:
        update_db(db, project_name, source_path, svace_dir,
                  "failed", "svace analyze failed")
        return False


# =============================================================================
# ОСНОВНОЙ ЦИКЛ
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description='Svace analysis script')
    parser.add_argument('--single-project', metavar='NAME',
                        help='Analyze only one project')
    parser.add_argument('--fast', action='store_true',
                        help='Run build+analyze sequentially per project')
    args = parser.parse_args()

    # --- Инициализация директорий ---
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(os.path.join(LOG_DIR, "projects"), exist_ok=True)
    os.makedirs(SVACE_WORK_DIR, exist_ok=True)
    os.makedirs(SVACE_DB_DIR, exist_ok=True)

    run_log_path = os.path.join(
        LOG_DIR,
        "run_{}.log".format(datetime.now().strftime('%Y%m%d_%H%M%S'))
    )
    with open(run_log_path, 'w', encoding='utf-8') as f:
        f.write("Svace analyze run started: {}\n".format(datetime.now().isoformat()))
        f.write("BASE_DIR: {}\n".format(BASE_DIR))
        f.write("SVACE_BIN: {}\n".format(SVACE_BIN))
        f.write("SVACER_BIN: {}\n".format(SVACER_BIN))
        f.write("=" * 60 + "\n\n")

    log = Logger(run_log_path)
    log.info("BASE_DIR  : {}".format(BASE_DIR))
    log.info("SVACE_BIN : {}".format(SVACE_BIN))
    log.info("SVACER_BIN: {}".format(SVACER_BIN))

    if not check_svace(log):
        log.error("Svace not available. Aborting.")
        sys.exit(1)

    db = load_db()
    log.info("Loaded DB: {} projects".format(len(db.get("projects", {}))))

    # --- Список проектов ---
    if args.single_project:
        src_dir = os.path.join(UNPACKED_DIR, args.single_project, "src")
        if not os.path.isdir(src_dir):
            log.error("Project source not found: {}".format(src_dir))
            sys.exit(1)
        projects = [args.single_project]
    else:
        if not os.path.isdir(UNPACKED_DIR):
            log.error("Unpacked directory not found: {}".format(UNPACKED_DIR))
            sys.exit(1)
        projects = sorted([
            p for p in os.listdir(UNPACKED_DIR)
            if os.path.isdir(os.path.join(UNPACKED_DIR, p, "src"))
        ])
        if not projects:
            log.error("No projects with src/ found in: {}".format(UNPACKED_DIR))
            sys.exit(1)

    log.info("Projects to analyze: {}".format(len(projects)))
    log.info("Projects: {}".format(', '.join(projects)))
    if args.fast:
        log.info("Mode: --fast (build+analyze per project)")
    else:
        log.info("Mode: two-pass (all builds, then all analyzes)")

    start_time = datetime.now()
    results = {}

    if args.fast:
        # Режим --fast: каждый проект обрабатываем полностью сразу
        for proj in projects:
            proj_log_path = os.path.join(LOG_DIR, "projects", "{}.log".format(proj))
            log.set_project_log(proj_log_path)
            log.info("=" * 50)
            log.info("Processing project: {} (fast mode)".format(proj))
            log.info("=" * 50)

            build_ok, analyze_ok = process_project_build_and_analyze(proj, db, log, fast_mode=True)
            results[proj] = analyze_ok  # для статистики считаем успешным если analyze ok
            log.set_project_log(None)
    else:
        # Двухпроходный режим: сначала все build
        log.info("=== PHASE 1: svace build for all projects ===")
        build_results = {}  # project -> (build_ok, need_analyze)
        for proj in projects:
            proj_log_path = os.path.join(LOG_DIR, "projects", "{}.log".format(proj))
            log.set_project_log(proj_log_path)
            log.info("=" * 50)
            log.info("Building project: {}".format(proj))
            log.info("=" * 50)

            build_ok, _ = process_project_build_and_analyze(proj, db, log, fast_mode=False)
            build_results[proj] = build_ok
            log.set_project_log(None)

        # Затем все analyze для успешно построенных
        log.info("=== PHASE 2: svace analyze for all projects ===")
        for proj in projects:
            if not build_results.get(proj, False):
                log.info("Skipping analyze for {} (build failed)".format(proj))
                results[proj] = False
                continue

            proj_log_path = os.path.join(LOG_DIR, "projects", "{}.log".format(proj))
            log.set_project_log(proj_log_path)
            log.info("=" * 50)
            log.info("Analyzing project: {}".format(proj))
            log.info("=" * 50)

            analyze_ok = process_project_analyze(proj, db, log)
            results[proj] = analyze_ok
            log.set_project_log(None)

    elapsed = datetime.now() - start_time
    success_count = sum(1 for v in results.values() if v)
    fail_count    = len(results) - success_count

    log.info("")
    log.info("=" * 50)
    log.info("Analysis complete!")
    log.info("=" * 50)
    log.info("Total projects : {}".format(len(projects)))
    log.info("Successful     : {}".format(success_count))
    log.info("Failed         : {}".format(fail_count))
    log.info("Time elapsed   : {}".format(elapsed))
    log.info("DB             : {}".format(SVACE_PROJECTS_DB))
    log.info("Run log        : {}".format(run_log_path))

    if fail_count > 0:
        log.info("Failed projects:")
        for name, ok in results.items():
            if not ok:
                log.error("  - {}".format(name))

    sys.exit(0 if fail_count == 0 else 1)


if __name__ == '__main__':
    main()