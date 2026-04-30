#!/usr/bin/env python3
"""
=============================================================================
svace-analyze.py — Скрипт анализа проектов через Svace
=============================================================================

ОПИСАНИЕ:
    Анализирует исходники проектов из unpacked/PROJ/src через Svace.
    Языки определяются автоматически (Python, JavaScript/TypeScript).
    Ведёт БД проектов в BASE_DIR/results/svace/svace-projects.json.
    Результаты анализа сохраняются в BASE_DIR/results/svace/PROJ/.

ИСПОЛЬЗОВАНИЕ:
    python3 svace-analyze.py [OPTIONS]

ОПЦИИ:
    --single-project NAME   Обработать только один указанный проект
    --reanalyze             Повторно анализировать проекты со статусом success
    -h, --help              Показать справку

ПРИМЕРЫ:
    python3 svace-analyze.py
    python3 svace-analyze.py --single-project PROJ1
    python3 svace-analyze.py --reanalyze
    python3 svace-analyze.py --single-project PROJ1 --reanalyze

ОЖИДАЕМАЯ СТРУКТУРА:
    BASE_DIR/
    ├── scripts/
    │   └── svace-analyze.py
    ├── unpacked/
    │   └── PROJ1/
    │       └── src/              ← анализируемые исходники
    ├── logs/
    │   └── svace-analyze/
    │       ├── run_YYYYMMDD_HHMMSS.log
    │       └── projects/
    │           └── PROJ1.log
    └── results/
        └── svace/
            ├── svace-projects.json   ← БД проектов
            └── PROJ1/                ← рабочая директория svace (.svace-dir)

ЗАВИСИМОСТИ:
    Python 3.6+, svace, svacer
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
# Скрипт лежит в BASE_DIR/scripts/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Пути к бинарникам (если не в PATH — указать полный путь)
# Пример: "/opt/svace-3.4/bin/svace"
SVACE_BIN  = "svace"
SVACER_BIN = "svacer"

# Таймаут на каждую команду svace (секунды). 0 = без таймаута.
SVACE_TIMEOUT = 0

# Директории
UNPACKED_DIR   = os.path.join(BASE_DIR, "unpacked")
RESULTS_DIR    = os.path.join(BASE_DIR, "results")
SVACE_DIR      = os.path.join(RESULTS_DIR, "svace")
LOG_DIR        = os.path.join(BASE_DIR, "logs", "svace-analyze")
SVACE_PROJECTS_DB = os.path.join(SVACE_DIR, "svace-projects.json")
# =============================================================================

# Расширения файлов по языкам
LANGUAGE_EXTENSIONS = {
    "python":     [".py", ".pyw"],
    "javascript": [".js", ".ts", ".tsx", ".jsx", ".mjs", ".cjs"],
}


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
    os.makedirs(SVACE_DIR, exist_ok=True)
    try:
        with open(SVACE_PROJECTS_DB, 'w', encoding='utf-8') as f:
            json.dump(db, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print("[ERROR] Failed to save DB: {}".format(e))


def update_db(db, project_name, source_path, svace_dir,
              languages, status, error_message=None):
    """Обновляет запись проекта в БД."""
    existing = db["projects"].get(project_name, {})

    # Сохраняем данные о выгрузке снапшета если они уже были
    snapshot_status      = existing.get("snapshot_status", "never")
    snapshot_uploaded_at = existing.get("snapshot_uploaded_at", "")
    snapshot_project_name = existing.get("snapshot_project_name", "")

    db["projects"][project_name] = {
        "project_name":         project_name,
        "source_path":          source_path,
        "svace_dir":            svace_dir,
        "languages":            sorted(list(languages)),
        "status":               status,
        "analyzed_at":          datetime.now().isoformat(),
        "error_message":        error_message or "",
        # Поля для следующего скрипта выгрузки снапшета
        "snapshot_status":       snapshot_status,
        "snapshot_uploaded_at":  snapshot_uploaded_at,
        "snapshot_project_name": snapshot_project_name,
    }
    save_db(db)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЯЗЫКОВ
# =============================================================================
def detect_languages(source_path, log):
    """
    Обходит source_path и определяет языки по расширениям файлов.
    Возвращает set языков: {"python", "javascript"}.
    """
    found = set()
    ext_counts = {}

    for root, dirs, files in os.walk(source_path):
        # Пропускаем служебные директории
        dirs[:] = [d for d in dirs if d not in ('.svace-dir', '.svacer-dir', '__pycache__',
                                                  'node_modules', '.git')]
        for fname in files:
            ext = os.path.splitext(fname)[1].lower()
            ext_counts[ext] = ext_counts.get(ext, 0) + 1
            for lang, exts in LANGUAGE_EXTENSIONS.items():
                if ext in exts:
                    found.add(lang)

    for lang in sorted(found):
        exts = LANGUAGE_EXTENSIONS[lang]
        count = sum(ext_counts.get(e, 0) for e in exts)
        log.info("  Language detected: {} ({} files)".format(lang, count))

    if not found:
        log.warn("  No supported languages detected (Python/JavaScript)")

    return found


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
        log.error("Set SVACE_BIN/SVACER_BIN variables to correct paths")
        return False, ""
    except Exception as e:
        log.error("{}: unexpected error: {}".format(name, e))
        return False, ""


# =============================================================================
# ПРОВЕРКА SVACE
# =============================================================================
def check_svace(log):
    """Проверяет доступность svace и svacer."""
    log.info("Checking svace availability...")
    try:
        r = subprocess.run([SVACE_BIN, '--version'],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = r.stdout.decode('utf-8', errors='replace').strip()
        log.info("svace: {}".format(stdout[:80] if stdout else "ok"))
    except OSError:
        log.error("svace not found: {}".format(SVACE_BIN))
        return False

    try:
        r = subprocess.run([SVACER_BIN, '--version'],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = r.stdout.decode('utf-8', errors='replace').strip()
        log.info("svacer: {}".format(stdout[:80] if stdout else "ok"))
    except OSError:
        log.error("svacer not found: {}".format(SVACER_BIN))
        return False

    return True


# =============================================================================
# ОБРАБОТКА ОДНОГО ПРОЕКТА
# =============================================================================
def process_project(project_name, db, args, log):
    """
    Анализирует один проект через svace.
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

    source_path = os.path.join(UNPACKED_DIR, project_name, "src")
    svace_dir   = os.path.join(SVACE_DIR, project_name)

    # --- Проверяем исходники ---
    if not os.path.isdir(source_path):
        log.error("Source directory not found: {}".format(source_path))
        update_db(db, project_name, source_path, svace_dir,
                  set(), "failed", "Source directory not found")
        log.set_project_log(None)
        return False

    log.info("Source path: {}".format(source_path))
    log.info("Svace dir  : {}".format(svace_dir))

    # --- Проверяем БД ---
    existing = db["projects"].get(project_name)
    if existing and existing.get("status") == "success" and not args.reanalyze:
        log.info("Status=success in DB, skipping (use --reanalyze to force)")
        log.set_project_log(None)
        return True

    # --- Определяем языки ---
    languages = detect_languages(source_path, log)
    if not languages:
        log.error("No supported languages found, skipping project")
        update_db(db, project_name, source_path, svace_dir,
                  languages, "failed", "No supported languages")
        log.set_project_log(None)
        return False

    # --- Готовим svace директорию ---
    os.makedirs(svace_dir, exist_ok=True)

    # Очищаем содержимое svace_dir если уже есть результаты предыдущего анализа
    if os.listdir(svace_dir):
        log.info("Cleaning svace dir: {}".format(svace_dir))
        try:
            shutil.rmtree(svace_dir)
            os.makedirs(svace_dir, exist_ok=True)
        except Exception as e:
            log.error("Failed to clean svace dir: {}".format(e))
            update_db(db, project_name, source_path, svace_dir,
                      languages, "failed", str(e))
            log.set_project_log(None)
            return False

    timeout = SVACE_TIMEOUT if SVACE_TIMEOUT > 0 else None

    # --- svace init ---
    ok, _ = run_cmd(
        [SVACE_BIN, 'init', '--svace-dir', svace_dir],
        source_path, log, "svace init", timeout
    )
    if not ok:
        update_db(db, project_name, source_path, svace_dir,
                  languages, "failed", "svace init failed")
        log.set_project_log(None)
        return False

    # --- svace build ---
    # svace build --svace-dir SVACE_DIR [--python SRC] [--javascript SRC]
    build_cmd = [SVACE_BIN, 'build', '--svace-dir', svace_dir]
    if 'python' in languages:
        build_cmd.extend(['--python', source_path])
    if 'javascript' in languages:
        build_cmd.extend(['--javascript', source_path])

    ok, _ = run_cmd(build_cmd, source_path, log, "svace build", timeout)
    if not ok:
        update_db(db, project_name, source_path, svace_dir,
                  languages, "failed", "svace build failed")
        log.set_project_log(None)
        return False

    # --- svace analyze ---
    ok, _ = run_cmd(
        [SVACE_BIN, 'analyze', '--svace-dir', svace_dir],
        source_path, log, "svace analyze", timeout
    )
    if not ok:
        update_db(db, project_name, source_path, svace_dir,
                  languages, "failed", "svace analyze failed")
        log.set_project_log(None)
        return False

    # --- Успех ---
    update_db(db, project_name, source_path, svace_dir, languages, "success")
    log.info("Project {} — DONE (success)".format(project_name))
    log.info("Svace dir: {}".format(svace_dir))
    log.set_project_log(None)
    return True


# =============================================================================
# ОСНОВНОЙ ЦИКЛ
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description='Svace analysis script')
    parser.add_argument('--single-project', metavar='NAME',
                        help='Analyze only one project')
    parser.add_argument('--reanalyze', action='store_true',
                        help='Re-analyze projects with status=success in DB')
    args = parser.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(os.path.join(LOG_DIR, "projects"), exist_ok=True)
    os.makedirs(SVACE_DIR, exist_ok=True)

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
        src = os.path.join(UNPACKED_DIR, args.single_project, "src")
        if not os.path.isdir(src):
            log.error("Project source not found: {}".format(src))
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
    if args.reanalyze:
        log.info("Mode: --reanalyze (re-analyze successful projects)")

    start_time = datetime.now()
    results = {}

    for project_name in projects:
        success = process_project(project_name, db, args, log)
        results[project_name] = success

    elapsed = datetime.now() - start_time
    success_count = sum(1 for v in results.values() if v)
    fail_count    = len(results) - success_count
    skip_count    = sum(
        1 for name in projects
        if db["projects"].get(name, {}).get("status") == "success"
        and not results.get(name, False) is False
    )

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
        log.info("")
        log.info("Failed projects:")
        for name, ok in results.items():
            if not ok:
                log.error("  - {}".format(name))

    sys.exit(0 if fail_count == 0 else 1)


if __name__ == '__main__':
    main()
