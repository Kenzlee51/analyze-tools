#!/usr/bin/env python3
"""
=============================================================================
sq-analyze.py — Скрипт анализа проектов через SonarQube
=============================================================================

ОПИСАНИЕ:
    Анализирует проекты из unpacked/PROJ/src через SonarQube.
    Языки для анализа определяются автоматически из extensions_src.json.
    Ведёт БД проектов в BASE_DIR/SQ/sq-projects.json.

    Для обхода ограничения SonarQube на длину ключа (400 символов)
    создаёт временный симлинк с коротким путём перед запуском scanner
    и удаляет его после. Конфиг sonar-project.properties пишется
    с путём симлинка чтобы SonarQube использовал короткий путь.

    Если scanner падает из-за превышения длины ключа — автоматически
    перезапускается с ограничением глубины вложенности архивов.
    Достигнутая глубина сохраняется в БД в поле depth_exclusion.

ИСПОЛЬЗОВАНИЕ:
    python3 sq-analyze.py [OPTIONS]

ОПЦИИ:
    --single-project NAME   Обработать только один указанный проект
    --reuse-success         Пропустить проекты со статусом success в БД
    --clean-configs         Удалять sonar-project.properties после анализа
    -h, --help              Показать справку

ПРИМЕРЫ:
    python3 sq-analyze.py
    python3 sq-analyze.py --single-project PROJ1
    python3 sq-analyze.py --reuse-success
    python3 sq-analyze.py --single-project PROJ1 --clean-configs

ОЖИДАЕМАЯ СТРУКТУРА:
    BASE_DIR/
    ├── scripts/
    │   └── sq-analyze.py
    ├── unpacked/
    │   └── PROJ1/
    │       └── src/              ← анализируемые исходники
    ├── results/
    │   └── PROJ1/
    │       └── expr/
    │           └── extensions_src.json   ← языки проекта
    ├── logs/
    │   └── SQ-analyze/
    │       ├── run_YYYYMMDD_HHMMSS.log   ← общий лог запуска
    │       └── projects/
    │           └── PROJ1.log             ← лог анализа проекта
    └── SQ/
        ├── sq-projects.json              ← БД проектов
        └── configs/
            └── PROJ1/
                └── sonar-project.properties

ЗАВИСИМОСТИ:
    Python 3.6+, sonar-scanner, requests
=============================================================================
"""

import os
import sys
import json
import argparse
import subprocess
import requests
import shutil
import random
import string
from datetime import datetime

# =============================================================================
# НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
# =============================================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SONARQUBE_URL     = "http://192.168.25.85:9000"
SONARQUBE_TOKEN   = "squ_2ea978eff3573e01550db66c6959968b8cd498b4"
SONAR_SCANNER_BIN = "sonar-scanner"
SONAR_SCANNER_MEMORY = "-Xmx2048m -Xms512m"
SYMLINK_BASE_DIR  = "/tmp/sq"

UNPACKED_DIR   = os.path.join(BASE_DIR, "unpacked")
RESULTS_DIR    = os.path.join(BASE_DIR, "results")
LOG_DIR        = os.path.join(BASE_DIR, "logs", "SQ-analyze")
SQ_DIR         = os.path.join(BASE_DIR, "SQ")
SQ_CONFIGS_DIR = os.path.join(SQ_DIR, "configs")
SQ_PROJECTS_DB = os.path.join(SQ_DIR, "sq-projects.json")
# =============================================================================

# =============================================================================
# ОГРАНИЧЕНИЯ ГЛУБИНЫ ВЛОЖЕННОСТИ (применяются по очереди при ошибке длины ключа)
# None  — без ограничений (попытка по умолчанию)
# str   — паттерн exclusion который добавляется к стандартным исключениям
# =============================================================================
KEY_TOO_LONG_ERROR = "is longer than the maximum authorized (400)"

DEPTH_EXCLUSIONS = [
    None,                             # попытка 1: без ограничений
    "**/*_dir/**/*_dir/**/*_dir/**",  # попытка 2: не глубже 3 уровней архивов
    "**/*_dir/**/*_dir/**",           # попытка 3: не глубже 2 уровней архивов
    "**/*_dir/**",                    # попытка 4: не заходить в архивы вообще
]
# =============================================================================

EXTENSION_TO_LANGUAGE = {
    ".c":     "c",     ".h":     "c",
    ".cpp":   "cpp",   ".cc":    "cpp",   ".cxx":   "cpp",
    ".c++":   "cpp",   ".hpp":   "cpp",   ".hh":    "cpp",
    ".hxx":   "cpp",   ".h++":   "cpp",
    ".py":    "python", ".pyw":  "python",
    ".java":  "java",
    ".js":    "javascript", ".jsx": "javascript",
    ".mjs":   "javascript", ".cjs": "javascript",
    ".ts":    "typescript", ".tsx": "typescript",
    ".go":    "go",
    ".cs":    "csharp",
    ".php":   "php",   ".phtml": "php",
    ".kt":    "kotlin", ".kts":  "kotlin",
    ".rb":    "ruby",  ".rake":  "ruby",
    ".scala": "scala", ".sc":    "scala",
    ".swift": "swift",
}

LANGUAGE_EXCLUSION_PATTERNS = {
    "c":          ["**/*.c", "**/*.h"],
    "cpp":        ["**/*.cpp", "**/*.cc", "**/*.cxx", "**/*.c++",
                   "**/*.hpp", "**/*.hh", "**/*.hxx", "**/*.h++"],
    "python":     ["**/*.py", "**/*.pyw"],
    "java":       ["**/*.java"],
    "javascript": ["**/*.js", "**/*.jsx", "**/*.mjs", "**/*.cjs"],
    "typescript": ["**/*.ts", "**/*.tsx"],
    "go":         ["**/*.go"],
    "csharp":     ["**/*.cs"],
    "php":        ["**/*.php", "**/*.phtml"],
    "kotlin":     ["**/*.kt", "**/*.kts"],
    "ruby":       ["**/*.rb", "**/*.rake"],
    "scala":      ["**/*.scala", "**/*.sc"],
    "swift":      ["**/*.swift"],
}

LANGUAGES_ALWAYS_EXCLUDE = ["java", "cpp", "c", "python", "javascript", "go"]

STANDARD_EXCLUSIONS = [
    "**/.scannerwork/**", "**/.git/**", "**/.svn/**", "**/.idea/**",
    "**/node_modules/**", "**/*.min.js", "**/*.bundle.js",
    "**/dist/**", "**/build/**", "**/*.log",
    "**/*.class",  # скомпилированный Java байткод — не исходник, может давать key > 400
]


# =============================================================================
# ЛОГИРОВАНИЕ
# =============================================================================
class Logger(object):
    def __init__(self, run_log, project_log=None):
        self.run_log     = run_log
        self.project_log = project_log

    def _write(self, message, log_path):
        try:
            with open(log_path, 'a', encoding='utf-8') as f:
                f.write(message + '\n')
        except Exception as e:
            print("[LOG ERROR] {}: {}".format(log_path, e))

    def info(self, message):
        line = "[{}] [INFO] {}".format(datetime.now().strftime('%H:%M:%S'), message)
        print(line)
        self._write(line, self.run_log)
        if self.project_log:
            self._write(line, self.project_log)

    def warn(self, message):
        line = "[{}] [WARN] {}".format(datetime.now().strftime('%H:%M:%S'), message)
        print(line)
        self._write(line, self.run_log)
        if self.project_log:
            self._write(line, self.project_log)

    def error(self, message):
        line = "[{}] [ERROR] {}".format(datetime.now().strftime('%H:%M:%S'), message)
        print(line)
        self._write(line, self.run_log)
        if self.project_log:
            self._write(line, self.project_log)

    def raw(self, message):
        if self.project_log:
            self._write(message, self.project_log)

    def set_project_log(self, project_log):
        self.project_log = project_log


# =============================================================================
# БД ПРОЕКТОВ
# =============================================================================
def load_projects_db():
    if not os.path.exists(SQ_PROJECTS_DB):
        return {"projects": {}}
    try:
        with open(SQ_PROJECTS_DB, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print("[WARN] Failed to load {}: {}".format(SQ_PROJECTS_DB, e))
        return {"projects": {}}


def save_projects_db(db):
    db["updated_at"] = datetime.now().isoformat()
    os.makedirs(SQ_DIR, exist_ok=True)
    try:
        with open(SQ_PROJECTS_DB, 'w', encoding='utf-8') as f:
            json.dump(db, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print("[ERROR] Failed to save {}: {}".format(SQ_PROJECTS_DB, e))


def update_project_in_db(db, project_name, project_key, source_path,
                         languages_analyzed, languages_excluded,
                         status, depth_exclusion, error_message=None):
    existing = db["projects"].get(project_name, {})
    if status == "success":
        hotspots_status      = existing.get("hotspots_status", "never")
        hotspots_exported_at = existing.get("hotspots_exported_at", "")
    else:
        hotspots_status      = "never"
        hotspots_exported_at = ""

    db["projects"][project_name] = {
        "project_key":          project_key,
        "project_name":         project_name,
        "sonarqube_url":        SONARQUBE_URL,
        "source_path":          source_path,
        "languages_analyzed":   sorted(list(languages_analyzed)),
        "languages_excluded":   sorted(list(languages_excluded)),
        "status":               status,
        "analyzed_at":          datetime.now().isoformat(),
        "dashboard_url":        "{}/dashboard?id={}".format(SONARQUBE_URL, project_key),
        "depth_exclusion":      depth_exclusion,  # None или паттерн строкой
        "error_message":        error_message or "",
        "hotspots_status":      hotspots_status,
        "hotspots_exported_at": hotspots_exported_at,
    }


# =============================================================================
# SONARQUBE API
# =============================================================================
def get_session():
    session = requests.Session()
    session.auth = (SONARQUBE_TOKEN, '')
    return session


def check_sonarqube_connection(log):
    log.info("Checking SonarQube connection: {}".format(SONARQUBE_URL))
    try:
        response = get_session().get(
            "{}/api/server/version".format(SONARQUBE_URL), timeout=10)
        if response.status_code == 200:
            log.info("SonarQube available, version: {}".format(response.text.strip()))
            return True
        log.error("SonarQube returned HTTP {}: {}".format(
            response.status_code, response.text[:200]))
        return False
    except requests.exceptions.ConnectionError:
        log.error("Cannot connect to SonarQube at {}".format(SONARQUBE_URL))
        return False
    except requests.exceptions.Timeout:
        log.error("Connection timeout to SonarQube")
        return False
    except Exception as e:
        log.error("Unexpected error connecting to SonarQube: {}".format(e))
        return False


def project_exists_in_sq(project_key, log):
    try:
        response = get_session().get(
            "{}/api/projects/search".format(SONARQUBE_URL),
            params={"projects": project_key}, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return bool(data.get("components")) and any(
                c["key"] == project_key for c in data["components"])
        return False
    except Exception as e:
        log.warn("Error checking project existence: {}".format(e))
        return False


def delete_project_from_sq(project_key, log):
    log.info("Deleting project from SonarQube: {}".format(project_key))
    try:
        response = get_session().post(
            "{}/api/projects/delete".format(SONARQUBE_URL),
            data={"project": project_key}, timeout=10)
        if response.status_code == 204:
            log.info("Project deleted: {}".format(project_key))
            return True
        log.error("Failed to delete project {}: HTTP {} {}".format(
            project_key, response.status_code, response.text[:200]))
        return False
    except Exception as e:
        log.error("Error deleting project {}: {}".format(project_key, e))
        return False


def create_project_in_sq(project_key, project_name, log):
    log.info("Creating project in SonarQube: {} ({})".format(project_key, project_name))
    try:
        response = get_session().post(
            "{}/api/projects/create".format(SONARQUBE_URL),
            data={"project": project_key, "name": project_name, "visibility": "private"},
            timeout=10)
        if response.status_code == 200:
            log.info("Project created: {}".format(project_key))
            return True
        log.error("Failed to create project {}: HTTP {} {}".format(
            project_key, response.status_code, response.text[:200]))
        return False
    except Exception as e:
        log.error("Error creating project {}: {}".format(project_key, e))
        return False


def make_project_key(project_name):
    safe   = ''.join(c if c.isalnum() or c in '-_.:' else '_' for c in project_name)
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
    return "{}_{}".format(safe.strip('_')[:95], suffix)


# =============================================================================
# СИМЛИНК С КОРОТКИМ ПУТЁМ
# =============================================================================
def create_short_symlink(source_path, log):
    """Создаёт временный симлинк /tmp/sq/XXXX -> source_path."""
    try:
        os.makedirs(SYMLINK_BASE_DIR, exist_ok=True)
        for _ in range(100):
            name      = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
            link_path = os.path.join(SYMLINK_BASE_DIR, name)
            if not os.path.exists(link_path):
                break
        else:
            log.error("Cannot generate unique symlink name in {}".format(SYMLINK_BASE_DIR))
            return None
        os.symlink(source_path, link_path)
        log.info("Symlink created: {} -> {}".format(link_path, source_path))
        return link_path
    except Exception as e:
        log.error("Failed to create symlink: {}".format(e))
        return None


def remove_symlink(link_path, log):
    """Удаляет симлинк. Не трогает реальные файлы."""
    if link_path and os.path.islink(link_path):
        try:
            os.unlink(link_path)
            log.info("Symlink removed: {}".format(link_path))
        except Exception as e:
            log.warn("Failed to remove symlink {}: {}".format(link_path, e))


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЯЗЫКОВ
# =============================================================================
def get_languages_from_extensions(project_name, log):
    extensions_file = os.path.join(RESULTS_DIR, project_name, "expr", "extensions_src.json")

    if not os.path.exists(extensions_file):
        log.warn("extensions_src.json not found: {}".format(extensions_file))
        log.warn("Will analyze without language filtering")
        return set(), set()

    try:
        with open(extensions_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        log.error("Failed to load extensions_src.json: {}".format(e))
        return set(), set()

    extensions_in_project = set(data.get("extensions", {}).keys())
    languages_found = set()
    for ext in extensions_in_project:
        lang = EXTENSION_TO_LANGUAGE.get(ext.lower())
        if lang:
            languages_found.add(lang)

    all_known_languages  = set(LANGUAGE_EXCLUSION_PATTERNS.keys())
    always_exclude       = set(LANGUAGES_ALWAYS_EXCLUDE)
    languages_to_exclude = (all_known_languages - languages_found) | always_exclude
    languages_found      = languages_found - always_exclude

    log.info("Extensions found: {}".format(', '.join(sorted(extensions_in_project))))
    log.info("Languages always excluded: {}".format(', '.join(sorted(always_exclude))))
    log.info("Languages to analyze: {}".format(
        ', '.join(sorted(languages_found)) if languages_found else "all (no filter)"))
    log.info("Languages to exclude: {}".format(
        ', '.join(sorted(languages_to_exclude)) if languages_to_exclude else "none"))

    return languages_found, languages_to_exclude


# =============================================================================
# КОНФИГУРАЦИЯ SONAR-SCANNER
# =============================================================================
def build_sonar_properties(project_key, project_name, scan_path,
                           languages_to_exclude, depth_exclusion=None):
    """
    Формирует содержимое sonar-project.properties.
    scan_path      — путь симлинка (короткий).
    depth_exclusion — дополнительный паттерн для ограничения глубины или None.
    """
    exclusions = list(STANDARD_EXCLUSIONS)
    for lang in sorted(languages_to_exclude):
        exclusions.extend(LANGUAGE_EXCLUSION_PATTERNS.get(lang, []))
    if depth_exclusion:
        exclusions.append(depth_exclusion)

    exclusions_str = ",\\\n    ".join(exclusions)
    exclusions_str = exclusions_str.replace("{", "{{").replace("}", "}}")

    return """# Auto-generated by sq-analyze.py
# Project: {project_name}
# Generated: {generated_at}
# depth_exclusion: {depth_exclusion}

sonar.projectKey={project_key}
sonar.projectName={project_name}
sonar.sources=.
sonar.projectBaseDir={scan_path}
sonar.host.url={sonarqube_url}
sonar.token={token}
sonar.sourceEncoding=UTF-8
sonar.scm.disabled=true

# Exclusions
sonar.exclusions=\\
    {exclusions}
""".format(
        project_name=project_name,
        generated_at=datetime.now().isoformat(),
        depth_exclusion=depth_exclusion if depth_exclusion else "none",
        project_key=project_key,
        scan_path=scan_path,
        sonarqube_url=SONARQUBE_URL,
        token=SONARQUBE_TOKEN,
        exclusions=exclusions_str,
    )


def write_sonar_config(project_key, project_name, scan_path,
                       languages_to_exclude, depth_exclusion, log):
    config_dir  = os.path.join(SQ_CONFIGS_DIR, project_name)
    os.makedirs(config_dir, exist_ok=True)
    config_file = os.path.join(config_dir, "sonar-project.properties")

    props = build_sonar_properties(
        project_key, project_name, scan_path, languages_to_exclude, depth_exclusion)
    with open(config_file, 'w', encoding='utf-8') as f:
        f.write(props)

    log.info("Config written: scan_path={}, depth_exclusion={}".format(
        scan_path, depth_exclusion if depth_exclusion else "none"))
    return config_file


def clean_scannerwork(path, log):
    scannerwork = os.path.join(path, ".scannerwork")
    if os.path.isdir(scannerwork):
        try:
            shutil.rmtree(scannerwork)
            log.info("Removed .scannerwork: {}".format(scannerwork))
        except Exception as e:
            log.warn("Failed to remove .scannerwork: {}".format(e))


# =============================================================================
# ЗАПУСК ОДНОЙ ПОПЫТКИ SONAR-SCANNER
# =============================================================================
def _run_scanner_once(scan_path, config_file, log):
    """
    Запускает sonar-scanner один раз.
    Возвращает (success: bool, key_too_long: bool).
    """
    env = os.environ.copy()
    env["SONAR_SCANNER_OPTS"] = SONAR_SCANNER_MEMORY
    env["SONAR_USER_HOME"]    = os.path.join(SQ_DIR, ".sonar-cache")

    cmd = [SONAR_SCANNER_BIN, "-Dproject.settings={}".format(config_file)]
    log.info("Command: {}".format(' '.join(cmd)))

    try:
        result = subprocess.run(
            cmd,
            cwd=scan_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=1800,
            env=env,
        )

        stdout = result.stdout.decode('utf-8', errors='replace')
        stderr = result.stderr.decode('utf-8', errors='replace')

        if stdout:
            log.raw("--- sonar-scanner stdout ---")
            log.raw(stdout)
        if stderr:
            log.raw("--- sonar-scanner stderr ---")
            log.raw(stderr)
        log.raw("--- return code: {} ---".format(result.returncode))

        key_too_long = KEY_TOO_LONG_ERROR in stderr

        if result.returncode == 0 and "EXECUTION SUCCESS" in stdout:
            log.info("sonar-scanner: EXECUTION SUCCESS")
            return True, False
        elif key_too_long:
            log.warn("sonar-scanner: failed due to key length limit")
            return False, True
        elif "EXECUTION FAILURE" in stdout:
            log.error("sonar-scanner: EXECUTION FAILURE")
            for line in stdout.split('\n'):
                if 'ERROR' in line:
                    log.error("  {}".format(line.strip()))
            return False, False
        elif result.returncode == 0:
            log.info("sonar-scanner: finished with code 0")
            return True, False
        else:
            log.error("sonar-scanner: finished with code {}".format(result.returncode))
            return False, False

    except subprocess.TimeoutExpired:
        log.error("sonar-scanner timed out (30 min)")
        return False, False
    except OSError:
        log.error("sonar-scanner not found: {}".format(SONAR_SCANNER_BIN))
        return False, False
    except Exception as e:
        log.error("Unexpected error running sonar-scanner: {}".format(e))
        return False, False


# =============================================================================
# ЗАПУСК SONAR-SCANNER С АВТОПОДБОРОМ ГЛУБИНЫ
# =============================================================================
def diagnose_long_paths(scan_path, project_key, log):
    """
    Находит файлы у которых component key превысит 400 символов.
    Логирует их расширения — чтобы понять это исходники или бинарники.
    component key = project_key + ":" + relative_path
    """
    log.info("Diagnosing long paths (limit=400)...")
    prefix_len  = len(project_key) + 1  # +1 для ":"
    long_files  = {}  # ext -> count
    total_long  = 0
    examples    = []  # до 5 примеров

    for dirpath, dirnames, filenames in os.walk(scan_path):
        # Пропускаем .scannerwork
        dirnames[:] = [d for d in dirnames if d != ".scannerwork"]
        for fname in filenames:
            full_path = os.path.join(dirpath, fname)
            rel_path  = os.path.relpath(full_path, scan_path)
            key_len   = prefix_len + len(rel_path)
            if key_len > 400:
                total_long += 1
                ext = os.path.splitext(fname)[1].lower() or "no_extension"
                long_files[ext] = long_files.get(ext, 0) + 1
                if len(examples) < 5:
                    examples.append((key_len, rel_path))

    if total_long == 0:
        log.info("No long paths found — depth exclusion not needed")
        return

    log.warn("Found {} files with key > 400 chars:".format(total_long))
    # Сортируем расширения по количеству
    for ext, count in sorted(long_files.items(), key=lambda x: -x[1]):
        log.warn("  {:30s} {}".format(ext, count))
    log.warn("Examples:")
    for key_len, rel_path in sorted(examples, key=lambda x: -x[0]):
        log.warn("  [{}] ...{}".format(key_len, rel_path[-80:]))


def run_sonar_scanner(source_path, project_key, project_name,
                      languages_to_exclude, args, log):
    """
    Создаёт симлинк, затем перебирает DEPTH_EXCLUSIONS:
      - Попытка 1: без ограничений (depth_exclusion=None)
      - При ошибке длины ключа — повтор с увеличивающимся ограничением
    Возвращает (success: bool, depth_exclusion: None|str).
    """
    log.info("Running sonar-scanner...")
    log.info("Source path (real): {}".format(source_path))

    link_path = create_short_symlink(source_path, log)
    if link_path is None:
        log.warn("Symlink creation failed, using real path (may hit 400 char limit)")
        scan_path = source_path
    else:
        scan_path = link_path
        log.info("Source path (scan): {}".format(scan_path))

    # Диагностика длинных путей (выполняется один раз до первой попытки)
    diagnose_long_paths(scan_path, project_key, log)

    success        = False
    used_exclusion = None  # будет заполнено при успехе или финальной попытке

    try:
        for attempt, depth_exclusion in enumerate(DEPTH_EXCLUSIONS, start=1):
            if depth_exclusion is None:
                log.info("Attempt {}: no depth limit".format(attempt))
            else:
                log.info("Attempt {}: depth_exclusion = {}".format(attempt, depth_exclusion))

            clean_scannerwork(scan_path, log)
            clean_scannerwork(source_path, log)

            try:
                config_file = write_sonar_config(
                    project_key, project_name, scan_path,
                    languages_to_exclude, depth_exclusion, log)
            except Exception as e:
                log.error("Failed to write sonar config: {}".format(e))
                break

            ok, key_too_long = _run_scanner_once(scan_path, config_file, log)
            used_exclusion = depth_exclusion

            if ok:
                success = True
                break

            if key_too_long:
                if attempt < len(DEPTH_EXCLUSIONS):
                    log.warn("Key too long, will retry with stricter depth limit")
                    continue
                else:
                    log.error("Key too long even with maximum depth restriction, giving up")
                    break
            else:
                # Другая ошибка — не связана с длиной ключа, повтор не поможет
                break

    finally:
        clean_scannerwork(scan_path, log)
        clean_scannerwork(source_path, log)
        remove_symlink(link_path, log)
        if args.clean_configs:
            config_dir = os.path.join(SQ_CONFIGS_DIR, project_name)
            config_file = os.path.join(config_dir, "sonar-project.properties")
            if os.path.exists(config_file):
                try:
                    os.remove(config_file)
                    log.info("Config removed (--clean-configs): {}".format(config_file))
                except Exception as e:
                    log.warn("Failed to remove config: {}".format(e))

    if success:
        log.info("Analysis succeeded with depth_exclusion: {}".format(
            used_exclusion if used_exclusion else "none"))
    else:
        log.error("Analysis failed. Last depth_exclusion tried: {}".format(
            used_exclusion if used_exclusion else "none"))

    return success, used_exclusion


# =============================================================================
# ОБРАБОТКА ОДНОГО ПРОЕКТА
# =============================================================================
def process_project(project_name, db, args, log):
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
    if not os.path.isdir(source_path):
        log.error("Source directory not found: {}".format(source_path))
        log.set_project_log(None)
        return False

    project_key = make_project_key(project_name)
    log.info("Project key: {}".format(project_key))

    existing = db["projects"].get(project_name)
    if args.reuse_success and existing:
        if existing.get("status") == "success":
            log.info("Project has status=success, skipping (--reuse-success)")
            log.set_project_log(None)
            return True
        elif existing.get("status") == "failed":
            log.info("Project has status=failed, will recreate")
        else:
            log.info("Project has unknown status, will recreate")

    if project_exists_in_sq(project_key, log):
        if not delete_project_from_sq(project_key, log):
            log.error("Cannot delete existing project, aborting")
            update_project_in_db(db, project_name, project_key, source_path,
                                  set(), set(), "failed", None,
                                  "Cannot delete existing project")
            save_projects_db(db)
            log.set_project_log(None)
            return False

    if not create_project_in_sq(project_key, project_name, log):
        log.error("Cannot create project in SonarQube, aborting")
        update_project_in_db(db, project_name, project_key, source_path,
                              set(), set(), "failed", None,
                              "Cannot create project in SonarQube")
        save_projects_db(db)
        log.set_project_log(None)
        return False

    languages_found, languages_to_exclude = get_languages_from_extensions(project_name, log)

    success, depth_exclusion = run_sonar_scanner(
        source_path, project_key, project_name,
        languages_to_exclude, args, log
    )

    status    = "success" if success else "failed"
    error_msg = None if success else "sonar-scanner returned failure"

    update_project_in_db(db, project_name, project_key, source_path,
                         languages_found, languages_to_exclude,
                         status, depth_exclusion, error_msg)
    save_projects_db(db)

    if success:
        log.info("Project {} — DONE (success), depth_exclusion: {}".format(
            project_name, depth_exclusion if depth_exclusion else "none"))
        log.info("Dashboard: {}/dashboard?id={}".format(SONARQUBE_URL, project_key))
    else:
        log.error("Project {} — DONE (failed), depth_exclusion: {}".format(
            project_name, depth_exclusion if depth_exclusion else "none"))

    log.set_project_log(None)
    return success


# =============================================================================
# ОСНОВНОЙ ЦИКЛ
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description='SonarQube analysis script')
    parser.add_argument('--single-project', metavar='NAME', help='Analyze only one project')
    parser.add_argument('--reuse-success', action='store_true',
                        help='Skip projects with status=success in DB')
    parser.add_argument('--clean-configs', action='store_true',
                        help='Delete sonar-project.properties after analysis')
    args = parser.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(os.path.join(LOG_DIR, "projects"), exist_ok=True)
    os.makedirs(SQ_DIR, exist_ok=True)
    os.makedirs(SQ_CONFIGS_DIR, exist_ok=True)
    os.makedirs(SYMLINK_BASE_DIR, exist_ok=True)

    run_log_path = os.path.join(
        LOG_DIR, "run_{}.log".format(datetime.now().strftime('%Y%m%d_%H%M%S')))

    with open(run_log_path, 'w', encoding='utf-8') as f:
        f.write("SQ Analyze run started: {}\n".format(datetime.now().isoformat()))
        f.write("BASE_DIR: {}\n".format(BASE_DIR))
        f.write("SONARQUBE_URL: {}\n".format(SONARQUBE_URL))
        f.write("SONAR_SCANNER_BIN: {}\n".format(SONAR_SCANNER_BIN))
        f.write("SONAR_SCANNER_MEMORY: {}\n".format(SONAR_SCANNER_MEMORY))
        f.write("SYMLINK_BASE_DIR: {}\n".format(SYMLINK_BASE_DIR))
        f.write("DEPTH_EXCLUSIONS: {}\n".format(DEPTH_EXCLUSIONS))
        f.write("=" * 60 + "\n\n")

    log = Logger(run_log_path)
    log.info("BASE_DIR: {}".format(BASE_DIR))
    log.info("SONARQUBE_URL: {}".format(SONARQUBE_URL))
    log.info("SONAR_SCANNER_MEMORY: {}".format(SONAR_SCANNER_MEMORY))
    log.info("SYMLINK_BASE_DIR: {}".format(SYMLINK_BASE_DIR))
    log.info("DEPTH_EXCLUSIONS: {}".format(DEPTH_EXCLUSIONS))

    if not check_sonarqube_connection(log):
        log.error("Cannot connect to SonarQube. Aborting.")
        sys.exit(1)

    db = load_projects_db()
    log.info("Loaded projects DB: {} projects".format(len(db.get("projects", {}))))

    if args.single_project:
        project_dir = os.path.join(UNPACKED_DIR, args.single_project)
        if not os.path.isdir(project_dir):
            log.error("Project not found: {}".format(project_dir))
            sys.exit(1)
        projects = [args.single_project]
    else:
        if not os.path.isdir(UNPACKED_DIR):
            log.error("Unpacked directory not found: {}".format(UNPACKED_DIR))
            sys.exit(1)
        projects = sorted([
            e for e in os.listdir(UNPACKED_DIR)
            if os.path.isdir(os.path.join(UNPACKED_DIR, e))
        ])
        if not projects:
            log.error("No projects found in: {}".format(UNPACKED_DIR))
            sys.exit(1)

    log.info("Projects to analyze: {}".format(len(projects)))
    log.info("Projects: {}".format(', '.join(projects)))
    if args.reuse_success:
        log.info("Mode: --reuse-success (skip successful projects)")
    if args.clean_configs:
        log.info("Mode: --clean-configs (delete configs after analysis)")

    start_time = datetime.now()
    results    = {}

    for project_name in projects:
        success = process_project(project_name, db, args, log)
        results[project_name] = success

    elapsed       = datetime.now() - start_time
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
    log.info("Projects DB    : {}".format(SQ_PROJECTS_DB))
    log.info("Run log        : {}".format(run_log_path))

    # Итоговая таблица глубин
    log.info("")
    log.info("Depth exclusions summary:")
    for name in projects:
        entry = db.get("projects", {}).get(name, {})
        dep   = entry.get("depth_exclusion", "n/a")
        stat  = entry.get("status", "n/a")
        log.info("  {:40s} status={:8s} depth_exclusion={}".format(
            name, stat, dep if dep else "none"))

    if fail_count > 0:
        log.info("")
        log.info("Failed projects:")
        for name, ok in results.items():
            if not ok:
                log.error("  - {}".format(name))

    sys.exit(0 if fail_count == 0 else 1)


if __name__ == '__main__':
    main()
