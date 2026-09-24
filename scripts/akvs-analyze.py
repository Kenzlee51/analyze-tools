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
    │   ├── PROJ/
    │   │   └── akvs/                 ← ИТОГОВЫЙ распакованный отчёт
    │   └── summary/
    │       └── akvs/
    │           └── PROJ/             ← копия отчёта (чистая пачка: index.html, data/, static/)
    │                                    только для успешных проектов; папку summary/akvs
    │                                    можно целиком скопировать и раздать
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
import re
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

# Расширения исходников, которые анализирует АК-ВС 3 (C/C++, Java, C#, Go,
# JavaScript, PHP, Python, Perl). В src.zip пакуются ТОЛЬКО файлы с этими
# расширениями (без учёта регистра). Файлы других языков и файлы без
# расширения не упаковываются. Отключается флагом --no-lang-filter.
AKVS_SOURCE_EXTS = {
    # C/C++
    ".c", ".h", ".cpp", ".cc", ".cxx", ".hpp", ".hh", ".hxx", ".c++", ".h++",
    # Java
    ".java",
    # C#
    ".cs", ".csx",
    # Go
    ".go",
    # JavaScript
    ".js", ".mjs", ".cjs", ".jsx",
    # PHP
    ".php", ".php3", ".php4", ".php5", ".php7", ".phtml",
    # Python
    ".py", ".pyx", ".pxd", ".pyi",
    # Perl
    ".pl", ".pm", ".t", ".pod",
    # Прочие расширения из авторитетного списка загрузчика АК-ВС
    # (launch-лог: -e cs,c,cpp,...,inc,...,plugin). Без них C-проекты
    # с .inc-инклюдами и .plugin отсеивались целиком.
    ".inc", ".plugin",
}

# =============================================================================

BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UNPACKED_DIR = os.path.join(BASE_DIR, "unpacked")
RESULTS_DIR  = os.path.join(BASE_DIR, "results")
SUMMARY_DIR  = os.path.join(RESULTS_DIR, "summary", "akvs")  # чистая пачка готовых отчётов
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
    """Корень html-отчёта.

    Клиент распаковывает html-отчёт в папку с именем '<project>.report' ->
    '<project>-report'. Ищем именно её по суффиксу '-report', потому что в
    выгрузке данных (-sd data) лежат исходники проекта, где могут быть свои
    index.html (веб-проекты, node_modules и т.п.) — по index.html легко
    зацепить чужой каталог. Дополнительно убеждаемся, что внутри есть
    index.html (это действительно отчёт).
    """
    candidates = []
    for dirpath, dirnames, filenames in os.walk(root):
        for d in dirnames:
            if d.endswith("-report"):
                full = os.path.join(dirpath, d)
                if os.path.isfile(os.path.join(full, "index.html")):
                    candidates.append(full)
    if not candidates:
        return None
    # если вдруг несколько — берём самый короткий путь (ближе к корню выгрузки)
    return sorted(candidates, key=len)[0]


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


def zip_sources(src_dir, zip_path, log, lang_filter=True):
    """Пакует СОДЕРЖИМОЕ src_dir в zip_path (без верхней папки src).

    - Все симлинки пропускаются (и битые, и валидные): битые симлинки
      (шрифты/сертификаты из prebuild-дистрибутивов) ломают штатный
      архиватор клиента. Кладём только реальные файлы.
    - При lang_filter=True (по умолчанию) пакуются ТОЛЬКО файлы с
      расширениями языков АК-ВС (AKVS_SOURCE_EXTS), без учёта регистра.
      Файлы других языков и файлы без расширения не упаковываются —
      это резко уменьшает объём и снимает нагрузку с сервера.
      Отключается флагом --no-lang-filter (lang_filter=False).
    Возвращает (packed, skipped_links, skipped_ext).
    """
    packed = 0
    skipped_links = 0
    skipped_ext = 0
    if os.path.exists(zip_path):
        os.remove(zip_path)
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for dirpath, dirnames, filenames in os.walk(src_dir):
            # не заходим в каталоги-симлинки (и не тащим их)
            dirnames[:] = [d for d in dirnames
                           if not os.path.islink(os.path.join(dirpath, d))]
            for name in filenames:
                full = os.path.join(dirpath, name)
                if os.path.islink(full):
                    skipped_links += 1
                    continue
                if not os.path.isfile(full):   # только обычные файлы
                    continue
                if lang_filter:
                    ext = os.path.splitext(name)[1].lower()
                    # без расширения -> ext == "" -> не проходит фильтр
                    if ext not in AKVS_SOURCE_EXTS:
                        skipped_ext += 1
                        continue
                arcname = os.path.relpath(full, src_dir)   # без верхней папки src
                try:
                    zf.write(full, arcname)
                    packed += 1
                except (OSError, IOError) as e:
                    skipped_links += 1
                    log.error("пропущен файл при упаковке: {} ({})".format(arcname, e))
    log.info("Упаковано в zip: файлов={}, пропущено симлинков={}, "
             "пропущено по расширению={}".format(packed, skipped_links, skipped_ext))
    return packed, skipped_links, skipped_ext


# =============================================================================
# КОМАНДЫ АК-ВС
# =============================================================================
def auth_args(cfg):
    return ['-s', cfg['server'], '-p', str(cfg['port']),
            '-u', cfg['login'], '-w', cfg['password'], '-t', str(cfg['timeout'])]


def akvs_delete_project(cfg, project, log):
    """Удаление одного проекта по имени. Возвращает True при успехе."""
    cmd = [cfg['java'], '-jar', cfg['jar'], 'project', 'delete'] \
        + auth_args(cfg) + ['-n', project]
    return run_cmd(cmd, BASE_DIR, log, "project delete")


def akvs_list_projects(cfg, log):
    """Список имён проектов на сервере.

    Возвращает (names, ok):
      names — список имён (может быть пустым);
      ok    — True если сервер ответил корректно, False при ошибке связи
              (сервер недоступен/несовместим). Нужно, чтобы отличать
              'проектов нет' от 'сервер не ответил'.
    """
    cmd = [cfg['java'], '-jar', cfg['jar'], 'project', 'list'] + auth_args(cfg)
    try:
        result = subprocess.run(cmd, cwd=BASE_DIR,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        log.error("project list: ошибка запуска: {}".format(e))
        return [], False
    out = result.stdout.decode('utf-8', errors='replace')
    err = result.stderr.decode('utf-8', errors='replace')
    if result.returncode != 0:
        log.error("project list: код {} ({})".format(
            result.returncode, (err.strip() or out.strip())[:200]))
        return [], False
    # Формат вывода:
    #   [INFO] Projects:
    #   - name1
    #   - name2
    # либо '[INFO] Projects: empty' когда пусто.
    names = []
    for line in out.splitlines():
        s = line.strip()
        if s.startswith('- '):
            name = s[2:].strip()
            if name:
                names.append(name)
    return names, True


def akvs_purge_slot(cfg, log):
    """Освобождает слот: удаляет ВСЕ проекты на сервере (сервер однопроектный).

    Возвращает список проектов, которые удалить НЕ удалось (для предупреждений).
    """
    names, ok = akvs_list_projects(cfg, log)
    if not ok:
        log.error("СЕРВЕР НЕДОСТУПЕН при очистке слота — проекты могли остаться")
        return []
    if not names:
        return []
    log.info("На сервере найдены проекты: {} — удаляю все".format(', '.join(names)))
    not_deleted = []
    for name in names:
        if not akvs_delete_project(cfg, name, log):
            not_deleted.append(name)
    return not_deleted


def akvs_download_server_log(cfg, project, stage, log):
    """Скачивание логов с сервера -> logs/akvs/PROJ/serverlog/.

    Набор типов зависит от стадии; для успешного прогона (stage='done')
    качаем полный набор project+static+dynamic для контроля.
    """
    types_by_stage = {
        'static': ['p', 's'],
        'dyn.py': ['p', 's'],
        'dynamic': ['p', 'd'],
        'end.py': ['p', 's', 'd'],
        'done': ['p', 's', 'd'],   # успешный прогон — полный набор для контроля
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
def process_project(project, cfg, keep_raw, leftovers):
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

    # Пакуем исходники ДО обращений к серверу (упаковка сервер не трогает).
    #  - все симлинки пропускаем (обход падения клиентского архиватора на
    #    битых симлинках из prebuild-дистрибутивов);
    #  - по умолчанию пакуем только файлы языков АК-ВС (lang_filter).
    # Если после фильтра НЕ осталось файлов — проект без исходного кода
    # (конфиги/шрифты/deb-пакеты и т.п.): помечаем SKIPPED и НЕ создаём его
    # на сервере (иначе повиснет пустой проект и полезут ложные ошибки).
    src_zip = os.path.join(work, "src.zip")
    packed, _, _ = zip_sources(src_dir, src_zip, log,
                               lang_filter=cfg.get('lang_filter', True))
    if packed == 0:
        log.error("Нет файлов на языках АК-ВС после фильтра — в проекте нет "
                  "исходного кода. Пропускаю (сервер не трогаю).")
        if not keep_raw and os.path.isdir(work):
            shutil.rmtree(work)
        log.end_run("SKIPPED (нет исходного кода)")
        return "skipped"

    status = "success"
    stage = None
    fail_stage = None
    try:
        # Освобождаем слот: сервер держит один проект за раз, поэтому
        # удаляем ВСЁ, что осталось от прошлых прогонов/тестов.
        akvs_purge_slot(cfg, log)

        # ---------- 1. СТАТИКА (src.zip уже собран выше) ----------
        stage = "static"
        cmd = [cfg['java'], '-jar', cfg['jar'], 'analyze', 'static'] \
            + auth_args(cfg) + ['-n', project, '-l', str(cfg['level']),
                                '-i', src_zip, '-o', static_out]
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

        # У «тривиальных» проектов (нет неопределённых ФО) сервер не создаёт
        # папку data/fo_without_def, а end.py её ожидает. Создаём пустую:
        # end.py всё равно выводит 0 неопределённых ФО (numberUncertainFO=0),
        # так что результат идентичен обычному прогону.
        fo_wd = os.path.join(dyn_dir, "data", "fo_without_def")
        if not os.path.isdir(fo_wd):
            os.makedirs(fo_wd)
            log.info("data/fo_without_def отсутствует (нет неопределённых ФО) — "
                     "создана пустая папка для end.py")

        if not run_cmd([sys.executable, END_PY], work, log, "end.py"):
            raise StageError(stage, "end.py завершился с ошибкой")

        # ---------- 5. Итоговый отчёт -> results/PROJ/akvs + summary ----------
        # На успешном прогоне обе папки проекта перезатираются (fresh_copytree).
        result_dir = os.path.join(RESULTS_DIR, project, "akvs")
        fresh_copytree(dyn_dir, result_dir)
        log.info("Готовый отчёт : {}".format(result_dir))

        summary_dir = os.path.join(SUMMARY_DIR, project)
        fresh_copytree(dyn_dir, summary_dir)
        log.info("Копия в summary: {}".format(summary_dir))

    except StageError as e:
        status = "failed"
        fail_stage = e.stage
        log.error("Стадия '{}': {}".format(e.stage, e.message))
    except Exception as e:
        status = "failed"
        fail_stage = stage or "static"
        log.error("Непредвиденная ошибка на стадии '{}': {}".format(stage, e))
    finally:
        # Серверный лог тянем ВСЕГДА (и при успехе — для контроля), и строго
        # ДО удаления проекта (после delete логи уже не скачать). При ошибке —
        # набор под упавшую стадию, при успехе — полный (project+static+dynamic).
        try:
            akvs_download_server_log(
                cfg, project,
                (fail_stage or "static") if status == "failed" else "done",
                log)
        except Exception as le:
            log.error("Не удалось скачать логи с сервера: {}".format(le))

        # Проект на сервере удаляем ВСЕГДА (сервер держит только один проект).
        # ВАЖНО: лицензия сервера = 1 проект, поэтому незакрытый «хвост»
        # роняет сервер при следующем старте. Отслеживаем неудачи явно.
        deleted = akvs_delete_project(cfg, project, log)
        if not deleted:
            # Проверяем, действительно ли проект остался на сервере
            names, ok = akvs_list_projects(cfg, log)
            if not ok:
                log.error("НЕ УДАЛОСЬ УДАЛИТЬ проект и сервер недоступен — "
                          "проект '{}' МОГ остаться на сервере".format(project))
                if project not in leftovers:
                    leftovers.append(project)
            elif project in names:
                log.error("ПРОЕКТ '{}' ОСТАЛСЯ НА СЕРВЕРЕ — удалите вручную, "
                          "иначе сервер может упасть по лимиту лицензии".format(project))
                leftovers.append(project)

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
# --- Починка битых cp1251-имён ПАПОК проектов --------------------------------
_TRANSLIT = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'zh','з':'z',
    'и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r',
    'с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts','ч':'ch','ш':'sh',
    'щ':'sch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya',
    'А':'A','Б':'B','В':'V','Г':'G','Д':'D','Е':'E','Ё':'E','Ж':'ZH','З':'Z',
    'И':'I','Й':'Y','К':'K','Л':'L','М':'M','Н':'N','О':'O','П':'P','Р':'R',
    'С':'S','Т':'T','У':'U','Ф':'F','Х':'KH','Ц':'TS','Ч':'CH','Ш':'SH',
    'Щ':'SCH','Ъ':'','Ы':'Y','Ь':'','Э':'E','Ю':'YU','Я':'YA',
}
_ENCODINGS = ('cp1251', 'koi8-r', 'cp866', 'iso8859-5')


def _is_broken_name(name):
    try:
        name.encode('utf-8', 'strict')
        return False
    except UnicodeEncodeError:
        return True


def _decode_broken(name):
    if not _is_broken_name(name):
        return name
    nb = name.encode('utf-8', 'surrogateescape')
    for enc in _ENCODINGS:
        try:
            dec = nb.decode(enc)
            if dec.encode(enc) == nb:
                return dec
        except (UnicodeDecodeError, UnicodeEncodeError):
            continue
    return name


def _clean_latin(name):
    """Битое/кириллическое имя -> безопасный латинский идентификатор.

    Имя папки проекта служит и именем на сервере (-n), и частью путей,
    поэтому приводим к латинице (транслит) — как договаривались для верхних
    папок. Небитые латинские имена не трогаем.
    """
    decoded = _decode_broken(name)
    out = ''.join(_TRANSLIT.get(ch, ch) for ch in decoded)
    out = out.replace(' ', '_')
    out = re.sub(r'[^A-Za-z0-9._-]', '', out)
    out = re.sub(r'_+', '_', out).strip('_')
    return out


def _needs_latin(name):
    """True, если имя нужно привести к латинице: битое (суррогаты) ИЛИ
    содержит не-ASCII символы (кириллица). Сервер АК-ВС не принимает
    кириллические имена проектов (-n), а в путях они тоже создают проблемы.
    """
    if _is_broken_name(name):
        return True
    return any(ord(ch) > 127 for ch in name)


def repair_project_dir_names():
    """Приводит имена папок проектов в unpacked/ к латинице.

    Затрагивает битые (cp1251-суррогаты) И валидные кириллические имена —
    оба транслитерируются в латиницу (ИСАТ.01342 -> ISAT.01342). Чисто
    ASCII-имена не трогает. Возвращает список пар (было_repr, стало).
    """
    if not os.path.isdir(UNPACKED_DIR):
        return []
    renamed = []
    for raw in list(os.listdir(UNPACKED_DIR)):
        if not _needs_latin(raw):
            continue
        clean = _clean_latin(raw) or 'project'
        final = clean
        n = 2
        while os.path.exists(os.path.join(UNPACKED_DIR, final)):
            final = '{}_{}'.format(clean, n)
            n += 1
        try:
            os.rename(os.path.join(UNPACKED_DIR, raw),
                      os.path.join(UNPACKED_DIR, final))
            renamed.append((repr(raw), final))
        except Exception as e:
            print("[ВНИМАНИЕ] не удалось переименовать битую папку {!r}: {}".format(raw, e))
    return renamed


def discover_all_projects():
    if not os.path.isdir(UNPACKED_DIR):
        return []
    return sorted([
        p for p in os.listdir(UNPACKED_DIR)
        if os.path.isdir(os.path.join(UNPACKED_DIR, p, "src"))
    ])


LOCK_PATH = os.path.join(WORK_ROOT, ".lock")


def _pid_alive(pid):
    """True, если процесс с таким PID существует."""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def acquire_lock(force=False):
    """Ставит лок от параллельного запуска. Возвращает True при успехе.

    Сервер держит один проект за раз, поэтому два параллельных прогона
    конфликтуют (снос чужого проекта, гонки). Лок это предотвращает.
    Если найден протухший лок (процесс с записанным PID мёртв) — забираем его.
    С force=True лок ставится принудительно поверх любого.
    """
    os.makedirs(WORK_ROOT, exist_ok=True)
    if os.path.exists(LOCK_PATH) and not force:
        old_pid = None
        old_info = ""
        try:
            with open(LOCK_PATH) as f:
                old_info = f.read().strip()
            old_pid = int(old_info.split()[0])
        except Exception:
            old_pid = None
        if old_pid and _pid_alive(old_pid):
            print("\n[ОШИБКА] Уже идёт другой прогон (lock: {}).".format(old_info))
            print("   Файл лока: {}".format(LOCK_PATH))
            print("   Дождитесь завершения или запустите с --force, если уверены,")
            print("   что другого прогона нет.")
            return False
        # лок протух (процесс мёртв) — заберём
        print("[ИНФО] Найден протухший лок (PID {} не активен) — перезабираю".format(old_pid))
    try:
        with open(LOCK_PATH, 'w') as f:
            f.write("{} {}".format(os.getpid(),
                                   datetime.now().isoformat(timespec='seconds')))
    except Exception as e:
        print("[ОШИБКА] Не удалось создать лок {}: {}".format(LOCK_PATH, e))
        return False
    return True


def release_lock():
    """Снимает лок, если он наш (или просто удаляет файл)."""
    try:
        if os.path.exists(LOCK_PATH):
            os.remove(LOCK_PATH)
    except Exception:
        pass


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
    parser.add_argument('--no-lang-filter', action='store_true',
                        help='Паковать ВСЕ файлы (иначе только расширения языков АК-ВС)')
    parser.add_argument('--force', action='store_true',
                        help='Игнорировать лок и запуститься даже если есть другой прогон')
    args = parser.parse_args()

    cfg = {
        'server': args.server, 'port': args.port,
        'login': args.login, 'password': args.password,
        'timeout': args.timeout, 'level': args.level,
        'java': args.java_bin, 'jar': args.jar,
        'lang_filter': not args.no_lang_filter,
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

    # Чиним битые cp1251-имена ПАПОК проектов в unpacked/ (иначе падаем
    # на выводе списка и на путях). Латиница безопасна и для -n на сервере.
    for _old, _new in repair_project_dir_names():
        print("[ИНФО] Битое имя папки проекта {} -> {}".format(_old, _new))

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

    # Лок от параллельного запуска (сервер держит 1 проект — два прогона
    # конфликтуют). Снимается в finally ниже.
    if not acquire_lock(force=args.force):
        sys.exit(1)

    try:
        _run_all(projects, cfg, args)
    finally:
        release_lock()


def _run_all(projects, cfg, args):
    # Проверка сервера ДО старта: если недоступен — не начинаем, чтобы не
    # плодить полусозданные проекты (сервер держит только 1 проект).
    class _PreLog(object):
        def info(self, m): print("    {}".format(m))
        def error(self, m): print("    [ERROR] {}".format(m))
    names0, ok0 = akvs_list_projects(cfg, _PreLog())
    if not ok0:
        print("\n[ОШИБКА] Сервер {}:{} недоступен (project list не ответил).".format(
            cfg['server'], cfg['port']))
        print("   Проверьте, что сервер запущен и совместим, затем повторите.")
        sys.exit(1)
    if names0:
        print("[ВНИМАНИЕ] На сервере уже есть проекты: {}".format(', '.join(names0)))
        print("           Они будут удалены при очистке слота перед каждым проектом.")

    start_time = datetime.now()
    results = {}
    leftovers = []   # проекты, которые НЕ удалось удалить с сервера
    for project in projects:
        results[project] = process_project(project, cfg, args.keep_raw, leftovers)

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
    print("Сводка (summary): {}".format(SUMMARY_DIR))
    print("Логи           : {}".format(LOG_DIR))

    if failed:
        print("\nПроекты с ошибкой (лог + serverlog в logs/akvs/<project>/):")
        for p in failed:
            print("  - {}".format(p))
    if skipped:
        print("\nПропущенные проекты (нет src/):")
        for p in skipped:
            print("  - {}".format(p))

    # Уровень 1+2: громкое предупреждение о «хвостах» на сервере.
    # Лицензия сервера = 1 проект, поэтому незакрытые проекты при следующем
    # старте роняют сервер в цикл падений (Too many projects for this license).
    if leftovers:
        uniq = sorted(set(leftovers))
        print("\n" + "!" * 60)
        print("ВНИМАНИЕ: следующие проекты, возможно, ОСТАЛИСЬ на сервере:")
        for p in uniq:
            print("  - {}".format(p))
        print("Удалите их вручную, иначе сервер может упасть по лимиту лицензии:")
        print("  java -jar {} project delete -s {} -p {} -n <ИМЯ>".format(
            cfg['jar'], cfg['server'], cfg['port']))
        print("Проверить слот: project list. Если сервер уже в цикле падений —")
        print("почистите БД: mongo akvs3 --eval 'db.projects.deleteMany({})'")
        print("!" * 60)

    sys.exit(0 if (not failed and not leftovers) else 1)


if __name__ == '__main__':
    main()
