#!/usr/bin/env python3
"""
=============================================================================
svace-upload.py — Выгрузка результатов Svace (OB) в Svacer с дедупликацией
=============================================================================

ОПИСАНИЕ:
    Выгружает общий анализ (Python + JavaScript) из svace/PROJ/OB/ на сервер
    Svacer как снапшот PROJ_ob_vN и ведёт хронологию итераций по версиям
    исходного кода. Выгрузка этапов (PY, JS и др.) скриптом не выполняется —
    такие снапшоты выгружаются вручную.

    Формат имени снапшота:
        <Изделие>[_<Этап>]_(b|ob)_v<N>[_rc]

УЧИТЫВАЕМЫЕ СНАПШОТЫ:
    PROJ_ob_vN              — итерации скрипта (N_max и проверка на дубль)
    PROJ_ob_vN_rc           — разметка разработчика (N_max, закрывает итерацию)
    PROJ_<Этап>_ob_vN_rc    — разметка разработчика любого этапа (то же)
    Игнорируются: PROJ_<Этап>_ob_vN без _rc, все PROJ_..._b_...

АЛГОРИТМ (по снимку сервера, снятому один раз на изделие):
    1. Учитываемых снапшотов нет                  -> PROJ_ob_v1.
    2. N_max — старшая версия среди учитываемых.
    3. На N_max есть любой _rc                    -> PROJ_ob_v(N_max+1), без проверки на дубль.
    4. Иначе проверка на дубль с PROJ_ob_vN_max:
         совпадает                                -> пропуск (SKIPPED);
         отличается                               -> PROJ_ob_v(N_max+1).

    Дубль — совпадают версия svace, исходники и срабатывания.
    Исходники: хэши ГОСТ Р 34.11-2012 (256 бит) и MD5 деревьев Python и JS,
    записываются в custom_fields каждого снапшота:
        src_py_gost12_256, src_py_md5, src_js_gost12_256, src_js_md5, dedup_mode
    Хэш дерева: строки "<хэш файла>  <отн. путь>\\n", отсортированные по байтам
    пути, хэшированные тем же алгоритмом.
    Если у ob_vN хэшей нет (выгружен ранее), исходники проверяются по .snap
    (svacer server export): множество md5 файлов снапшота против файлов src.
    Если проверить нельзя — сравнение только по срабатываниям с [WARN].

    Разметка из PROJ_<Этап>_ob_vN_rc ресивером в PROJ_ob_v(N+1) не переносится
    (ресивер ищет PROJ_ob_vN_rc) — в этом случае выводится [WARN].

    При старте выполняется самопроверка алгоритмов на эталонном файле
    test/ensure-natural-number-value.js. ГОСТ считается через rhash
    (--gost12-256) или gostsum (--gost-2012).

ИСПОЛЬЗОВАНИЕ:
    python3 svace-upload.py [OPTIONS]

ОПЦИИ:
    --project NAME         Изделие (можно несколько раз). По умолчанию все
                           изделия, у которых есть svace/PROJ/OB/.svace-dir
    --dry-run              Показать решения, ничего не выгружать
    --force                Выгружать, даже если исходники изменены после анализа
    --no-snap-check        Не проверять исходники старых снапшотов через .snap
    --host / --port        Сервер Svacer ($SVACER_HOST / $SVACER_PORT)
    --user / --password    Учётная запись ($SVACER_USER / $SVACER_PASSWORD)
    --svacer-bin PATH      Бинарь svacer ($SVACER_BIN, по умолчанию svacer)
    --timeout SEC          Таймаут HTTP-запросов (по умолчанию 300)

СТРУКТУРА:
    analyze-tools/
    ├── scripts/svace-upload.py
    ├── test/ensure-natural-number-value.js   ← эталон самопроверки
    ├── unpacked/PROJ/src/
    ├── svace/PROJ/OB/.svace-dir/             ← результат svace-analyze.py без флагов
    └── logs/svacer-upload/
        ├── run_YYYYMMDD_HHMMSS.log / .json
        ├── projects/PROJ.log
        └── locks/PROJ.lock

ЗАВИСИМОСТИ:
    Python 3.6+, svacer, rhash >= 1.4 или gostsum, zstd (для проверки по .snap)
=============================================================================
"""

import os
import re
import sys
import json
import shutil
import base64
import hashlib
import argparse
import subprocess
import tempfile
import zlib
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime

# =============================================================================
# НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
# =============================================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

UNPACKED_DIR   = os.path.join(BASE_DIR, "unpacked")
SVACE_WORK_DIR = os.path.join(BASE_DIR, "svace")
LOG_DIR        = os.path.join(BASE_DIR, "logs", "svacer-upload")

DEFAULT_SVACER_BIN = os.environ.get("SVACER_BIN", "svacer")
DEFAULT_HOST       = os.environ.get("SVACER_HOST", "192.168.25.167")
DEFAULT_PORT       = os.environ.get("SVACER_PORT", "8080")
DEFAULT_USER       = os.environ.get("SVACER_USER", "admin")
DEFAULT_PASSWORD   = os.environ.get("SVACER_PASSWORD", "admin")

TRACK = "ob"
MODE_DIR = "OB"     # подпапка результатов общего анализа в svace/PROJ/

# Эталон для самопроверки алгоритмов хэширования
TEST_VECTOR_FILE   = os.path.join(BASE_DIR, "test", "ensure-natural-number-value.js")
TEST_VECTOR_GOST12 = "6cd6b4a2f0887bebf443193f2a9e0233a3c5049b48dbc0679b588d07df3f6554"
TEST_VECTOR_MD5    = "dc6fd4a06cb9a6b47fbf1c369f4a84ab"
EMPTY_GOST12       = "3f539a213e97c802cc229d474c6aa32a825a360b2a933a949fd925208d9ce1bb"

LANG_EXTENSIONS = {
    'py': ('.py', '.pyw'),
    'js': ('.js', '.jsx', '.ts', '.tsx', '.mjs', '.cjs'),
}

HASH_ALGS = ('gost12_256', 'md5')
# =============================================================================

def field_name(lang, alg):
    return "src_{}_{}".format(lang, alg)


# =============================================================================
# ЛОГИРОВАНИЕ
# =============================================================================
class Logger(object):
    def __init__(self, run_log):
        self.run_log = run_log
        self.project_log = None
        self.secrets = []

    def _mask(self, text):
        for s in self.secrets:
            if s:
                text = text.replace(s, '***')
        return text

    def _write(self, path, text):
        try:
            with open(path, 'a', encoding='utf-8') as f:
                f.write(text + '\n')
        except Exception as e:
            print("[LOG ERROR] {}: {}".format(path, e))

    def _log(self, level, message):
        line = self._mask("[{}] [{}] {}".format(
            datetime.now().strftime('%H:%M:%S'), level, message))
        print(line)
        self._write(self.run_log, line)
        if self.project_log:
            self._write(self.project_log, line)

    def info(self, message):  self._log("INFO", message)
    def warn(self, message):  self._log("WARN", message)
    def error(self, message): self._log("ERROR", message)

    def raw(self, text):
        if self.project_log:
            self._write(self.project_log, self._mask(text))


# =============================================================================
# ЗАПУСК КОМАНД
# =============================================================================
def run_cmd(cmd, cwd, log, name):
    log.info("{}: {}".format(name, ' '.join(cmd)))
    log.raw("--- {} --- CWD: {}".format(name, cwd))
    try:
        r = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as e:
        log.error("{}: не удалось запустить {}: {}".format(name, cmd[0], e))
        return False, ""
    out = r.stdout.decode('utf-8', errors='replace') + r.stderr.decode('utf-8', errors='replace')
    if out.strip():
        log.raw(out.rstrip())
    log.raw("--- код возврата: {} ---".format(r.returncode))
    if r.returncode != 0:
        log.error("{}: код возврата {}".format(name, r.returncode))
        return False, out
    log.info("{}: OK".format(name))
    return True, out


# =============================================================================
# ХЭШИРОВАНИЕ
# =============================================================================
HEX64 = re.compile(r'^[0-9a-f]{64}$')


class GostHasher(object):
    """ГОСТ Р 34.11-2012 256 бит через rhash или gostsum."""

    def __init__(self):
        if shutil.which('rhash'):
            self.kind = 'rhash'
        elif shutil.which('gostsum'):
            self.kind = 'gostsum'
        else:
            raise RuntimeError("не найдены ни rhash, ни gostsum")

    def _cmd(self, args):
        if self.kind == 'rhash':
            return ['rhash', '--gost12-256', '--printf=%{gost12-256}\\n'] + args
        return ['gostsum', '--gost-2012'] + args

    @staticmethod
    def _parse(output):
        hashes = []
        for line in output.decode('utf-8', errors='replace').splitlines():
            if not line.strip():
                continue
            token = line.split()[0].lstrip('\\').lower()
            if not HEX64.match(token):
                raise RuntimeError("неожиданный вывод: {!r}".format(line[:120]))
            hashes.append(token)
        return hashes

    def hash_bytes(self, data):
        args = ['-'] if self.kind == 'rhash' else []
        r = subprocess.run(self._cmd(args), input=data,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if r.returncode != 0:
            raise RuntimeError(r.stderr.decode('utf-8', errors='replace').strip())
        hashes = self._parse(r.stdout)
        if len(hashes) != 1:
            raise RuntimeError("ожидался один хэш, получено {}".format(len(hashes)))
        return hashes[0]

    def hash_files(self, paths, chunk=100):
        result = []
        for i in range(0, len(paths), chunk):
            part = paths[i:i + chunk]
            hashes = None
            try:
                r = subprocess.run(self._cmd(list(part)),
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if r.returncode == 0:
                    parsed = self._parse(r.stdout)
                    if len(parsed) == len(part):
                        hashes = parsed
            except RuntimeError:
                hashes = None
            if hashes is None:
                # Пакетный режим не дал однозначного результата — по одному файлу через stdin
                hashes = []
                for p in part:
                    with open(p, 'rb') as f:
                        hashes.append(self.hash_bytes(f.read()))
            result.extend(hashes)
        return result


def md5_file(path):
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def collect_files(src_dir, extensions):
    """Как find -type f -name: без симлинков, сортировка по байтам относительного пути."""
    files = []
    for root, _dirs, names in os.walk(src_dir):
        for name in names:
            if not name.endswith(extensions):
                continue
            path = os.path.join(root, name)
            if os.path.islink(path) or not os.path.isfile(path):
                continue
            files.append((os.fsencode(os.path.relpath(path, src_dir)), path))
    files.sort(key=lambda x: x[0])
    return files


def compute_source_hashes(src_dir, hasher):
    """Хэши деревьев исходников Python и JS + число файлов и максимальный mtime."""
    result = {'counts': {}, 'max_mtime': {}, 'md5set': {}}
    for lang, exts in LANG_EXTENSIONS.items():
        files = collect_files(src_dir, exts)
        paths = [p for _, p in files]
        gost = hasher.hash_files(paths) if paths else []
        man_g, man_m = [], []
        result['max_mtime'][lang] = 0.0
        result['md5set'][lang] = set()
        for (rel, path), g in zip(files, gost):
            man_g.append(g.encode('ascii') + b'  ' + rel + b'\n')
            file_md5 = md5_file(path)
            result['md5set'][lang].add(file_md5)
            man_m.append(file_md5.encode('ascii') + b'  ' + rel + b'\n')
            result['max_mtime'][lang] = max(result['max_mtime'][lang], os.path.getmtime(path))
        result[field_name(lang, 'gost12_256')] = hasher.hash_bytes(b''.join(man_g))
        result[field_name(lang, 'md5')] = hashlib.md5(b''.join(man_m)).hexdigest()
        result['counts'][lang] = len(files)
    return result


# =============================================================================
# ПРОВЕРКА ИСХОДНИКОВ СТАРЫХ СНАПШОТОВ ЧЕРЕЗ .snap
# =============================================================================
# У снапшотов, выгруженных до появления хэшей в custom_fields, исходники
# проверяются по содержимому: svacer server export -> zstd -> gzip-блоки.
# Формат .snap внутренний: при любой неудаче проверка возвращает "нельзя
# проверить", и скрипт выбирает безопасный вариант (новая итерация).
# Сравнивается множество содержимого файлов (md5), без путей: переименование
# файла без изменения содержимого этой проверкой не обнаруживается.

GOB_PREFIX = b'\r\xff\x83\x02\x01\x02\xff\x84'  # служебные блоки разметки токенов svacer


def zstd_decompress_to(src, dst):
    if shutil.which('zstd'):
        with open(dst, 'wb') as out:
            r = subprocess.run(['zstd', '-dc', src], stdout=out, stderr=subprocess.PIPE)
        if r.returncode != 0:
            raise RuntimeError("zstd: " + r.stderr.decode('utf-8', errors='replace').strip())
        return
    try:
        import zstandard
    except ImportError:
        raise RuntimeError("нет ни утилиты zstd, ни модуля zstandard")
    with open(src, 'rb') as fi, open(dst, 'wb') as fo:
        zstandard.ZstdDecompressor().copy_stream(fi, fo)


def gzip_member_md5s(data, chunk=1 << 16):
    """md5 содержимого всех gzip-блоков внутри data, кроме служебных."""
    mv = memoryview(data)
    found = set()
    pos = 0
    size = len(data)
    while True:
        i = data.find(b'\x1f\x8b\x08', pos)
        if i < 0:
            break
        dec = zlib.decompressobj(31)
        parts = []
        p = i
        try:
            while not dec.eof and p < size:
                piece = mv[p:p + chunk]
                parts.append(dec.decompress(piece))
                p += len(piece)
        except zlib.error:
            pos = i + 1
            continue
        if not dec.eof:
            pos = i + 1
            continue
        blob = b''.join(parts)
        if not blob.startswith(GOB_PREFIX):
            found.add(hashlib.md5(blob).hexdigest())
        pos = p - len(dec.unused_data)
    return found


class SnapVerifier(object):
    def __init__(self, args, product, log):
        self.args = args
        self.product = product
        self.log = log
        self.cache = {}

    def content(self, snap):
        name = snap['name']
        if name in self.cache:
            return self.cache[name]
        result = None
        tmp = tempfile.mkdtemp(prefix='svace-snap-')
        try:
            snap_file = os.path.join(tmp, 'snapshot.snap')
            host = re.sub(r'^https?://', '', self.args.host).rstrip('/')
            ok, _ = run_cmd([self.args.svacer_bin, 'server', 'export', '--host', host, '--port', str(self.args.port),
                             '--user', self.args.user, '--password', self.args.password,
                             '--project', self.product, '--snapshot', name, snap_file],
                            tmp, self.log, "svacer server export " + name)
            if ok and os.path.isfile(snap_file):
                raw = os.path.join(tmp, 'snapshot.raw')
                zstd_decompress_to(snap_file, raw)
                with open(raw, 'rb') as f:
                    result = gzip_member_md5s(f.read())
                self.log.info("{}: из .snap извлечено {} уникальных файлов".format(name, len(result)))
        except Exception as e:
            self.log.warn("{}: проверка по .snap не выполнена: {}".format(name, e))
            result = None
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        self.cache[name] = result
        return result


def snap_sources_verdict(verifier, snap, hashes):
    """
    True/False — исходники совпадают/отличаются с содержимым снапшота ob (Python + JS),
    None — проверить нельзя.
    """
    if verifier is None:
        return None
    content = verifier.content(snap)
    if not content:
        return None
    return content == (hashes['md5set']['py'] | hashes['md5set']['js'])


def self_test(hasher, log):
    log.info("Самопроверка хэширования (backend: {})".format(hasher.kind))
    if not os.path.isfile(TEST_VECTOR_FILE):
        log.error("Эталонный файл не найден: {}".format(TEST_VECTOR_FILE))
        return False
    ok = True
    checks = [
        ("gost12-256(file)",  hasher.hash_files([TEST_VECTOR_FILE])[0], TEST_VECTOR_GOST12),
        ("gost12-256(stdin)", hasher.hash_bytes(b''),                 EMPTY_GOST12),
        ("md5(file)",         md5_file(TEST_VECTOR_FILE),             TEST_VECTOR_MD5),
    ]
    for name, got, want in checks:
        if got == want:
            log.info("  {:<18} OK".format(name))
        else:
            log.error("  {:<18} ОШИБКА: {} != {}".format(name, got, want))
            ok = False
    return ok


# =============================================================================
# ЛОКАЛЬНЫЕ РЕЗУЛЬТАТЫ И СРАБАТЫВАНИЯ
# =============================================================================
def parse_analysis_info_text(text):
    info = {}
    for line in text.splitlines():
        if ':' not in line:
            continue
        key, value = line.split(':', 1)
        key = key.strip()
        if key in ('Svace version', 'Current directory') and key not in info:
            info[key] = value.strip()
    return info


def rel_path(path, src):
    path = path or ''
    if src:
        prefix = src.rstrip('/') + '/'
        if path.startswith(prefix):
            return path[len(prefix):]
    return path


def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def marker_key(warn_class, mtid, tool, lang, file_rel, line, function,
               orig_func, msg, details_sha1, flags):
    return (warn_class or '', mtid or '', tool or '', str(lang or '').upper(),
            file_rel, to_int(line), function or '', orig_func or '', msg or '',
            details_sha1 or '', to_int(flags))


def load_svres(path):
    keys = Counter()
    src = ''
    for _event, elem in ET.iterparse(path, events=('end',)):
        if elem.tag == 'projectSrcDir':
            src = (elem.text or '').strip()
        elif elem.tag == 'WarnInfo':
            details = elem.get('details') or ''
            keys[marker_key(
                elem.get('warnClass'), elem.get('mtid'), elem.get('tool'), elem.get('lang'),
                rel_path(elem.get('file'), src), elem.get('line'), elem.get('function'),
                elem.get('origFunc'), elem.get('msg'),
                hashlib.sha1(details.encode('utf-8')).hexdigest(), elem.get('flags'))] += 1
            elem.clear()
    return keys, src


def markers_to_keys(markers, src):
    keys = Counter()
    for m in markers:
        keys[marker_key(
            m.get('warnClass'), m.get('mtid'), m.get('tool'), m.get('lang'),
            rel_path(m.get('file'), src), m.get('line'), m.get('function'),
            m.get('origFunc'), m.get('msg'), m.get('details'), m.get('flags'))] += 1
    return keys


class LocalMode(object):
    """Результаты анализа одного режима на диске."""

    def __init__(self, product, mode):
        self.product = product
        self.mode = mode
        self.mode_dir = os.path.join(SVACE_WORK_DIR, product, mode)
        self.res_dir = os.path.join(self.mode_dir, '.svace-dir', 'analyze-res')
        self.svres = None
        self.svace_version = None
        self._keys = None
        self.src_in_svres = ''
        self.error = None

        if not os.path.isdir(self.res_dir):
            self.error = "нет результатов анализа: {}".format(self.res_dir)
            return
        svres = sorted(f for f in os.listdir(self.res_dir) if f.endswith('.svres'))
        if not svres:
            self.error = "не найден .svres в {}".format(self.res_dir)
            return
        self.svres = os.path.join(self.res_dir, svres[0])
        info_path = os.path.join(self.res_dir, 'analysis-info.txt')
        if os.path.isfile(info_path):
            with open(info_path, encoding='utf-8', errors='replace') as f:
                self.svace_version = parse_analysis_info_text(f.read()).get('Svace version')

    def keys(self):
        if self._keys is None:
            self._keys, self.src_in_svres = load_svres(self.svres)
        return self._keys


# =============================================================================
# API SVACER
# =============================================================================
class SvacerApi(object):
    def __init__(self, host, port, user, password, timeout):
        host = re.sub(r'^https?://', '', host).rstrip('/')
        self.base = "http://{}:{}".format(host, port)
        self.user = user
        self.password = password
        self.timeout = timeout
        self.token = None

    def login(self):
        creds = base64.b64encode("{}:{}".format(self.user, self.password).encode('utf-8')).decode('ascii')
        req = urllib.request.Request(self.base + "/api/login", data=b'', method='POST',
                                     headers={'Authorization': 'Basic ' + creds})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            self.token = json.loads(resp.read().decode('utf-8'))['token']

    def get(self, path, _retry=True):
        if not self.token:
            self.login()
        req = urllib.request.Request(self.base + path,
                                     headers={'Authorization': 'Bearer ' + self.token})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            if e.code == 401 and _retry:
                self.login()
                return self.get(path, _retry=False)
            raise

    def find_project(self, name):
        for item in self.get("/api/public/projects"):
            if item.get('project', {}).get('name') == name:
                branch = next((b for b in item.get('branches') or [] if b.get('name') == 'master'), None)
                return item['project']['id'], (branch or {}).get('id')
        return None, None

    def snapshots(self, project_id, branch_id):
        data = self.get("/api/public/projects/{}/branch/{}/snapshots".format(project_id, branch_id))
        if isinstance(data, dict):
            for key in ('snapshots', 'items', 'data', 'results'):
                if isinstance(data.get(key), list):
                    return data[key]
            raise RuntimeError("неожиданный формат списка снапшотов")
        return data

    def fullmarkers(self, project_id, branch_id, snapshot_id):
        return self.get("/api/public/projects/{}/branch/{}/snapshots/{}/fullmarkers".format(
            project_id, branch_id, snapshot_id))


# =============================================================================
# ИМЕНА СНАПШОТОВ
# =============================================================================

# =============================================================================
# ИМЕНА СНАПШОТОВ
# =============================================================================
def series_regex(product):
    return re.compile(r'^' + re.escape(product) + r'(?:_(?P<stage>.+?))?_' + TRACK +
                      r'_v(?P<n>\d+)(?P<rc>_rc)?$')


def parse_snapshots(product, raw_snapshots):
    """Все ob-снапшоты изделия: name, id, stage, n, rc, details."""
    rx = series_regex(product)
    result = []
    for s in raw_snapshots:
        m = rx.match(s.get('name', ''))
        if m:
            result.append({'name': s['name'], 'id': s.get('id'), 'stage': m.group('stage'),
                           'n': int(m.group('n')), 'rc': bool(m.group('rc')),
                           'details': s.get('details') or {}})
    return result


def is_counted(snap):
    """Учитываются ob_vN, ob_vN_rc и <Этап>_ob_vN_rc; <Этап>_ob_vN без _rc — нет."""
    return snap['stage'] is None or snap['rc']


def snapshot_name(product, n):
    return "{}_{}_v{}".format(product, TRACK, n)


def cf_value(details, name):
    value = (details.get('custom_fields') or {}).get(name)
    if isinstance(value, list):
        value = value[0] if value else None
    if value is None:
        return None
    value = str(value).strip().strip('"').strip().lower()
    return value or None


# =============================================================================
# ПРИНЯТИЕ РЕШЕНИЯ
# =============================================================================
def decide(snaps, compare):
    """
    snaps    — ob-снапшоты изделия (parse_snapshots)
    compare  — f(snap) -> (bool дубль, [причины]) для PROJ_ob_vN_max
    Возвращает dict: action ('upload' | 'skip'), iteration, base, notes
    """
    plan = {'action': 'upload', 'iteration': None, 'base': None, 'notes': []}
    counted = [s for s in snaps if is_counted(s)]

    if not counted:
        plan['iteration'] = 1
        plan['notes'].append("учитываемых ob-снапшотов нет -> v1")
        return plan

    n_max = max(s['n'] for s in counted)
    top = [s for s in counted if s['n'] == n_max]
    top_rc = [s for s in top if s['rc']]

    if top_rc:
        plan['iteration'] = n_max + 1
        plan['notes'].append("на v{} есть разметка разработчика ({}) -> v{}".format(
            n_max, ', '.join(s['name'] for s in top_rc), n_max + 1))
        return plan

    base = top[0]  # на N_max без _rc учитывается только PROJ_ob_vN
    plan['base'] = base['name']
    same, reasons = compare(base)
    plan['notes'].extend("vs {}: {}".format(base['name'], r) for r in reasons)
    if same:
        plan['action'] = 'skip'
        plan['iteration'] = n_max
        plan['notes'].append("совпадает с {} -> дубль".format(base['name']))
    else:
        plan['iteration'] = n_max + 1
        plan['notes'].append("отличается от {} -> v{}".format(base['name'], n_max + 1))
    return plan


# =============================================================================
# ОБРАБОТКА ИЗДЕЛИЯ
# =============================================================================
class ProductLock(object):
    def __init__(self, product):
        self.path = os.path.join(LOG_DIR, 'locks', product + '.lock')
        self.acquired = False

    def acquire(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        for _ in range(2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode('ascii'))
                os.close(fd)
                self.acquired = True
                return True, None
            except FileExistsError:
                try:
                    with open(self.path) as f:
                        pid = int(f.read().strip() or 0)
                    if pid <= 0:
                        raise ValueError
                    os.kill(pid, 0)
                    return False, pid
                except PermissionError:
                    return False, pid  # процесс жив, но принадлежит другому пользователю
                except (ValueError, ProcessLookupError):
                    try:
                        os.remove(self.path)  # устаревшая блокировка
                    except FileNotFoundError:
                        pass
        return False, None

    def release(self):
        if self.acquired and os.path.exists(self.path):
            os.remove(self.path)


def upload_host_arg(host, port):
    host = re.sub(r'^https?://', '', host).rstrip('/')
    return host if str(port) == '8080' else "http://{}:{}".format(host, port)


def process_product(product, args, api, hasher, log):
    result = {'product': product, 'status': None, 'iteration': None, 'snapshot': None,
              'notes': [], 'message': ''}
    log.project_log = os.path.join(LOG_DIR, 'projects', product + '.log')
    log.raw("\n" + "=" * 70 + "\nRUN {}\n".format(datetime.now().isoformat(timespec='seconds')) + "=" * 70)
    log.info("=" * 60)
    log.info("Изделие: {}".format(product))

    def finish(status, message=''):
        result['status'] = status
        result['message'] = message
        if message:
            (log.error if status == 'FAILED' else log.info)("{}: {}".format(status, message))
        log.project_log = None
        return result

    src_dir = os.path.join(UNPACKED_DIR, product, "src")
    if not os.path.isdir(src_dir):
        return finish('FAILED', "нет исходников {}".format(src_dir))

    # --- Хэши исходников ---
    try:
        hashes = compute_source_hashes(src_dir, hasher)
    except Exception as e:
        return finish('FAILED', "ошибка хэширования: {}".format(e))
    log.info("Файлов: py={} js={}".format(hashes['counts']['py'], hashes['counts']['js']))
    for lang in LANG_EXTENSIONS:
        log.info("  {}: gost12={} md5={}".format(lang, hashes[field_name(lang, 'gost12_256')],
                                                 hashes[field_name(lang, 'md5')]))
    if hashes['counts']['py'] + hashes['counts']['js'] == 0:
        return finish('SKIPPED', "нет файлов Python и JavaScript")

    # --- Локальные результаты OB ---
    lm = LocalMode(product, MODE_DIR)
    if lm.error:
        return finish('FAILED', lm.error)
    newest = max(hashes['max_mtime'].values())
    if newest > os.path.getmtime(lm.svres):
        if not args.force:
            return finish('FAILED', "исходники изменены после анализа ({}), перезапустите "
                                    "svace-analyze.py (обход: --force)".format(lm.svres))
        log.warn("--force: исходники изменены после анализа")
    log.info("Результаты: {} (svace {})".format(lm.svres, lm.svace_version or '?'))

    # --- Блокировка и снимок сервера ---
    lock = ProductLock(product)
    ok, pid = lock.acquire()
    if not ok:
        return finish('FAILED', "изделие уже обрабатывается (pid {})".format(pid))
    try:
        try:
            project_id, branch_id = api.find_project(product)
            raw = api.snapshots(project_id, branch_id) if project_id and branch_id else []
        except Exception as e:
            return finish('FAILED', "ошибка API: {}".format(e))

        snaps = sorted(parse_snapshots(product, raw), key=lambda s: (s['n'], s['stage'] or '', s['rc']))
        counted = [s['name'] for s in snaps if is_counted(s)]
        ignored = [s['name'] for s in snaps if not is_counted(s)]
        log.info("Снимок сервера: учитываются [{}]".format(', '.join(counted)))
        if ignored:
            log.info("               игнорируются [{}]".format(', '.join(ignored)))

        warnings = []
        verifier = None if args.no_snap_check else SnapVerifier(args, product, log)

        def compare(snap):
            srv_info = parse_analysis_info_text(snap['details'].get('analysis-info') or '')
            srv_ver = srv_info.get('Svace version')
            if lm.svace_version and srv_ver and lm.svace_version != srv_ver:
                return False, ["версия svace {} != {}".format(lm.svace_version, srv_ver)]
            reasons = []
            no_hash = False
            for lang in LANG_EXTENSIONS:
                for alg in HASH_ALGS:
                    name = field_name(lang, alg)
                    srv = cf_value(snap['details'], name)
                    if srv is None:
                        no_hash = True
                    elif srv != hashes[name]:
                        return False, ["{} отличается".format(name)]
            if no_hash:
                verdict = snap_sources_verdict(verifier, snap, hashes)
                if verdict is False:
                    return False, ["исходники отличаются (проверка по .snap)"]
                if verdict is True:
                    reasons.append("исходники совпадают (проверка по .snap)")
                else:
                    warnings.append("у {} нет хэшей исходников и проверка по .snap невозможна: "
                                    "сравнение только по срабатываниям".format(snap['name']))
            else:
                reasons.append("хэши исходников совпадают")
            markers = api.fullmarkers(project_id, branch_id, snap['id'])
            srv_keys = markers_to_keys(markers, srv_info.get('Current directory') or lm.src_in_svres)
            loc_keys = lm.keys()
            if loc_keys != srv_keys:
                reasons.append("срабатывания отличаются: локально {}, на сервере {}, "
                               "только локально {}, только на сервере {}".format(
                                   sum(loc_keys.values()), sum(srv_keys.values()),
                                   sum((loc_keys - srv_keys).values()), sum((srv_keys - loc_keys).values())))
                return False, reasons
            reasons.append("срабатывания совпадают ({})".format(sum(loc_keys.values())))
            return True, reasons

        try:
            plan = decide(snaps, compare)
        except Exception as e:
            return finish('FAILED', "ошибка сравнения: {}".format(e))

        n = plan['iteration']
        stage_rc = [s['name'] for s in snaps if s['rc'] and s['stage'] is not None and s['n'] == n - 1]
        if plan['action'] == 'upload' and stage_rc:
            warnings.append("разметка из [{}] не будет перенесена ресивером в {} "
                            "(ресивер ищет {})".format(', '.join(stage_rc), snapshot_name(product, n),
                                                       snapshot_name(product, n - 1) + "_rc"))
        for w in warnings:
            log.warn(w)
        for note in plan['notes']:
            log.info("  " + note)
        result['notes'] = warnings + plan['notes']
        result['iteration'] = n
        name = snapshot_name(product, n)
        result['snapshot'] = name

        if plan['action'] == 'skip':
            return finish('SKIPPED', "дубль {}".format(name))

        if any(s['name'] == name for s in snaps):
            return finish('FAILED', "снапшот {} уже существует — решение противоречиво".format(name))

        if args.dry_run:
            log.info("DRY-RUN: выгрузил бы {}".format(name))
            return finish('DRY-RUN')

        # --- Выгрузка ---
        store = os.path.join(lm.mode_dir, '.svacer-dir')
        if os.path.isdir(store):
            shutil.rmtree(store)  # чтобы upload отправил ровно одну запись
        cmd = [args.svacer_bin, 'import', '--project', product, '--snapshot', name]
        for lang in LANG_EXTENSIONS:
            for alg in HASH_ALGS:
                cmd += ['--field', "{}:{}".format(field_name(lang, alg), hashes[field_name(lang, alg)])]
        cmd += ['--field', "dedup_mode:{}".format(MODE_DIR), lm.mode_dir]
        ok, _ = run_cmd(cmd, lm.mode_dir, log, "svacer import " + name)
        if not ok:
            return finish('FAILED', "import {}".format(name))
        ok, out = run_cmd([args.svacer_bin, 'upload',
                           '--host', upload_host_arg(args.host, args.port),
                           '--user', args.user, '--password', args.password],
                          lm.mode_dir, log, "svacer upload " + name)
        if not ok:
            return finish('FAILED', "upload {}".format(name))
        found = re.search(r'Found (\d+) records', out)
        if found and found.group(1) != '1':
            log.warn("upload отправил {} записей вместо 1".format(found.group(1)))

        # --- Контроль на сервере ---
        try:
            project_id, branch_id = api.find_project(product)
            names = set(s.get('name') for s in api.snapshots(project_id, branch_id))
            if name not in names:
                return finish('FAILED', "после выгрузки {} не найден на сервере".format(name))
        except Exception as e:
            log.warn("контроль после выгрузки не выполнен: {}".format(e))

        return finish('UPLOADED')
    finally:
        lock.release()


# =============================================================================
# ОСНОВНОЙ ЦИКЛ
# =============================================================================
def discover_products():
    if not os.path.isdir(SVACE_WORK_DIR):
        return []
    return sorted(p for p in os.listdir(SVACE_WORK_DIR)
                  if os.path.isdir(os.path.join(SVACE_WORK_DIR, p, MODE_DIR, '.svace-dir')))


def main():
    parser = argparse.ArgumentParser(description='Выгрузка результатов Svace (OB) в Svacer с дедупликацией',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--project', metavar='NAME', action='append', default=None)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--no-snap-check', action='store_true')
    parser.add_argument('--host', default=DEFAULT_HOST)
    parser.add_argument('--port', default=DEFAULT_PORT)
    parser.add_argument('--user', default=DEFAULT_USER)
    parser.add_argument('--password', default=DEFAULT_PASSWORD)
    parser.add_argument('--svacer-bin', default=DEFAULT_SVACER_BIN)
    parser.add_argument('--timeout', type=int, default=300)
    args = parser.parse_args()

    for d in ('', 'projects', 'locks'):
        os.makedirs(os.path.join(LOG_DIR, d), exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log = Logger(os.path.join(LOG_DIR, "run_{}.log".format(stamp)))
    log.secrets = [args.password]

    log.info("BASE_DIR : {}".format(BASE_DIR))
    log.info("Сервер   : {}:{} (user {})".format(args.host, args.port, args.user))
    log.info("Режим    : OB{}{}".format("  [DRY-RUN]" if args.dry_run else "",
                                        "  [FORCE]" if args.force else ""))

    try:
        hasher = GostHasher()
        if not self_test(hasher, log):
            log.error("Самопроверка не пройдена. Выгрузка остановлена.")
            sys.exit(2)
    except Exception as e:
        log.error("Хэширование недоступно: {}".format(e))
        sys.exit(2)

    try:
        subprocess.run([args.svacer_bin, '--version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError:
        log.error("svacer не найден: {}".format(args.svacer_bin))
        sys.exit(2)

    api = SvacerApi(args.host, args.port, args.user, args.password, args.timeout)
    try:
        api.login()
        log.info("Авторизация на сервере: OK")
    except Exception as e:
        log.error("Авторизация на сервере не удалась: {}".format(e))
        sys.exit(2)

    products = args.project or discover_products()
    if not products:
        log.error("Нет изделий с результатами svace/PROJ/{}/ в {}".format(MODE_DIR, SVACE_WORK_DIR))
        sys.exit(1)
    log.info("Изделий: {} ({})".format(len(products), ', '.join(products)))

    results = []
    for product in products:
        try:
            results.append(process_product(product, args, api, hasher, log))
        except Exception as e:
            log.project_log = None
            log.error("{}: непредвиденная ошибка: {}".format(product, e))
            results.append({'product': product, 'status': 'FAILED', 'message': str(e),
                            'iteration': None, 'snapshot': None, 'notes': []})

    log.info("")
    log.info("=" * 60)
    log.info("ИТОГ")
    for r in results:
        log.info("  {:<10} {:<24} {}".format(r['status'], r['product'],
                                             r.get('snapshot') or r.get('message', '')))

    report = os.path.join(LOG_DIR, "run_{}.json".format(stamp))
    with open(report, 'w', encoding='utf-8') as f:
        json.dump({'started': stamp, 'dry_run': args.dry_run, 'results': results},
                  f, ensure_ascii=False, indent=2)
    log.info("Отчёт: {}".format(report))

    sys.exit(1 if any(r['status'] == 'FAILED' for r in results) else 0)


if __name__ == '__main__':
    main()
