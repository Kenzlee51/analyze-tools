#!/usr/bin/env python3
"""
=============================================================================
svace-upload.py — Выгрузка результатов Svace в Svacer с дедупликацией
=============================================================================

ОПИСАНИЕ:
    Выгружает результаты svace-analyze.py из svace/PROJ/{OB,PY,JS}/ на сервер
    Svacer, назначая номер итерации и не допуская дублей.

    Формат имени снапшота:
        <Изделие>[_<Этап>]_ob_v<N>[_rc]
    Режимы (как в svace-analyze.py):
        (без флагов)       -> svace/PROJ/OB/  -> PROJ_ob_vN
        --only-python      -> svace/PROJ/PY/  -> PROJ_PY_ob_vN
        --only-javascript  -> svace/PROJ/JS/  -> PROJ_JS_ob_vN
        --separate         -> PY и JS         -> PROJ_PY_ob_vN, PROJ_JS_ob_vN

    Учитываются только ob-снапшоты изделия (без этапа, PY, JS, прочие этапы).
    b-снапшоты нумеруются независимо и не рассматриваются.

АЛГОРИТМ (по снимку состояния сервера, снятому один раз на изделие):
    1. ob-снапшотов нет                         -> v1.
    2. N_max = максимум N по ob-снапшотам, включая _rc.
    3. На уровне N_max есть _rc                  -> новая итерация N_max+1.
       Если _rc есть только у других этапов, а не у выгружаемых,
       разметка не будет перенесена ресивером -> BLOCKED (обход: --force).
    4. _rc нет — для каждого режима сравнение со снапшотом того же этапа
       на N_max (версия svace, хэши исходников, срабатывания):
         - хоть один режим отличается           -> N_max+1, все режимы;
         - все режимы совпали                   -> SKIP (дубль);
         - части режимов нет на N_max           -> если исходники совпадают
           с итерацией N_max, недостающие добавляются в N_max,
           иначе (или проверить нельзя)         -> N_max+1, все режимы.

    Хэши исходников (ГОСТ Р 34.11-2012 256 бит и MD5) считаются для Python и
    JavaScript и записываются в custom_fields каждого снапшота:
        src_py_gost12_256, src_py_md5, src_js_gost12_256, src_js_md5, dedup_mode
    Хэш дерева: строки "<хэш файла>  <отн. путь>\\n", отсортированные по
    байтам пути, хэшированные тем же алгоритмом.

    При старте выполняется самопроверка алгоритмов на эталонном файле
    test/ensure-natural-number-value.js. ГОСТ считается через rhash
    (--gost12-256) или gostsum (--gost-2012).

ИСПОЛЬЗОВАНИЕ:
    python3 svace-upload.py [OPTIONS]

ОПЦИИ:
    --project NAME         Изделие (можно несколько раз). По умолчанию все
                           изделия из svace/, где есть результаты режима.
    --only-python          Режим PY
    --only-javascript,
    --only-js              Режим JS
    --separate             Режимы PY и JS
    --dry-run              Показать решения, ничего не выгружать
    --force                Выгружать несмотря на BLOCKED
    --host / --port        Сервер Svacer ($SVACER_HOST / $SVACER_PORT)
    --user / --password    Учётная запись ($SVACER_USER / $SVACER_PASSWORD)
    --svacer-bin PATH      Бинарь svacer ($SVACER_BIN, по умолчанию svacer)
    --timeout SEC          Таймаут HTTP-запросов (по умолчанию 300)

СТРУКТУРА:
    analyze-tools/
    ├── scripts/svace-upload.py
    ├── test/ensure-natural-number-value.js   ← эталон самопроверки
    ├── unpacked/PROJ/src/
    ├── svace/PROJ/{OB,PY,JS}/.svace-dir/
    └── logs/svacer-upload/
        ├── run_YYYYMMDD_HHMMSS.log / .json
        ├── projects/PROJ.log
        └── locks/PROJ.lock

ЗАВИСИМОСТИ:
    Python 3.6+, svacer, rhash >= 1.4 или gostsum
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

# Эталон для самопроверки алгоритмов хэширования
TEST_VECTOR_FILE   = os.path.join(BASE_DIR, "test", "ensure-natural-number-value.js")
TEST_VECTOR_GOST12 = "6cd6b4a2f0887bebf443193f2a9e0233a3c5049b48dbc0679b588d07df3f6554"
TEST_VECTOR_MD5    = "dc6fd4a06cb9a6b47fbf1c369f4a84ab"
EMPTY_GOST12       = "3f539a213e97c802cc229d474c6aa32a825a360b2a933a949fd925208d9ce1bb"

LANG_EXTENSIONS = {
    'py': ('.py', '.pyw'),
    'js': ('.js', '.jsx', '.ts', '.tsx', '.mjs', '.cjs'),
}

# Режим -> этап в имени снапшота и языки
MODES = {
    'OB': {'stage': None, 'langs': ['py', 'js']},
    'PY': {'stage': 'PY', 'langs': ['py']},
    'JS': {'stage': 'JS', 'langs': ['js']},
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
    result = {'counts': {}, 'max_mtime': {}}
    for lang, exts in LANG_EXTENSIONS.items():
        files = collect_files(src_dir, exts)
        paths = [p for _, p in files]
        gost = hasher.hash_files(paths) if paths else []
        man_g, man_m = [], []
        result['max_mtime'][lang] = 0.0
        for (rel, path), g in zip(files, gost):
            man_g.append(g.encode('ascii') + b'  ' + rel + b'\n')
            man_m.append(md5_file(path).encode('ascii') + b'  ' + rel + b'\n')
            result['max_mtime'][lang] = max(result['max_mtime'][lang], os.path.getmtime(path))
        result[field_name(lang, 'gost12_256')] = hasher.hash_bytes(b''.join(man_g))
        result[field_name(lang, 'md5')] = hashlib.md5(b''.join(man_m)).hexdigest()
        result['counts'][lang] = len(files)
    return result


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
def series_regex(product):
    return re.compile(r'^' + re.escape(product) + r'(?:_(?P<stage>.+?))?_' + TRACK +
                      r'_v(?P<n>\d+)(?P<rc>_rc)?$')


def parse_snapshots(product, raw_snapshots):
    rx = series_regex(product)
    result = []
    for s in raw_snapshots:
        m = rx.match(s.get('name', ''))
        if m:
            result.append({'name': s['name'], 'id': s.get('id'), 'stage': m.group('stage'),
                           'n': int(m.group('n')), 'rc': bool(m.group('rc')),
                           'details': s.get('details') or {}})
    return result


def snapshot_name(product, mode, n):
    stage = MODES[mode]['stage']
    return "{}{}_{}_v{}".format(product, "_" + stage if stage else "", TRACK, n)


def stage_label(stage):
    return stage if stage else "(без этапа)"


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
def decide(modes, snaps, compare_mode, sources_state, force):
    """
    modes          — список режимов прогона, например ['PY', 'JS']
    snaps          — ob-снапшоты изделия (parse_snapshots)
    compare_mode   — f(mode, snap) -> (bool одинаковы, [причины])
    sources_state  — f(langs, [snaps]) -> True / False / None (нельзя проверить)
    Возвращает dict: iteration, upload [режимы], skip [режимы], blocked, notes
    """
    plan = {'iteration': None, 'upload': [], 'skip': [], 'blocked': None, 'notes': []}

    if not snaps:
        plan['iteration'] = 1
        plan['upload'] = list(modes)
        plan['notes'].append("ob-снапшотов на сервере нет -> v1")
        return plan

    n_max = max(s['n'] for s in snaps)
    top = [s for s in snaps if s['n'] == n_max]
    top_rc = [s for s in top if s['rc']]
    our_stages = set(MODES[m]['stage'] for m in modes)

    if top_rc:
        rc_stages = set(s['stage'] for s in top_rc)
        plan['iteration'] = n_max + 1
        plan['notes'].append("на v{} есть разметка разработчика ({}) -> новая итерация v{}".format(
            n_max, ', '.join(s['name'] for s in top_rc), n_max + 1))
        if not (rc_stages & our_stages):
            msg = ("_rc есть только для этапов [{}], а выгружаются [{}]: ресивер не перенесёт "
                   "разметку").format(', '.join(sorted(stage_label(s) for s in rc_stages)),
                                      ', '.join(sorted(stage_label(s) for s in our_stages)))
            if not force:
                plan['blocked'] = msg + " (обход: --force)"
                return plan
            plan['notes'].append("--force: " + msg)
        plan['upload'] = list(modes)
        return plan

    by_stage = {s['stage']: s for s in top}
    states = {}
    for m in modes:
        snap = by_stage.get(MODES[m]['stage'])
        if snap is None:
            states[m] = 'missing'
            continue
        same, reasons = compare_mode(m, snap)
        states[m] = 'same' if same else 'different'
        for r in reasons:
            plan['notes'].append("{} vs {}: {}".format(m, snap['name'], r))

    if any(v == 'different' for v in states.values()):
        plan['iteration'] = n_max + 1
        plan['upload'] = list(modes)
        plan['notes'].append("результаты отличаются от v{} -> новая итерация v{}".format(n_max, n_max + 1))
        return plan

    if all(v == 'same' for v in states.values()):
        plan['iteration'] = n_max
        plan['skip'] = list(modes)
        plan['notes'].append("все режимы совпадают с v{} -> дубль".format(n_max))
        return plan

    missing = [m for m in modes if states[m] == 'missing']
    langs = sorted(set(l for m in missing for l in MODES[m]['langs']))
    state = sources_state(langs, top)
    if state is True:
        plan['iteration'] = n_max
        plan['upload'] = missing
        plan['skip'] = [m for m in modes if states[m] == 'same']
        plan['notes'].append("исходники совпадают с v{} -> добавляем [{}] в v{}".format(
            n_max, ', '.join(missing), n_max))
    else:
        plan['iteration'] = n_max + 1
        plan['upload'] = list(modes)
        plan['notes'].append("режимов [{}] нет на v{}, исходники {} -> новая итерация v{}".format(
            ', '.join(missing), n_max,
            "отличаются" if state is False else "проверить нельзя (нет хэшей)", n_max + 1))
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


def process_product(product, modes, args, api, hasher, log):
    result = {'product': product, 'status': None, 'iteration': None,
              'uploaded': [], 'skipped': [], 'notes': [], 'message': ''}
    log.project_log = os.path.join(LOG_DIR, 'projects', product + '.log')
    log.raw("\n" + "=" * 70 + "\nRUN {}\n".format(datetime.now().isoformat(timespec='seconds')) + "=" * 70)
    log.info("=" * 60)
    log.info("Изделие: {}  режимы: {}".format(product, ', '.join(modes)))

    def finish(status, message=''):
        result['status'] = status
        result['message'] = message
        if message:
            (log.error if status in ('FAILED', 'BLOCKED') else log.info)("{}: {}".format(status, message))
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

    # --- Локальные результаты режимов ---
    local = {}
    active_modes = []
    for m in modes:
        if sum(hashes['counts'][l] for l in MODES[m]['langs']) == 0:
            log.info("{}: нет файлов языка — режим пропущен".format(m))
            continue
        lm = LocalMode(product, m)
        if lm.error:
            return finish('FAILED', "{}: {}".format(m, lm.error))
        newest = max(hashes['max_mtime'][l] for l in MODES[m]['langs'])
        if newest > os.path.getmtime(lm.svres) and not args.force:
            return finish('BLOCKED', "{}: исходники изменены после анализа ({}), "
                                     "перезапустите svace-analyze.py (обход: --force)".format(m, lm.svres))
        local[m] = lm
        active_modes.append(m)
        log.info("{}: {} (svace {})".format(m, lm.svres, lm.svace_version or '?'))
    if not active_modes:
        return finish('SKIPPED', "нет режимов для выгрузки")

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
        snaps = parse_snapshots(product, raw)
        log.info("Снимок сервера: {} ob-снапшотов{}".format(
            len(snaps), (": " + ', '.join(s['name'] for s in sorted(snaps, key=lambda s: (s['n'], s['name']))))
            if snaps else ""))

        warn_notes = []

        def compare_mode(mode, snap):
            lm = local[mode]
            srv_info = parse_analysis_info_text(snap['details'].get('analysis-info') or '')
            srv_ver = srv_info.get('Svace version')
            if lm.svace_version and srv_ver and lm.svace_version != srv_ver:
                return False, ["версия svace {} != {}".format(lm.svace_version, srv_ver)]
            no_hash = False
            for lang in MODES[mode]['langs']:
                for alg in HASH_ALGS:
                    name = field_name(lang, alg)
                    srv = cf_value(snap['details'], name)
                    if srv is None:
                        no_hash = True
                    elif srv != hashes[name]:
                        return False, ["{} отличается".format(name)]
            reasons = []
            if no_hash:
                warn_notes.append("{}: у {} нет хэшей исходников, сравнение только по срабатываниям".format(
                    mode, snap['name']))
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

        def sources_state(langs, top):
            for lang in langs:
                verdict = None
                for snap in top:
                    vals = [cf_value(snap['details'], field_name(lang, a)) for a in HASH_ALGS]
                    if all(vals):
                        verdict = all(v == hashes[field_name(lang, a)] for v, a in zip(vals, HASH_ALGS))
                        break
                if verdict is None:
                    return None
                if not verdict:
                    return False
            return True

        try:
            plan = decide(active_modes, snaps, compare_mode, sources_state, args.force)
        except Exception as e:
            return finish('FAILED', "ошибка сравнения: {}".format(e))

        for w in warn_notes:
            log.warn(w)
        for n in plan['notes']:
            log.info("  " + n)
        result['notes'] = warn_notes + plan['notes']
        result['iteration'] = plan['iteration']

        if plan['blocked']:
            return finish('BLOCKED', plan['blocked'])

        for m in plan['skip']:
            result['skipped'].append(snapshot_name(product, m, plan['iteration']))
            log.info("SKIP {} (дубль)".format(snapshot_name(product, m, plan['iteration'])))

        if not plan['upload']:
            return finish('SKIPPED', "дубль v{}".format(plan['iteration']))

        existing = set(s['name'] for s in snaps)
        for m in plan['upload']:
            name = snapshot_name(product, m, plan['iteration'])
            if name in existing:
                return finish('FAILED', "снапшот {} уже существует — решение противоречиво".format(name))

        if args.dry_run:
            for m in plan['upload']:
                log.info("DRY-RUN: выгрузил бы {}".format(snapshot_name(product, m, plan['iteration'])))
            result['uploaded'] = [snapshot_name(product, m, plan['iteration']) for m in plan['upload']]
            return finish('DRY-RUN')

        # --- Выгрузка ---
        for m in plan['upload']:
            name = snapshot_name(product, m, plan['iteration'])
            lm = local[m]
            store = os.path.join(lm.mode_dir, '.svacer-dir')
            if os.path.isdir(store):
                shutil.rmtree(store)  # чтобы upload отправил ровно одну запись
            cmd = [args.svacer_bin, 'import', '--project', product, '--snapshot', name]
            for lang in LANG_EXTENSIONS:
                for alg in HASH_ALGS:
                    cmd += ['--field', "{}:{}".format(field_name(lang, alg), hashes[field_name(lang, alg)])]
            cmd += ['--field', "dedup_mode:{}".format(m), lm.mode_dir]
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
            result['uploaded'].append(name)

        # --- Контроль на сервере ---
        try:
            project_id, branch_id = api.find_project(product)
            names = set(s.get('name') for s in api.snapshots(project_id, branch_id))
            lost = [n for n in result['uploaded'] if n not in names]
            if lost:
                return finish('FAILED', "после выгрузки не найдены на сервере: {}".format(', '.join(lost)))
        except Exception as e:
            log.warn("контроль после выгрузки не выполнен: {}".format(e))

        return finish('UPLOADED')
    finally:
        lock.release()


# =============================================================================
# ОСНОВНОЙ ЦИКЛ
# =============================================================================
def select_modes(args):
    if args.only_python:
        return ['PY']
    if args.only_javascript:
        return ['JS']
    if args.separate:
        return ['PY', 'JS']
    return ['OB']


def discover_products(modes):
    if not os.path.isdir(SVACE_WORK_DIR):
        return []
    return sorted(p for p in os.listdir(SVACE_WORK_DIR)
                  if any(os.path.isdir(os.path.join(SVACE_WORK_DIR, p, m, '.svace-dir')) for m in modes))


def main():
    parser = argparse.ArgumentParser(description='Выгрузка результатов Svace в Svacer с дедупликацией',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--project', metavar='NAME', action='append', default=None)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--only-python', action='store_true')
    group.add_argument('--only-javascript', '--only-js', dest='only_javascript', action='store_true')
    group.add_argument('--separate', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--host', default=DEFAULT_HOST)
    parser.add_argument('--port', default=DEFAULT_PORT)
    parser.add_argument('--user', default=DEFAULT_USER)
    parser.add_argument('--password', default=DEFAULT_PASSWORD)
    parser.add_argument('--svacer-bin', default=DEFAULT_SVACER_BIN)
    parser.add_argument('--timeout', type=int, default=300)
    args = parser.parse_args()

    modes = select_modes(args)
    for d in ('', 'projects', 'locks'):
        os.makedirs(os.path.join(LOG_DIR, d), exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log = Logger(os.path.join(LOG_DIR, "run_{}.log".format(stamp)))
    log.secrets = [args.password]

    log.info("BASE_DIR : {}".format(BASE_DIR))
    log.info("Сервер   : {}:{} (user {})".format(args.host, args.port, args.user))
    log.info("Режимы   : {}{}{}".format(', '.join(modes), "  [DRY-RUN]" if args.dry_run else "",
                                        "  [FORCE]" if args.force else ""))

    try:
        hasher = GostHasher()
        if not self_test(hasher, log):
            log.error("Самопроверка не пройдена. Выгрузка остановлена.")
            sys.exit(2)
    except Exception as e:
        log.error("Хэширование недоступно: {}".format(e))
        sys.exit(2)

    if not args.dry_run:
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

    products = args.project or discover_products(modes)
    if not products:
        log.error("Нет изделий с результатами режимов {} в {}".format(', '.join(modes), SVACE_WORK_DIR))
        sys.exit(1)
    log.info("Изделий: {} ({})".format(len(products), ', '.join(products)))

    results = []
    for product in products:
        try:
            results.append(process_product(product, modes, args, api, hasher, log))
        except Exception as e:
            log.project_log = None
            log.error("{}: непредвиденная ошибка: {}".format(product, e))
            results.append({'product': product, 'status': 'FAILED', 'message': str(e),
                            'uploaded': [], 'skipped': [], 'notes': [], 'iteration': None})

    log.info("")
    log.info("=" * 60)
    log.info("ИТОГ")
    for r in results:
        detail = ', '.join(r['uploaded']) or ', '.join(r['skipped']) or r.get('message', '')
        log.info("  {:<10} {:<24} {}".format(r['status'], r['product'], detail))

    report = os.path.join(LOG_DIR, "run_{}.json".format(stamp))
    with open(report, 'w', encoding='utf-8') as f:
        json.dump({'started': stamp, 'modes': modes, 'dry_run': args.dry_run, 'results': results},
                  f, ensure_ascii=False, indent=2)
    log.info("Отчёт: {}".format(report))

    bad = [r for r in results if r['status'] in ('FAILED', 'BLOCKED')]
    sys.exit(1 if bad else 0)


if __name__ == '__main__':
    main()
