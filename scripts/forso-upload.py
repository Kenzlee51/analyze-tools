#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
forso-upload.py — Выгрузка результатов на сетевой диск (CIFS/SMB)
=============================================================================

ОПИСАНИЕ:
    Монтирует сетевой диск //SERVER/SHARE, находит на его ВЕРХНЕМ уровне
    папку проекта по номеру АДЛБ.ХХХ (или по полному имени, флаг --dir),
    создаёт при необходимости <проект>/Отчетность/НДВ/Приложение АКВС и
    копирует туда содержимое results/summary/akvs.

    Копирование инкрементальное, по хэшам:
        - для каждой подпапки (и файла верхнего уровня) из summary/akvs
          считается sha256 всего содержимого;
        - на шаре в <проект>/Отчетность/НДВ/hash.txt хранится карта
          «имя -> хэш» с прошлой выгрузки;
        - совпал хэш и папка на месте -> ПРОПУСК;
        - хэш другой / записи нет / папки нет -> папка на шаре удаляется
          и заливается заново целиком;
        - hash.txt перезаписывается после копирования (дозапись/обновление
          только изменившихся записей, остальные сохраняются).

    Остановка с размонтированием диска происходит, если:
        - папка проекта по номеру ХХХ не найдена;
        - найдено НЕСКОЛЬКО подходящих папок (все выводятся списком —
          нужную укажите флагом --dir "полное имя").

    Если диск уже был смонтирован в точку монтирования до запуска —
    скрипт с ним работает, но НЕ размонтирует его в конце.

ИСПОЛЬЗОВАНИЕ:
    python3 scripts/forso-upload.py --adlb 123
    python3 scripts/forso-upload.py --dir "АДЛБ.123 Изделие Х"
    python3 scripts/forso-upload.py --adlb 123 --dry-run
    python3 scripts/forso-upload.py --adlb 123 --force
    sudo -v && python3 scripts/forso-upload.py --adlb 123   # чтобы sudo не спрашивал пароль в процессе

ОПЦИИ:
    --adlb XXX          Номер проекта: РОВНО 3 цифры (по умолчанию так).
                        Больше/меньше цифр — только через --adlb-any.
    --adlb-any          Разрешить номер произвольной длины (например 01342).
    --dir NAME          Полное имя папки проекта на шаре (в обход поиска
                        по номеру). Взаимоисключающе с --adlb.
    --server VALUE      Адрес сервера           [см. SERVER]
    --share VALUE       Имя шары                [см. SHARE]
    --user VALUE        Логин                   [см. LOGIN]
    --password VALUE    Пароль                  [см. PASS]
    --ask-password      Спросить пароль интерактивно (не светится в ps)
    --mountpoint PATH   Точка монтирования      [см. MOUNTPOINT]
    --vers VALUE        Версия SMB (default: 3.0, при ошибке пробуем без неё)
    --source PATH       Что заливать (default: results/summary/akvs)
    --appdir NAME       Имя папки-приёмника     [см. APP_SUBDIR]
    --force             Заливать всё заново, игнорируя hash.txt
    --dry-run           Ничего не менять на шаре, только показать план
    --keep-mounted      Не размонтировать диск в конце
    -h, --help          Показать справку

ЗАВИСИМОСТИ:
    Python 3.5+, cifs-utils (sudo apt install cifs-utils), право на sudo
    для mount/umount.
=============================================================================
"""

import os
import re
import sys
import shutil
import getpass
import hashlib
import argparse
import subprocess
from datetime import datetime

# =============================================================================
# ГЛОБАЛЬНЫЕ НАСТРОЙКИ (меняются здесь; каждую можно переопределить флагом)
# =============================================================================
SERVER     = "192.168.25.230"        # адрес файлового сервера   (--server)
SHARE      = "Сертификация"          # имя сетевой шары          (--share)
LOGIN      = "kirill_v"              # логин                     (--user)
PASS       = "komusfanvil"           # пароль                    (--password)
MOUNTPOINT = "/mnt/sertifikaciya"    # точка монтирования        (--mountpoint)
SMB_VERS   = "3.0"                   # версия протокола SMB      (--vers)

REPORT_SUBPATH = ["Отчетность", "НДВ"]   # путь внутри папки проекта
APP_SUBDIR     = "Приложение АКВС"       # папка-приёмник внутри НДВ (--appdir)
HASH_FILE      = "hash.txt"              # карта хэшей, лежит в .../НДВ/

# =============================================================================

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SRC = os.path.join(BASE_DIR, "results", "summary", "akvs")


# =============================================================================
# ВЫВОД
# =============================================================================
def info(msg):
    print("    {}".format(msg))


def warn(msg):
    print("[ВНИМАНИЕ] {}".format(msg))


def err(msg):
    print("[ОШИБКА] {}".format(msg))


def _mask(cmd):
    """Прячем пароль в отображаемой команде."""
    shown = []
    for tok in cmd:
        if "password=" in tok:
            tok = re.sub(r'password=[^,]*', 'password=***', tok)
        shown.append(tok)
    return ' '.join(shown)


def run(cmd, quiet=False):
    """Запуск команды. Возвращает (ok, stdout, stderr)."""
    if not quiet:
        info("Запуск: {}".format(_mask(cmd)))
    try:
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError:
        return False, "", "исполняемый файл не найден: {}".format(cmd[0])
    except Exception as e:
        return False, "", str(e)
    out = res.stdout.decode('utf-8', errors='replace')
    errout = res.stderr.decode('utf-8', errors='replace')
    return res.returncode == 0, out, errout


# =============================================================================
# МОНТИРОВАНИЕ
# =============================================================================
def is_mounted(path):
    """True, если path — точка монтирования."""
    path = os.path.abspath(path)
    try:
        with open('/proc/mounts', encoding='utf-8', errors='replace') as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    # в /proc/mounts пробелы закодированы как \040
                    mp = parts[1].replace('\\040', ' ')
                    if os.path.abspath(mp) == path:
                        return True
    except Exception:
        pass
    return os.path.ismount(path)


def mount_share(cfg):
    """Монтирует шару. Возвращает True, если МЫ её смонтировали
    (значит, нам же её и размонтировать)."""
    mp = cfg['mountpoint']
    unc = "//{}/{}".format(cfg['server'], cfg['share'])

    if is_mounted(mp):
        info("Диск уже смонтирован в {} — использую как есть "
             "(размонтировать в конце не буду)".format(mp))
        return False

    if not os.path.isdir(mp):
        info("Создаю точку монтирования: {}".format(mp))
        ok, _, e = run(['sudo', 'mkdir', '-p', mp])
        if not ok:
            err("не удалось создать {}: {}".format(mp, e.strip()))
            sys.exit(1)

    uid = os.getuid()
    gid = os.getgid()
    base_opts = ("username={},password={},iocharset=utf8,"
                 "uid={},gid={},file_mode=0664,dir_mode=0775").format(
                     cfg['user'], cfg['password'], uid, gid)

    attempts = []
    if cfg['vers']:
        attempts.append(base_opts + ",vers={}".format(cfg['vers']))
    attempts.append(base_opts)          # без vers — пусть договорятся сами

    last_err = ""
    for opts in attempts:
        ok, _, e = run(['sudo', 'mount', '-t', 'cifs', '-o', opts, unc, mp])
        if ok:
            info("Смонтировано: {} -> {}".format(unc, mp))
            return True
        last_err = e.strip()
        info("Попытка монтирования не удалась: {}".format(last_err))

    err("не удалось смонтировать {} в {}:\n       {}".format(unc, mp, last_err))
    print("       Проверьте: установлен ли cifs-utils, доступен ли сервер,")
    print("       верны ли логин/пароль, не занята ли точка монтирования.")
    sys.exit(1)


def umount_share(mp, mounted_by_us, keep_mounted):
    if keep_mounted:
        info("--keep-mounted: диск оставлен смонтированным в {}".format(mp))
        return
    if not mounted_by_us:
        return
    if not is_mounted(mp):
        return
    ok, _, e = run(['sudo', 'umount', mp])
    if ok:
        info("Диск размонтирован: {}".format(mp))
        return
    info("umount не прошёл ({}) — пробую отложенное размонтирование".format(e.strip()))
    ok, _, e = run(['sudo', 'umount', '-l', mp])
    if ok:
        info("Диск размонтирован (lazy): {}".format(mp))
    else:
        warn("НЕ УДАЛОСЬ размонтировать {}: {}".format(mp, e.strip()))
        warn("Размонтируйте вручную: sudo umount {}".format(mp))


# =============================================================================
# ПОИСК ПАПКИ ПРОЕКТА
# =============================================================================
def find_project_dir(root, adlb):
    """Папки верхнего уровня, в имени которых есть число adlb.

    Номер ищется как отдельная группа цифр (123 не совпадёт с 1234 и 0123),
    регистр и окружающий текст значения не имеют: 'АДЛБ.123', 'АДЛБ.123-01',
    '123 Изделие' — совпадут.
    """
    pattern = re.compile(r'(?<!\d){}(?!\d)'.format(re.escape(adlb)))
    matches = []
    for name in sorted(os.listdir(root)):
        if not os.path.isdir(os.path.join(root, name)):
            continue
        if pattern.search(name):
            matches.append(name)
    return matches


# =============================================================================
# ХЭШИ
# =============================================================================
def hash_path(path):
    """sha256 содержимого файла или каталога (рекурсивно, детерминированно).

    В хэш идут относительные пути (в порядке сортировки), размеры и байты
    файлов — переименование или изменение любого файла меняет хэш.
    """
    h = hashlib.sha256()
    if os.path.isfile(path):
        h.update(b'F\x00' + os.path.basename(path).encode('utf-8') + b'\x00')
        _feed_file(h, path)
        return h.hexdigest()

    entries = []
    for dirpath, dirnames, filenames in os.walk(path):
        dirnames.sort()
        for name in sorted(filenames):
            full = os.path.join(dirpath, name)
            if os.path.islink(full) or not os.path.isfile(full):
                continue
            entries.append((os.path.relpath(full, path).replace(os.sep, '/'), full))
    entries.sort(key=lambda x: x[0])
    for rel, full in entries:
        h.update(rel.encode('utf-8') + b'\x00')
        h.update(str(os.path.getsize(full)).encode('ascii') + b'\x00')
        _feed_file(h, full)
    return h.hexdigest()


def _feed_file(h, path):
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)


def read_hash_file(path):
    """Читает hash.txt -> dict {имя: хэш}. Битые/чужие строки игнорируются."""
    table = {}
    if not os.path.isfile(path):
        return table
    try:
        with open(path, encoding='utf-8', errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split(None, 1)      # хэш<пробел>имя
                if len(parts) != 2:
                    continue
                digest, name = parts[0].strip(), parts[1].strip()
                if re.fullmatch(r'[0-9a-f]{64}', digest):
                    table[name] = digest
    except Exception as e:
        warn("не удалось прочитать {}: {} — считаю, что хэшей нет".format(path, e))
    return table


def write_hash_file(path, table):
    """Пишет hash.txt (сначала во временный файл рядом, затем подменяет)."""
    tmp = path + ".tmp"
    lines = ["# forso-upload.py — хэши выгруженных отчётов АК-ВС",
             "# обновлено: {}".format(datetime.now().isoformat(timespec='seconds')),
             "# формат: <sha256> <имя папки/файла>"]
    for name in sorted(table):
        lines.append("{} {}".format(table[name], name))
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    if os.path.exists(path):
        os.remove(path)
    os.rename(tmp, path)


# =============================================================================
# КОПИРОВАНИЕ
# =============================================================================
def copy_item(src, dst):
    """Полная замена: приёмник сносится и заливается заново."""
    if os.path.isdir(dst):
        shutil.rmtree(dst)
    elif os.path.exists(dst):
        os.remove(dst)
    parent = os.path.dirname(dst)
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)
    if os.path.isdir(src):
        shutil.copytree(src, dst)
    else:
        shutil.copy2(src, dst)


def ensure_dir(path, dry_run):
    if os.path.isdir(path):
        return True
    if dry_run:
        info("[dry-run] создал бы: {}".format(path))
        return True
    try:
        os.makedirs(path, exist_ok=True)
        info("Создана папка: {}".format(path))
        return True
    except Exception as e:
        err("не удалось создать {}: {}".format(path, e))
        return False


# =============================================================================
# ОСНОВНАЯ РАБОТА (после монтирования)
# =============================================================================
def do_upload(cfg, args):
    mp = cfg['mountpoint']
    src_root = cfg['source']

    # --- 1. Папка проекта на шаре ---
    if args.dir:
        project_dir_name = args.dir
        full = os.path.join(mp, project_dir_name)
        if not os.path.isdir(full):
            err("папка проекта не найдена на шаре: {}".format(full))
            return 1
        info("Папка проекта (--dir): {}".format(project_dir_name))
    else:
        matches = find_project_dir(mp, args.adlb)
        if not matches:
            err("на шаре //{}/{} не найдена папка проекта с номером {}".format(
                cfg['server'], cfg['share'], args.adlb))
            print("       Проверьте номер или укажите папку явно: "
                  "--dir \"полное имя папки\"")
            return 1
        if len(matches) > 1:
            print("")
            warn("номеру {} соответствует НЕСКОЛЬКО папок на шаре:".format(args.adlb))
            for m in matches:
                print("  - {}".format(m))
            print("Укажите нужную явно: --dir \"{}\"".format(matches[0]))
            return 1
        project_dir_name = matches[0]
        info("Папка проекта найдена: {}".format(project_dir_name))

    project_path = os.path.join(mp, project_dir_name)

    # --- 2. Отчетность/НДВ/Приложение АКВС ---
    ndv_path = project_path
    for part in REPORT_SUBPATH:
        ndv_path = os.path.join(ndv_path, part)
        if not ensure_dir(ndv_path, args.dry_run):
            return 1
    app_path = os.path.join(ndv_path, cfg['appdir'])
    if not ensure_dir(app_path, args.dry_run):
        return 1
    info("Приёмник      : {}".format(app_path))

    # --- 3. Что заливаем ---
    items = sorted(os.listdir(src_root))
    items = [i for i in items if not i.startswith('.')]
    if not items:
        err("нечего заливать: {} пуста".format(src_root))
        return 1

    hash_path_remote = os.path.join(ndv_path, HASH_FILE)
    remote_hashes = {} if args.force else read_hash_file(hash_path_remote)
    if args.force:
        info("--force: hash.txt игнорируется, заливаю всё заново")
    elif remote_hashes:
        info("Прочитан {}: записей={}".format(hash_path_remote, len(remote_hashes)))
    else:
        info("{} на шаре нет — считаю, что выгрузки ещё не было".format(HASH_FILE))

    # --- 4. Хэши локальных отчётов ---
    info("Считаю хэши {} элементов в {} ...".format(len(items), src_root))
    local_hashes = {}
    for name in items:
        local_hashes[name] = hash_path(os.path.join(src_root, name))

    # --- 5. Сравнение и копирование ---
    copied, skipped, failed = [], [], []
    print("")
    for name in items:
        src = os.path.join(src_root, name)
        dst = os.path.join(app_path, name)
        same_hash = remote_hashes.get(name) == local_hashes[name]
        exists = os.path.exists(dst)

        if same_hash and exists:
            print("  = пропуск   : {} (хэш совпал)".format(name))
            skipped.append(name)
            continue

        reason = "нет на шаре" if not exists else (
            "изменился" if name in remote_hashes else "нет записи о хэше")
        if args.dry_run:
            print("  [dry-run] залил бы: {} ({})".format(name, reason))
            copied.append(name)
            continue
        try:
            copy_item(src, dst)
            print("  + залито    : {} ({})".format(name, reason))
            copied.append(name)
        except Exception as e:
            print("  ! ошибка    : {} — {}".format(name, e))
            failed.append(name)

    # --- 6. Обновление hash.txt (только по успешно залитым + уцелевшим) ---
    new_table = dict(remote_hashes) if not args.force else {}
    for name in copied:
        if name not in failed:
            new_table[name] = local_hashes[name]
    for name in skipped:
        new_table[name] = local_hashes[name]
    # чистим записи о том, чего на шаре уже нет
    if not args.dry_run:
        for name in list(new_table):
            if not os.path.exists(os.path.join(app_path, name)):
                del new_table[name]

    if args.dry_run:
        info("[dry-run] обновил бы {} ({} записей)".format(
            hash_path_remote, len(new_table)))
    else:
        try:
            write_hash_file(hash_path_remote, new_table)
            info("Обновлён {} ({} записей)".format(hash_path_remote, len(new_table)))
        except Exception as e:
            err("не удалось записать {}: {}".format(hash_path_remote, e))
            failed.append(HASH_FILE)

    # --- 7. Итог ---
    print("\n{}".format("=" * 60))
    print("Выгрузка завершена{}".format(" (dry-run)" if args.dry_run else "!"))
    print("Проект на шаре : {}".format(project_dir_name))
    print("Приёмник       : {}".format(
        os.path.join(project_dir_name, *(REPORT_SUBPATH + [cfg['appdir']]))))
    print("Залито         : {}".format(len(copied)))
    print("Пропущено      : {}".format(len(skipped)))
    print("Ошибок         : {}".format(len(failed)))
    if failed:
        print("\nНе удалось залить:")
        for name in failed:
            print("  - {}".format(name))
    return 1 if failed else 0


# =============================================================================
# MAIN
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Выгрузка отчётов АК-ВС (results/summary/akvs) на сетевой "
                    "диск в <проект>/Отчетность/НДВ/Приложение АКВС",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Подробности см. в шапке файла.",
    )
    parser.add_argument('--adlb', metavar='XXX',
                        help='Номер проекта АДЛБ.ХХХ (ровно 3 цифры)')
    parser.add_argument('--adlb-any', action='store_true',
                        help='Разрешить номер произвольной длины')
    parser.add_argument('--dir', metavar='NAME',
                        help='Полное имя папки проекта на шаре (вместо --adlb)')
    parser.add_argument('--server',     default=SERVER,     help='Адрес сервера (default: {})'.format(SERVER))
    parser.add_argument('--share',      default=SHARE,      help='Имя шары (default: {})'.format(SHARE))
    parser.add_argument('--user',       default=LOGIN,      help='Логин (default: {})'.format(LOGIN))
    parser.add_argument('--password',   default=PASS,       help='Пароль')
    parser.add_argument('--ask-password', action='store_true', help='Спросить пароль интерактивно')
    parser.add_argument('--mountpoint', default=MOUNTPOINT, help='Точка монтирования (default: {})'.format(MOUNTPOINT))
    parser.add_argument('--vers',       default=SMB_VERS,   help='Версия SMB (default: {})'.format(SMB_VERS))
    parser.add_argument('--source',     default=DEFAULT_SRC, help='Источник (default: results/summary/akvs)')
    parser.add_argument('--appdir',     default=APP_SUBDIR, help='Папка-приёмник (default: {})'.format(APP_SUBDIR))
    parser.add_argument('--force',        action='store_true', help='Залить всё заново, игнорируя hash.txt')
    parser.add_argument('--dry-run',      action='store_true', help='Ничего не менять, только показать план')
    parser.add_argument('--keep-mounted', action='store_true', help='Не размонтировать диск в конце')
    args = parser.parse_args()

    # --- Валидация выбора проекта ---
    if bool(args.adlb) == bool(args.dir):
        err("укажите РОВНО одно: --adlb XXX или --dir \"полное имя папки\"")
        sys.exit(1)
    if args.adlb:
        if not args.adlb.isdigit():
            err("--adlb должен состоять только из цифр (получено: {!r})".format(args.adlb))
            sys.exit(1)
        if len(args.adlb) != 3 and not args.adlb_any:
            err("--adlb ожидает ровно 3 цифры (получено {} — {!r}). "
                "Для другой длины добавьте --adlb-any".format(len(args.adlb), args.adlb))
            sys.exit(1)

    src_root = os.path.abspath(args.source)
    if not os.path.isdir(src_root):
        err("источник не найден: {}".format(src_root))
        print("       Сначала выполните прогон: python3 scripts/akvs-analyze.py")
        sys.exit(1)

    password = args.password
    if args.ask_password:
        password = getpass.getpass("Пароль для {}: ".format(args.user))

    cfg = {
        'server': args.server, 'share': args.share,
        'user': args.user, 'password': password,
        'mountpoint': os.path.abspath(args.mountpoint),
        'vers': args.vers, 'source': src_root, 'appdir': args.appdir,
    }

    print("AK-VS upload")
    print("Источник   : {}".format(cfg['source']))
    print("Шара       : //{}/{}".format(cfg['server'], cfg['share']))
    print("Монтирую в : {}".format(cfg['mountpoint']))
    print("Проект     : {}".format(
        args.dir if args.dir else "по номеру АДЛБ.{}".format(args.adlb)))
    if args.dry_run:
        print("Режим      : dry-run (на шаре ничего не меняется)")

    mounted_by_us = mount_share(cfg)
    rc = 1
    try:
        rc = do_upload(cfg, args)
    except KeyboardInterrupt:
        print("")
        err("прервано пользователем")
        rc = 1
    except Exception as e:
        err("непредвиденная ошибка: {}".format(e))
        rc = 1
    finally:
        umount_share(cfg['mountpoint'], mounted_by_us, args.keep_mounted)

    sys.exit(rc)


if __name__ == '__main__':
    main()
