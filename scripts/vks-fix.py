#!/usr/bin/env python3
"""
vks-fix.py — Расчёт ВКС и генерация Приложений 4-6 в формате ФИКС 2.0.2.

Ожидаемая структура (лежит в scripts/, рядом с unpack.sh и akvs-analyze.py):
    BASE_DIR/
    ├── scripts/vks-fix.py
    ├── unpacked/<PROJ>/{src,bin}
    └── results/<PROJ>/vks/
        ├── baseline_vks.json
        ├── Приложение_4_фиксация_исходного.html
        ├── Приложение_5_фиксация_конечного.html
        └── Приложение_6_сравнение.html

Примеры:
    python3 scripts/vks-fix.py
    python3 scripts/vks-fix.py --project PROJ1 --project PROJ2
    python3 scripts/vks-fix.py --mode init --project PROJ1
    python3 scripts/vks-fix.py --mode control --project PROJ1
"""

import argparse
import json
import os
import struct
import sys
from collections import defaultdict
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UNPACKED_DIR = os.path.join(BASE_DIR, "unpacked")
RESULTS_DIR = os.path.join(BASE_DIR, "results")

CHUNK_SIZE = 1024 * 1024  # 1 MB


# =============================================================================
# АЛГОРИТМ ВКС (ФИКС 2.0.2)
# =============================================================================
class VKSHasher(object):
    """Стриминговая реализация ВКС (ФИКС 2.0.2): ROL-1 + Init(+2) + Padding(4)."""

    def __init__(self):
        self._h = 0
        self._buffer = bytearray()
        self._is_first_block = True

    def update(self, data):
        self._buffer.extend(data)
        limit = (len(self._buffer) // 4) * 4
        if limit > 0:
            self._process_chunk(self._buffer[:limit])
            self._buffer = self._buffer[limit:]

    @staticmethod
    def _rol1(val):
        return ((val << 1) | (val >> 31)) & 0xFFFFFFFF

    def _process_chunk(self, data):
        for i in range(0, len(data), 4):
            self._h = self._rol1(self._h)
            val = struct.unpack("<I", data[i:i + 4])[0]
            if self._is_first_block:
                val = (val + 2) & 0xFFFFFFFF
                self._is_first_block = False
            self._h = (self._h + val) & 0xFFFFFFFF

    def hexdigest(self):
        if len(self._buffer) > 0:
            self._buffer.extend(b"\x00" * (4 - len(self._buffer)))
            self._process_chunk(self._buffer)
            self._buffer.clear()
        return struct.pack("<I", self._h).hex().upper()


def compute_file_vks(file_path):
    """Вычисляет ВКС файла потоково (для пустого файла — 00000000)."""
    size = os.path.getsize(file_path)
    if size == 0:
        return "00000000"
    hasher = VKSHasher()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


# =============================================================================
# ОБХОД ФАЙЛОВ С УЧЁТОМ ОСОБЕННОСТЕЙ UNPACK
# =============================================================================
def is_unpacked_archive(file_path):
    """True, если рядом с файлом существует каталог <file>_dir или <file>_unpack."""
    return (
        os.path.isdir(file_path + "_dir")
        or os.path.isdir(file_path + "_unpack")
    )


def clean_rel_path(rel_path):
    """Убирает суффиксы _unpack и _dir из имён каталогов в относительном пути."""
    parts = rel_path.replace("\\", "/").split("/")
    cleaned = []
    for idx, part in enumerate(parts):
        if idx < len(parts) - 1:  # только каталоги, имя самого файла не трогаем
            if part.endswith("_unpack"):
                part = part[:-len("_unpack")]
            elif part.endswith("_dir"):
                part = part[:-len("_dir")]
        cleaned.append(part)
    return "/".join(cleaned)


def scan_directory(root_dir):
    """Возвращает словарь {clean_rel_path: {h, s, m}} для всех файлов в root_dir."""
    objects = {}
    for dirpath, dirnames, filenames in os.walk(root_dir):
        dirnames[:] = sorted(
            d for d in dirnames if not os.path.islink(os.path.join(dirpath, d))
        )
        for name in sorted(filenames):
            full = os.path.join(dirpath, name)
            if os.path.islink(full) or not os.path.isfile(full):
                continue
            if is_unpacked_archive(full):
                continue
            rel = os.path.relpath(full, root_dir)
            key = clean_rel_path(rel)
            st = os.stat(full)
            objects[key] = {
                "h": compute_file_vks(full),
                "s": int(st.st_size),
                "m": int(st.st_mtime),
            }
    return objects


# =============================================================================
# ГЕНЕРАЦИЯ HTML В ФОРМАТЕ ФИКС 2.0.2 (WINDOWS-1251)
# =============================================================================
def escape_html(text):
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _sort_key(value):
    s = str(value).replace("/", "\\")
    return (s.replace("_", "{").casefold(), s)


def _compute_group_vks(hashes):
    vks_sum = 0
    has_values = False
    for h in hashes:
        if not h:
            continue
        try:
            val = struct.unpack("<I", bytes.fromhex(str(h).strip()[:8]))[0]
            vks_sum = (vks_sum + val) & 0xFFFFFFFF
            has_values = True
        except Exception:
            continue
    if not has_values:
        return "00000000"
    return struct.pack("<I", vks_sum).hex().lower()


def _fmt_ks_fix202(value):
    return value.lower()[:8].replace("e", "е")


def _fmt_dir_display(rel_dir, base_label):
    rel_norm = (rel_dir or "").replace("/", "\\").strip("\\")
    base_norm = (base_label or "").replace("/", "\\").rstrip("\\")
    if base_norm and rel_norm:
        s = "{}\\{}".format(base_norm, rel_norm)
    elif base_norm:
        s = base_norm
    else:
        s = rel_norm if rel_norm else "."
    return s if s.endswith("\\") else s + "\\"


def write_fix202_state_html(out_path, objects, base_label, subtitle="о фиксации исходного состояния"):
    """Пишет Приложение 4 или Приложение 5 в формате ФИКС 2.0.2 (windows-1251)."""
    dir_groups = defaultdict(list)
    ext_stats = defaultdict(lambda: {"count": 0, "total_size": 0})

    for rel_path, info in objects.items():
        d = os.path.dirname(rel_path).replace("\\", "/")
        dir_groups[d].append((rel_path, info))
        ext = os.path.splitext(rel_path)[1].lower() or "."
        ext_stats[ext]["count"] += 1
        ext_stats[ext]["total_size"] += int(info.get("s", 0))

    sorted_dirs = sorted(dir_groups.items(), key=lambda kv: _sort_key(kv[0]))
    sorted_exts = sorted(ext_stats.items(), key=lambda kv: (kv[0] != ".", kv[0]))

    parts = [
        "<html>",
        '<meta http-equiv="Content-Type"content="text/html;charset=windows-1251" >',
        "<head>",
        '<style type="text/css">',
        "<--",
        "table {background-color : white; font-size : 11px;}",
        ".shapka {background-color : #DFDFDF; font-weight : bold;}",
        ".itog1 {background-color : #FFEDEC; font-weight : bold;}",
        ".var {background-color : #FF0000;}",
        ".ks {font-family : Courier New,Tahoma, Verdana, Geneva, Arial, Helvetica, sans-serif;}",
        ".itog_ks1 {background-color : #FFEDEC; font-family : Courier New,Tahoma, Verdana, Geneva, Arial, Helvetica, sans-serif; font-weight : bold;}",
        ".itog_ks {background-color : #FFE0E0; font-family : Courier New,Tahoma, Verdana, Geneva, Arial, Helvetica, sans-serif; font-weight : bold;}",
        ".itog {background-color : #FFE0E0; font-weight : bold;}",
        ".ktlg {background-color : #F4F4F4; font-weight : bold;font-family : Courier New,Tahoma, Verdana, Geneva, Arial, Helvetica, sans-serif;}",
        "-->",
        "</style>",
        "<title>ФИКС 2.0.2</title>",
        "<center><caption><BIG><B>ОТЧЁТ</B></BIG></caption></center>",
        "<center><caption><BIG><B>{}</B></BIG></caption></center>".format(escape_html(subtitle)),
        "<center><caption><BIG><B>_____________________________________</B></BIG></caption></center>",
        "<center><caption><BIG><B>(ВКС)</B></BIG></caption></center>",
        "<BR>",
        '<table BORDER=2 width="100%" cellspacing="1" cellpadding="1" style= "BORDER-COLLAPSE: collapse">',
        "",
        "<tr>",
        '<td class="shapka" ALIGN=CENTER>№ пп</td>',
        '<td class="shapka" ALIGN=CENTER>Имя файла</td>',
        '<td class="shapka" ALIGN=CENTER>Дата создания</td>',
        '<td class="shapka" ALIGN=CENTER>Длина, байт</td>',
        '<td class="shapka" ALIGN=CENTER>Длина, строк</td>',
        '<td class="shapka" ALIGN=CENTER>КС</td>',
        "</tr>",
    ]

    file_counter = 0
    total_size = 0
    all_hashes = []

    for rel_dir, items in sorted_dirs:
        items_sorted = sorted(items, key=lambda x: _sort_key(os.path.basename(x[0])))
        dir_disp = _fmt_dir_display(rel_dir, base_label)
        parts.append("<tr>")
        parts.append('<td class="ktlg" COLSPAN=6 ALIGN=CENTER>Каталог {}</td>'.format(escape_html(dir_disp)))
        parts.append("</tr>")

        dir_size = 0
        dir_hashes = []
        for rel_path, info in items_sorted:
            file_counter += 1
            fname = os.path.basename(rel_path)
            mtime = info.get("m")
            fdate = datetime.fromtimestamp(mtime).strftime("%d.%m.%y %H-%M") if mtime else "-"
            fsize = int(info.get("s", 0))
            h = info.get("h", "00000000")
            dir_size += fsize
            total_size += fsize
            dir_hashes.append(h)
            all_hashes.append(h)

            parts.append("<tr>")
            parts.append("<td ALIGN=CENTER>{}</td>".format(file_counter))
            parts.append('<td class="ks" >{}</td>'.format(escape_html(fname)))
            parts.append("<td  ALIGN=CENTER>{}</td>".format(fdate))
            parts.append("<td  ALIGN=RIGHT>{}</td>".format(fsize))
            parts.append('<td  ALIGN=RIGHT>-</td>')
            parts.append('<td class="ks" ALIGN=CENTER>{}</td>'.format(_fmt_ks_fix202(h)))
            parts.append("</tr>")

        dir_vks = _fmt_ks_fix202(_compute_group_vks(dir_hashes))
        parts.append("<tr>")
        parts.append('<td class="itog1" ALIGN=CENTER COLSPAN=3 >итого: файлов - {}</td>'.format(len(items_sorted)))
        parts.append('<td class="itog1" ALIGN=RIGHT>{}</td>'.format(dir_size))
        parts.append('<td class="itog1" ALIGN=RIGHT>0</td>')
        parts.append('<td class="itog_ks1" ALIGN=CENTER>{}</td>'.format(dir_vks))
        parts.append("</tr>")

    total_vks = _fmt_ks_fix202(_compute_group_vks(all_hashes))
    parts.extend([
        "<tr>",
        '<td class="itog" COLSPAN=3 ALIGN=CENTER>ВСЕГО: файлов - {}</td>'.format(file_counter),
        '<td class="itog" ALIGN=RIGHT>{}</td>'.format(total_size),
        '<td class="itog" ALIGN=RIGHT>0</td>',
        '<td class="itog_ks" ALIGN=CENTER>{}</td>'.format(total_vks),
        "</tr>",
        "<tr>",
        '<td COLSPAN=6 ALIGN=RIGHT><I>Конец</I></td>',
        "</tr>",
        "</table>",
        "<BR>",
        "<BR>",
        '<meta http-equiv="Content-Type"content="text/html;charset=windows-1251" >',
        '<style type="text/css">',
        "<--",
        "table {background-color : white; font-size : 11px;}",
        ".shapka {background-color : #DFDFDF; font-weight : bold;}",
        ".shapka_Red {background-color : #FFE0E0; font-weight : bold;}",
        ".ext {font-family : Courier New,Tahoma, Verdana, Geneva, Arial, Helvetica, sans-serif;}",
        ".itog {background-color : #FFE0E0; font-weight : bold;}",
        ".var {background-color : #FF0000;}",
        "-->",
        "</style>",
        '<table BORDER=2 width="70%" cellspacing="1" cellpadding="1" style= "BORDER-COLLAPSE: collapse">',
        "<BR>",
        "<center><caption><BIG><B>Отчёт о расширениях файлов</B></BIG></caption></center>",
        "<BR>",
        "<BR>",
        "<tr>",
        '<td class="shapka" ALIGN=CENTER>№ пп</td>',
        '<td class="shapka" ALIGN=CENTER>Расширение</td>',
        '<td class="shapka" ALIGN=CENTER>Количество файлов</td>',
        '<td class="shapka" ALIGN=CENTER>Общая длина, байт</td>',
        '<td class="shapka" ALIGN=CENTER>Общая длина, строк</td>',
        "</tr>",
    ])

    for idx, (ext, st) in enumerate(sorted_exts, 1):
        parts.extend([
            "<tr>",
            "<td ALIGN=CENTER>{}</td>".format(idx),
            '<td class="ext" ALIGN=CENTER><B>{}</B></td>'.format(escape_html(ext)),
            "<td ALIGN=CENTER>{}</td>".format(st["count"]),
            "<td ALIGN=RIGHT>{}</td>".format(st["total_size"]),
            "<td ALIGN=CENTER>0</td>",
            "</tr>",
        ])

    parts.extend([
        "<tr>",
        "<tr>",
        '<td class="itog" ALIGN=CENTER  COLSPAN=2>Итого:</td>',
        '<td class="itog" ALIGN=CENTER>{}</td>'.format(file_counter),
        '<td class="itog" ALIGN=CENTER>{}</td>'.format(total_size),
        '<td class="itog" ALIGN=CENTER>0</td>',
        "</tr>",
        '<td  COLSPAN=5 ALIGN=RIGHT><I>Конец</I></td>',
        "</tr>",
        "</table>",
        "</body>",
        "</html>",
        "",
    ])

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as f:
        f.write("\r\n".join(parts).encode("windows-1251", errors="replace"))


def compute_diff(baseline_objs, current_objs):
    """Сравнивает исходное (baseline) и текущее состояние."""
    entries = []
    for path, b in baseline_objs.items():
        if path in current_objs:
            c = current_objs[path]
            status = "OK" if b["h"] == c["h"] else "MODIFIED"
            entries.append({
                "path": path, "status": status,
                "exp_h": b["h"], "act_h": c["h"],
                "exp_s": b["s"], "act_s": c["s"],
            })
        else:
            entries.append({
                "path": path, "status": "DELETED",
                "exp_h": b["h"], "act_h": None,
                "exp_s": b["s"], "act_s": None,
            })
    for path, c in current_objs.items():
        if path not in baseline_objs:
            entries.append({
                "path": path, "status": "NEW",
                "exp_h": None, "act_h": c["h"],
                "exp_s": None, "act_s": c["s"],
            })
    return entries


def write_fix202_control_html(out_path, diff_entries, base_label):
    """Пишет Приложение 6 (отчёт о сравнении версий) в формате ФИКС 2.0.2."""
    dir_files = defaultdict(list)
    all_dirs = set([""])

    for e in diff_entries:
        d = os.path.dirname(e["path"]).replace("\\", "/").strip("/")
        dir_files[d].append(e)
        cur = d
        while cur:
            all_dirs.add(cur)
            cur = os.path.dirname(cur).strip("/")

    style_block = [
        '<meta http-equiv="Content-Type"content="text/html;charset=windows-1251" >',
        '<style type="text/css">',
        "<--",
        "table {background-color : white; font-size : 11px;}",
        ".shapka {background-color : #DFDFDF; font-weight : bold;}",
        ".shapka_Red {background-color : #FFE0E0; font-weight : bold;}",
        ".ext {font-family : Courier New,Tahoma, Verdana, Geneva, Arial, Helvetica, sans-serif;}",
        ".itog {background-color : #FFE0E0; font-weight : bold;}",
        ".var {background-color : #FF0000;}",
        "-->",
        "</style>",
        "<title>ФИКС 2.0.2</title>",
    ]

    parts = ["<html>", "<head>"] + style_block + [
        "<center><caption><BIG><B>Результаты сравнения версий</B></BIG></caption></center>",
        '<table BORDER=2 width="100%" cellspacing="1" cellpadding="1" style= "BORDER-COLLAPSE: collapse">',
        "<BR>",
        "<tr>",
        '<td class="shapka_Red" ALIGN=CENTER COLSPAN=8 >Результаты сравнения каталогов (Ключей реестра)</td>',
        "</tr>",
        "<tr>",
        '<td class="shapka" ALIGN=CENTER>№ пп</td>',
        '<td class="shapka" ALIGN=CENTER>Признак</td>',
        '<td class="shapka" ALIGN=CENTER COLSPAN=2 >Имя каталога (Ключа)</td>',
        '<td class="shapka" ALIGN=CENTER>Количество файлов</td>',
        '<td class="shapka" ALIGN=CENTER COLSPAN=3 >Длина</td>',
        "</tr>",
    ]

    row_no = 0
    for rel_dir in sorted(all_dirs, key=lambda x: (x != "", x)):
        files = dir_files.get(rel_dir, [])
        if rel_dir and not files:
            continue
        row_no += 1
        dsize = sum(int(e["act_s"] if e["act_s"] is not None else (e["exp_s"] or 0)) for e in files)
        status_label = "Совпадает" if all(e["status"] == "OK" for e in files) else "Изменён"
        parts.extend([
            "<tr>",
            "<td ALIGN=CENTER>{}</td>".format(row_no),
            "<td ALIGN=CENTER>{}</td>".format(status_label),
            '<td ALIGN=CENTER COLSPAN=2 >{}</td>'.format(escape_html(_fmt_dir_display(rel_dir, base_label))),
            "<td ALIGN=CENTER >{}</td>".format(len(files)),
            "<td ALIGN=CENTER COLSPAN=3 >{}</td>".format(dsize),
            "</tr>",
        ])

    parts.extend([
        "<tr>",
        '<td COLSPAN=8 ALIGN=RIGHT><I>Конец</I></td>',
        "</tr>",
        "</table>",
        "<BR>",
        "<BR>",
    ])

    def render_section(title, items, kind):
        parts.extend(style_block + [
            '<table BORDER=2 width="100%" cellspacing="1" cellpadding="1" style= "BORDER-COLLAPSE: collapse">',
            "<BR>",
            "<tr>",
            '<td class="shapka_Red" ALIGN=CENTER COLSPAN=8 >{}</td>'.format(title),
            "</tr>",
            "<tr>",
            '<td class="shapka" ALIGN=CENTER>№ пп</td>',
            '<td class="shapka" ALIGN=CENTER>Имя</td>',
            '<td class="shapka" ALIGN=CENTER COLSPAN=2 >Имя каталога (Ключа)</td>',
            '<td class="shapka" ALIGN=CENTER >Количество байт</td>',
            '<td class="shapka" ALIGN=CENTER >Количество строк</td>',
            '<td class="shapka" ALIGN=CENTER COLSPAN=2 >КС</td>',
            "</tr>",
        ])
        for i, e in enumerate(items, 1):
            name = os.path.basename(e["path"])
            rdir = os.path.dirname(e["path"]).replace("\\", "/").strip("/")
            size = e["act_s"] if e["act_s"] is not None else (e["exp_s"] or 0)
            h1 = (e["exp_h"] or "-").lower()[:8] if kind in ("modified", "deleted") else "-"
            h2 = (e["act_h"] or "-").lower()[:8] if kind in ("modified", "new") else "-"
            parts.extend([
                "<tr>",
                "<td ALIGN=CENTER>{}</td>".format(i),
                "<td ALIGN=CENTER>{}</td>".format(escape_html(name)),
                '<td ALIGN=CENTER COLSPAN=2 >{}</td>'.format(escape_html(_fmt_dir_display(rdir, base_label))),
                "<td ALIGN=CENTER >{}</td>".format(int(size)),
                "<td ALIGN=CENTER >0</td>",
                "<td ALIGN=CENTER>{}</td>".format(h1),
                "<td ALIGN=CENTER>{}</td>".format(h2),
                "</tr>",
            ])
        parts.extend([
            "<tr>",
            '<td COLSPAN=8 ALIGN=RIGHT><I>Конец</I></td>',
            "</tr>",
            "</table>",
            "<BR>",
            "<BR>",
        ])

    render_section("Измененные файлы (значения ключей реестра)",
                   [e for e in diff_entries if e["status"] == "MODIFIED"], "modified")
    render_section("Добавленые файлы (значения ключей реестра)",
                   [e for e in diff_entries if e["status"] == "NEW"], "new")
    render_section("Отсутствующие файлы (значения ключей реестра)",
                   [e for e in diff_entries if e["status"] == "DELETED"], "deleted")

    ext_stats = defaultdict(lambda: {"unch": 0, "mod": 0, "add": 0, "del": 0})
    for e in diff_entries:
        suf = os.path.splitext(e["path"])[1].upper()
        ext = suf[1:] if suf.startswith(".") else (suf or ".")
        st = e["status"]
        if st == "OK":
            ext_stats[ext]["unch"] += 1
        elif st == "MODIFIED":
            ext_stats[ext]["mod"] += 1
        elif st == "NEW":
            ext_stats[ext]["add"] += 1
        elif st == "DELETED":
            ext_stats[ext]["del"] += 1

    def ext_sort(k):
        if k == "MD5":
            return (0, "")
        if k == ".":
            return (1, "")
        return (2, k)

    parts.extend(style_block + [
        '<table BORDER=2 width="60%" cellspacing="1" cellpadding="1" style= "BORDER-COLLAPSE: collapse">',
        "<BR>",
        "<tr>",
        '<td class="shapka_Red" ALIGN=CENTER COLSPAN=8 >Отчет по расширениям</td>',
        "</tr>",
        "<tr>",
        '<td class="shapka" ALIGN=CENTER>№ пп</td>',
        '<td class="shapka" ALIGN=CENTER>Расширение</td>',
        '<td class="shapka" ALIGN=CENTER>Неизмененных</td>',
        '<td class="shapka" ALIGN=CENTER >Измененных</td>',
        '<td class="shapka" ALIGN=CENTER >Добавленных</td>',
        '<td class="shapka" ALIGN=CENTER >Отсутствующих</td>',
        '<td class="shapka" ALIGN=CENTER >Дублей</td>',
        '<td class="shapka" ALIGN=CENTER >Сумма</td>',
        "</tr>",
    ])

    t_unch = t_mod = t_add = t_del = t_sum = 0
    ext_keys = sorted(ext_stats.keys(), key=ext_sort)
    for idx, ext in enumerate(ext_keys, 1):
        s = ext_stats[ext]
        row_sum = s["unch"] + s["mod"] + s["add"] + s["del"]
        t_unch += s["unch"]
        t_mod += s["mod"]
        t_add += s["add"]
        t_del += s["del"]
        t_sum += row_sum
        parts.extend([
            "<tr>",
            "<td ALIGN=CENTER>{}</td>".format(idx),
            "<td ALIGN=CENTER>{}</td>".format(escape_html(ext)),
            "<td ALIGN=LEFT>{}</td>".format(s["unch"]),
            "<td ALIGN=CENTER>{}</td>".format(s["mod"]),
            "<td ALIGN=CENTER>{}</td>".format(s["add"]),
            "<td ALIGN=CENTER>{}</td>".format(s["del"]),
            "<td ALIGN=CENTER>0</td>",
            "<td ALIGN=CENTER>{}</td>".format(row_sum),
            "</tr>",
        ])

    parts.extend([
        "<tr>",
        "<td ALIGN=CENTER>{}</td>".format(len(ext_keys) + 1),
        "<td ALIGN=CENTER>сумм</td>",
        "<td ALIGN=LEFT>{}</td>".format(t_unch),
        "<td ALIGN=CENTER>{}</td>".format(t_mod),
        "<td ALIGN=CENTER>{}</td>".format(t_add),
        "<td ALIGN=CENTER>{}</td>".format(t_del),
        "<td ALIGN=CENTER>0</td>",
        "<td ALIGN=CENTER>{}</td>".format(t_sum),
        "</tr>",
        "<tr>",
        '<td COLSPAN=8 ALIGN=RIGHT><I>Конец</I></td>',
        "</tr>",
        "</table>",
        "</body>",
        "</html>",
    ])

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as f:
        f.write("\n".join(parts).encode("windows-1251", errors="replace"))


# =============================================================================
# ОСНОВНОЙ ЦИКЛ
# =============================================================================
def process_project(project, mode, target):
    proj_root = os.path.join(UNPACKED_DIR, project)
    scan_root = proj_root if target == "all" else os.path.join(proj_root, target)
    if not os.path.isdir(scan_root):
        print("  [SKIP] Не найден каталог: {}".format(scan_root))
        return False

    out_dir = os.path.join(RESULTS_DIR, project, "vks")
    os.makedirs(out_dir, exist_ok=True)

    baseline_json = os.path.join(out_dir, "baseline_vks.json")
    app4_path = os.path.join(out_dir, "Приложение_4_фиксация_исходного.html")
    app5_path = os.path.join(out_dir, "Приложение_5_фиксация_конечного.html")
    app6_path = os.path.join(out_dir, "Приложение_6_сравнение.html")

    base_label = project if target == "all" else os.path.join(project, target)
    print("\n[Проект: {}] Сканирование {} ...".format(project, scan_root))
    current_objs = scan_directory(scan_root)
    print("  Файлов обработано (ВКС): {}".format(len(current_objs)))

    if mode == "init":
        with open(baseline_json, "w", encoding="utf-8") as f:
            json.dump({"project": project, "target": target, "objects": current_objs}, f, ensure_ascii=False, indent=2)
        write_fix202_state_html(app4_path, current_objs, base_label, "о фиксации исходного состояния")
        print("  [OK] Приложение 4: {}".format(app4_path))
        return True

    if mode == "control":
        if not os.path.isfile(baseline_json):
            print("  [ERROR] Нет эталона {}. Сначала запустите --mode init".format(baseline_json))
            return False
        with open(baseline_json, "r", encoding="utf-8") as f:
            baseline_objs = json.load(f).get("objects", {})
        write_fix202_state_html(app5_path, current_objs, base_label, "о фиксации конечного состояния")
        diff_entries = compute_diff(baseline_objs, current_objs)
        write_fix202_control_html(app6_path, diff_entries, base_label)
        print("  [OK] Приложение 5: {}".format(app5_path))
        print("  [OK] Приложение 6: {}".format(app6_path))
        return True

    # mode == "all"
    if os.path.isfile(baseline_json):
        with open(baseline_json, "r", encoding="utf-8") as f:
            baseline_objs = json.load(f).get("objects", {})
        if not os.path.isfile(app4_path):
            write_fix202_state_html(app4_path, baseline_objs, base_label, "о фиксации исходного состояния")
    else:
        baseline_objs = current_objs
        with open(baseline_json, "w", encoding="utf-8") as f:
            json.dump({"project": project, "target": target, "objects": baseline_objs}, f, ensure_ascii=False, indent=2)
        write_fix202_state_html(app4_path, baseline_objs, base_label, "о фиксации исходного состояния")

    write_fix202_state_html(app5_path, current_objs, base_label, "о фиксации конечного состояния")
    diff_entries = compute_diff(baseline_objs, current_objs)
    write_fix202_control_html(app6_path, diff_entries, base_label)
    print("  [OK] Приложение 4: {}".format(app4_path))
    print("  [OK] Приложение 5: {}".format(app5_path))
    print("  [OK] Приложение 6: {}".format(app6_path))
    return True


def main():
    parser = argparse.ArgumentParser(
        description="ВКС (ФИКС 2.0.2) — Приложения 4-6 по unpacked/<PROJ>/bin (по умолчанию)"
    )
    parser.add_argument("--project", action="append", default=None,
                        help="Имя проекта (можно указать несколько раз; по умолчанию все из unpacked/)")
    parser.add_argument("--mode", choices=["all", "init", "control"], default="all",
                        help="all: 4+5+6; init: только 4 + baseline; control: 5+6 по baseline")
    parser.add_argument("--target", choices=["bin", "src"], default="bin",
                        help="Подкаталог внутри unpacked/<PROJ>/ (по умолчанию: bin)")
    parser.add_argument("--all", dest="scan_all", action="store_true",
                        help="Фиксировать весь unpacked/<PROJ>/ целиком вместо только bin")
    args = parser.parse_args()

    target = "all" if args.scan_all else args.target

    if not os.path.isdir(UNPACKED_DIR):
        print("[ERROR] Не найдена директория: {}".format(UNPACKED_DIR))
        sys.exit(1)

    projects = args.project or sorted(
        d for d in os.listdir(UNPACKED_DIR)
        if os.path.isdir(os.path.join(UNPACKED_DIR, d))
    )
    if not projects:
        print("[INFO] Проектов в {} не найдено.".format(UNPACKED_DIR))
        sys.exit(1)

    ok_cnt = 0
    for proj in projects:
        if process_project(proj, args.mode, target):
            ok_cnt += 1

    print("\nГотово. Успешно проектов: {}/{}".format(ok_cnt, len(projects)))


if __name__ == "__main__":
    main()