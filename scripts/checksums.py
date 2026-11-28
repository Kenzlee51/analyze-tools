#!/usr/bin/env python3
"""
checksums.py — КС дистрибутива по origin_bin и исходных текстов по src.

Особенности:
  - Отдельные файлы для bin и src в checksums/bin/ и checksums/src/.
  - Для rhash -G и gostsum имя файла содержит gostsum.
  - Выбор системной утилиты параметром (--tool) или в интерактивном диалоге.
  - Пропуск архивов, которые были распакованы (рядом есть <file>_dir / <file>_unpack).
  - Суффиксы _unpack и _dir удаляются из путей в отчёте.

Примеры:
    python3 scripts/checksums.py
    python3 scripts/checksums.py --tool "rhash -G"
    python3 scripts/checksums.py --tool gostsum --project PROJ1
"""

import argparse
import os
import shlex
import shutil
import subprocess
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UNPACKED_DIR = os.path.join(BASE_DIR, "unpacked")
RESULTS_DIR = os.path.join(BASE_DIR, "results")

PRESET_TOOLS = [
    ("md5sum",   "md5sum"),
    ("gostsum",  "gostsum 12 256"),
    ("cksum",    "cksum"),
    ("rhash -G", "rhash -G (ГОСТ-12 256)"),
    ("sha256sum", "sha256sum"),
]


def choose_tool_interactive():
    print("\nВыберите утилиту для расчёта контрольных сумм:")
    for idx, (cmd, label) in enumerate(PRESET_TOOLS, 1):
        exe = shlex.split(cmd)[0]
        avail = "доступна" if shutil.which(exe) else "НЕ найдена"
        print("  {}. {:<20} [{}]".format(idx, label, avail))
    print("  0. Ввести свою команду")

    while True:
        choice = input("\nВаш выбор [1-{} / 0]: ".format(len(PRESET_TOOLS))).strip()
        if choice == "0":
            custom = input("Введите команду (например, 'rhash --gost12-256'): ").strip()
            if custom:
                return custom
        elif choice.isdigit() and 1 <= int(choice) <= len(PRESET_TOOLS):
            return PRESET_TOOLS[int(choice) - 1][0]
        print("Некорректный ввод, повторите.")


def is_unpacked_archive(file_path):
    return (
        os.path.isdir(file_path + "_dir")
        or os.path.isdir(file_path + "_unpack")
    )


def clean_rel_path(rel_path):
    parts = rel_path.replace("\\", "/").split("/")
    cleaned = []
    for idx, part in enumerate(parts):
        if idx < len(parts) - 1:
            if part.endswith("_unpack"):
                part = part[:-len("_unpack")]
            elif part.endswith("_dir"):
                part = part[:-len("_dir")]
        cleaned.append(part)
    return "/".join(cleaned)


def collect_files(target_dir):
    files = []
    for dirpath, dirnames, filenames in os.walk(target_dir):
        dirnames[:] = sorted(
            d for d in dirnames if not os.path.islink(os.path.join(dirpath, d))
        )
        for name in sorted(filenames):
            full = os.path.join(dirpath, name)
            if os.path.islink(full) or not os.path.isfile(full):
                continue
            if is_unpacked_archive(full):
                continue
            files.append(full)
    return files


def calc_checksum(cmd_parts, file_path, root_dir):
    """Запускает системную утилиту и возвращает строку '<КС>  <очищенный_путь>'."""
    res = subprocess.run(
        cmd_parts + [file_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if res.returncode != 0:
        err = res.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError("Ошибка {}: {}".format(" ".join(cmd_parts), err))

    line = res.stdout.decode("utf-8", errors="replace").strip().splitlines()[0]
    rel_clean = clean_rel_path(os.path.relpath(file_path, root_dir))
    # Linux может хранить имена с байтами, не являющимися UTF-8.
    # В отчёте представляем такие байты как \\xNN; путь для расчёта КС
    # остаётся исходным, корректные UTF-8 имена не изменяются.
    rel_clean = rel_clean.encode("utf-8", errors="surrogateescape").decode(
        "utf-8", errors="backslashreplace")

    # cksum выводит: "<crc> <bytes> <path>"
    exe_name = os.path.basename(cmd_parts[0]).lower()
    tokens = line.split()
    if exe_name == "cksum" and len(tokens) >= 2:
        return "{} {}  {}".format(tokens[0], tokens[1], rel_clean)

    # md5sum / gostsum / sha256sum / rhash -G выводят: "<hash>  <path>"
    checksum = tokens[0] if tokens else line
    return "{}  {}".format(checksum, rel_clean)


def safe_tool_tag(tool_cmd):
    return "_".join(shlex.split(tool_cmd)).replace("-", "").replace("/", "_")


def report_tool_tag(tool_cmd):
    """Название алгоритма для файла отчёта (rhash -G -> gostsum)."""
    parts = shlex.split(tool_cmd)
    exe = os.path.basename(parts[0]).lower()
    if exe == "gostsum":
        return "gostsum"
    if exe == "rhash" and ("-G" in parts[1:] or "--gost12-256" in parts[1:]):
        return "gostsum"
    return safe_tool_tag(tool_cmd)


def process_subdir(project, subdir_name, cmd_parts, tool_tag):
    # Приложение 1 — origin_bin, Приложение 2 — src.
    # Имя выходного каталога Приложения 1 остаётся checksums/bin.
    source_subdir = "origin_bin" if subdir_name == "bin" else subdir_name
    target_dir = os.path.join(UNPACKED_DIR, project, source_subdir)
    if not os.path.isdir(target_dir):
        print("  [SKIP] Нет каталога: {}".format(target_dir))
        return None

    files = collect_files(target_dir)
    # Отчёты bin и src имеют одинаковое имя, но разные каталоги.
    out_dir = os.path.join(RESULTS_DIR, project, "checksums", subdir_name)
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "{}_{}.txt".format(project, tool_tag))

    lines = []
    errors = 0
    for fpath in files:
        try:
            lines.append(calc_checksum(cmd_parts, fpath, target_dir))
        except Exception as e:
            errors += 1
            print("  [ERROR] {}: {}".format(fpath, e))

    with open(out_file, "w", encoding="utf-8", newline="\n") as f:
        for ln in lines:
            f.write(ln + "\n")

    print("  [OK] {:<3} -> {} (файлов: {}, ошибок: {})".format(
        subdir_name, out_file, len(lines), errors))
    return out_file


def main():
    parser = argparse.ArgumentParser(
        description="Расчёт КС дистрибутива (origin_bin) и исходных текстов (src)"
    )
    parser.add_argument("--project", action="append", default=None,
                        help="Имя проекта (можно несколько раз; по умолчанию все из unpacked/)")
    parser.add_argument("--tool", default=None,
                        help="Утилита КС (например: md5sum, gostsum, cksum, 'rhash -G')")
    args = parser.parse_args()

    if not os.path.isdir(UNPACKED_DIR):
        print("[ERROR] Не найдена директория: {}".format(UNPACKED_DIR))
        sys.exit(1)

    tool_cmd = args.tool
    if not tool_cmd:
        if not sys.stdin.isatty():
            print("[ERROR] Не задан --tool при неинтерактивном запуске.")
            sys.exit(1)
        tool_cmd = choose_tool_interactive()

    cmd_parts = shlex.split(tool_cmd)
    if not shutil.which(cmd_parts[0]):
        print("[ERROR] Утилита '{}' не найдена в PATH.".format(cmd_parts[0]))
        sys.exit(1)

    tool_tag = report_tool_tag(tool_cmd)
    projects = args.project or sorted(
        d for d in os.listdir(UNPACKED_DIR)
        if os.path.isdir(os.path.join(UNPACKED_DIR, d))
    )
    if not projects:
        print("[INFO] Проектов в {} не найдено.".format(UNPACKED_DIR))
        sys.exit(1)

    print("\nУтилита: {}".format(tool_cmd))
    for proj in projects:
        print("\n[Проект: {}]".format(proj))
        process_subdir(proj, "bin", cmd_parts, tool_tag)
        process_subdir(proj, "src", cmd_parts, tool_tag)

    print("\nГотово. Результаты в {}/<PROJ>/checksums/bin/ и src/".format(RESULTS_DIR))


if __name__ == "__main__":
    main()
