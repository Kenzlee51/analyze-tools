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
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UNPACKED_DIR = os.path.join(BASE_DIR, "unpacked")
RESULTS_DIR = os.path.join(BASE_DIR, "results")

# Настройки производительности (можно переопределить флагами).
DEFAULT_BATCH_SIZE = 1000
DEFAULT_THREADS = 4
MAX_BATCH_ARG_BYTES = 128 * 1024
PROGRESS_INTERVAL = 2.0

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


def format_checksum(checksum, file_path, root_dir):
    rel_clean = clean_rel_path(os.path.relpath(file_path, root_dir))
    # Linux может хранить имена с байтами, не являющимися UTF-8.
    # В отчёте представляем такие байты как \\xNN; путь для расчёта КС
    # остаётся исходным, корректные UTF-8 имена не изменяются.
    rel_clean = rel_clean.encode("utf-8", errors="surrogateescape").decode(
        "utf-8", errors="backslashreplace")
    return "{}  {}".format(checksum, rel_clean)


def calc_checksum(cmd_parts, file_path, root_dir):
    """Совместимый пофайловый режим для произвольных команд и ошибок пакета."""
    res = subprocess.run(
        cmd_parts + [file_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if res.returncode != 0:
        err = res.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError("Ошибка {}: {}".format(" ".join(cmd_parts), err))
    line = res.stdout.decode("utf-8", errors="replace").strip().splitlines()[0]
    exe_name = os.path.basename(cmd_parts[0]).lower()
    tokens = line.split()
    if exe_name == "cksum" and len(tokens) >= 2:
        checksum = "{} {}".format(tokens[0], tokens[1])
    else:
        checksum = tokens[0] if tokens else line
        # GNU coreutils помечает экранированное имя обратной косой чертой
        # перед хешем. Это не часть контрольной суммы.
        if exe_name in ("md5sum", "sha256sum") and re.fullmatch(
                r"\\[0-9a-fA-F]{32}|\\[0-9a-fA-F]{64}", checksum):
            checksum = checksum[1:]
    return format_checksum(checksum, file_path, root_dir)


def batch_kind(cmd_parts):
    """Пакетный режим только для известных форматов вывода."""
    exe = os.path.basename(cmd_parts[0]).lower()
    flags = cmd_parts[1:]
    if not flags and exe in ("md5sum", "sha256sum", "cksum", "gostsum"):
        return exe
    if exe == "rhash" and flags in (["-G"], ["--gost12-256"]):
        return "rhash"
    return None


def iter_batches(files, batch_size, cmd_parts):
    """Ограничиваем и число файлов, и размер argv, включая длинные пути."""
    command_bytes = sum(len(os.fsencode(arg)) + 9 for arg in cmd_parts)
    budget = MAX_BATCH_ARG_BYTES
    try:
        env_bytes = sum(len(os.fsencode(k)) + len(os.fsencode(v)) + 10
                        for k, v in os.environ.items())
        budget = min(budget, max(4096, os.sysconf("SC_ARG_MAX") - env_bytes - 32768))
    except (AttributeError, OSError, ValueError):
        pass
    batch, used = [], command_bytes
    for path in files:
        cost = len(os.fsencode(path)) + 9
        if batch and (len(batch) >= batch_size or used + cost > budget):
            yield batch
            batch, used = [], command_bytes
        batch.append(path)
        used += cost
    if batch:
        yield batch


def parse_batch_output(stdout, files, kind, separator):
    """Проверяем соответствие каждого хеша точному исходному пути (в байтах)."""
    expected = {os.fsencode(path): path for path in files}
    found = {}
    if kind == "cksum":
        pattern = rb"([0-9]+)[ \t]+([0-9]+)[ \t]+(/.+)"
    else:
        digits = 32 if kind == "md5sum" else 64
        pattern = rb"([0-9a-fA-F]{" + str(digits).encode("ascii") + rb"})[ \t]+\*?(/.+)"
    for record in stdout.split(separator):
        if not record:
            continue
        match = re.fullmatch(pattern, record, flags=re.DOTALL)
        if not match:
            raise ValueError("Неожиданный формат пакетного вывода")
        raw_path = match.group(3 if kind == "cksum" else 2)
        if raw_path not in expected or raw_path in found:
            raise ValueError("Путь в пакетном выводе не совпадает или повторяется")
        digest = match.group(1).decode("ascii")
        if kind == "cksum":
            digest += " " + match.group(2).decode("ascii")
        found[raw_path] = digest
    if len(found) != len(expected):
        raise ValueError("Получены КС не для всех файлов пакета")
    return [found[os.fsencode(path)] for path in files]


def calc_batch(cmd_parts, files, root_dir, kind):
    """Возвращаем результаты в исходном порядке; при сбое считаем по одному."""
    fallback = False
    if kind:
        try:
            # GNU -z сохраняет исходные байты имён, включая пробелы и переводы строк.
            # Для старых cksum и ГОСТ принимаем обычный вывод только после
            # строгой проверки соответствия имён.
            variants = [([], b"\n")]
            if kind in ("md5sum", "sha256sum", "cksum"):
                variants.insert(0, (["-z"], b"\0"))
            for extra, separator in variants:
                if separator == b"\n" and any(b"\n" in os.fsencode(p) or b"\r" in os.fsencode(p) for p in files):
                    continue
                try:
                    res = subprocess.run(cmd_parts + extra + files,
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    if res.returncode != 0:
                        continue
                    digests = parse_batch_output(res.stdout, files, kind, separator)
                    return ([format_checksum(digest, path, root_dir)
                             for path, digest in zip(files, digests)], [], False)
                except (OSError, ValueError):
                    continue
        except Exception:
            # Ошибка конкретной утилиты не должна потерять результаты пакета.
            pass
        fallback = True

    lines, errors = [], []
    for path in files:
        try:
            lines.append(calc_checksum(cmd_parts, path, root_dir))
        except Exception as exc:
            errors.append((path, str(exc)))
    return lines, errors, fallback


def positive_int(value):
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("Ожидается положительное целое число")
    if number < 1:
        raise argparse.ArgumentTypeError("Значение должно быть больше нуля")
    return number


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


def process_subdir(project, subdir_name, cmd_parts, tool_tag,
                   batch_size=DEFAULT_BATCH_SIZE, threads=DEFAULT_THREADS):
    # Приложение 1 — origin_bin, Приложение 2 — src.
    # Имя выходного каталога Приложения 1 остаётся checksums/bin.
    source_subdir = "origin_bin" if subdir_name == "bin" else subdir_name
    target_dir = os.path.join(UNPACKED_DIR, project, source_subdir)
    if not os.path.isdir(target_dir):
        print("  [SKIP] Нет каталога: {}".format(target_dir))
        return None

    started = time.monotonic()
    print("  [{}] Сбор списка файлов: {}".format(subdir_name, source_subdir), flush=True)
    files = collect_files(target_dir)
    # Отчёты bin и src имеют одинаковое имя, но разные каталоги.
    out_dir = os.path.join(RESULTS_DIR, project, "checksums", subdir_name)
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "{}_{}.txt".format(project, tool_tag))

    kind = batch_kind(cmd_parts)
    batches = list(iter_batches(files, batch_size, cmd_parts))
    results = [None] * len(batches)
    errors, completed, fallback_count = 0, 0, 0
    total = len(files)
    last_progress = time.monotonic()
    print("  [{}] Файлов: {}; пакетов: {}; потоков: {}; режим: {}".format(
        subdir_name, total, len(batches), min(threads, len(batches)),
        "пакетный" if kind else "пофайловый"), flush=True)

    if batches:
        with ThreadPoolExecutor(max_workers=min(threads, len(batches))) as pool:
            futures = {pool.submit(calc_batch, cmd_parts, batch, target_dir, kind): idx
                       for idx, batch in enumerate(batches)}
            for future in as_completed(futures):
                idx = futures[future]
                batch_lines, batch_errors, fallback = future.result()
                results[idx] = batch_lines
                completed += len(batches[idx])
                errors += len(batch_errors)
                fallback_count += int(fallback)
                for path, message in batch_errors:
                    print("  [ERROR] {}: {}".format(path, message), flush=True)
                now = time.monotonic()
                if completed == total or now - last_progress >= PROGRESS_INTERVAL:
                    elapsed = max(now - started, 0.001)
                    print("  [PROGRESS] {} {}/{} ({:.1f}%) | {:.0f} файлов/с | {:.1f} с".format(
                        subdir_name, completed, total, 100 * completed / total,
                        completed / elapsed, elapsed), flush=True)
                    last_progress = now

    if fallback_count:
        print("  [WARN] Пакетов пересчитано пофайлово: {}".format(fallback_count))
    lines = [line for batch_lines in results for line in batch_lines]

    with open(out_file, "w", encoding="utf-8", newline="\n") as f:
        for ln in lines:
            f.write(ln + "\n")

    print("  [{}] {:<3} -> {} (файлов: {}, ошибок: {}, время: {:.2f} с)".format(
        "OK" if errors == 0 else "WARN", subdir_name, out_file, len(lines), errors,
        time.monotonic() - started))
    return out_file


def main():
    parser = argparse.ArgumentParser(
        description="НАЗНАЧЕНИЕ: расчёт КС дистрибутива (origin_bin) и исходных текстов (src).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""ВХОД: unpacked/PROJ/origin_bin/ и unpacked/PROJ/src/.
РЕЗУЛЬТАТ: results/PROJ/checksums/{bin,src}/PROJ_АЛГОРИТМ.txt.
ЗАВИСИМОСТИ: Python 3 и выбранная утилита КС в PATH.

ПРИМЕРЫ:
  ./checksums.py
  ./checksums.py --project PROJ --tool md5sum
  ./checksums.py --tool "rhash -G" --batch-size 1000 --threads 4
  ./checksums.py --tool sha256sum --threads 1

ВАЖНО:
  Без --tool открывается меню (только интерактивно).
  Распакованные архивы и симлинки пропускаются; _dir/_unpack удаляются из путей.
  Имена файлов не меняются; некорректный UTF-8 экранируется в отчёте.
  Неизвестные команды и неподдерживаемые пакеты считаются пофайлово.
  Порядок и формат отчёта сохраняются; существующие отчёты перезаписываются."""
    )
    parser.add_argument("--project", action="append", default=None,
                        help="Имя проекта (можно несколько раз; по умолчанию все из unpacked/)")
    parser.add_argument("--tool", default=None,
                        help="Команда КС: md5sum, sha256sum, cksum, gostsum, 'rhash -G' или своя")
    parser.add_argument("--batch-size", type=positive_int, default=DEFAULT_BATCH_SIZE,
                        help="Максимум файлов в пакете (по умолчанию: %(default)s)")
    parser.add_argument("--threads", type=positive_int, default=DEFAULT_THREADS,
                        help="Параллельных пакетов (по умолчанию: %(default)s)")
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
    print("Размер пакета: {} | Потоков: {}".format(args.batch_size, args.threads))
    for proj in projects:
        print("\n[Проект: {}]".format(proj))
        process_subdir(proj, "bin", cmd_parts, tool_tag, args.batch_size, args.threads)
        process_subdir(proj, "src", cmd_parts, tool_tag, args.batch_size, args.threads)

    print("\nГотово. Результаты в {}/<PROJ>/checksums/bin/ и src/".format(RESULTS_DIR))


if __name__ == "__main__":
    main()
