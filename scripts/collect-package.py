#!/usr/bin/env python3
"""
collect-package.py — Сборка комплекта Приложений 1-6 по каждому проекту.

Источники:
  Приложение 1 — results/<PROJ>/checksums/bin/<PROJ>_<алгоритм>.txt
  Приложение 2 — results/<PROJ>/checksums/src/<PROJ>_<алгоритм>.txt
  (поддерживаются старые файлы checksums/<PROJ>_{bin,src}_*.txt)
  Приложение 3 — results/<PROJ>/akvs/ (или results/summary/akvs/<PROJ>/)
  Приложение 4 — results/<PROJ>/vks/Приложение_4_*.html
  Приложение 5 — results/<PROJ>/vks/Приложение_5_*.html
  Приложение 6 — results/<PROJ>/vks/Приложение_6_*.html

Итог:
  results/summary/package/<PROJ>/
    ├── Приложение 1. КС дистрибутива/
    ├── Приложение 2. КС исходных текстов/
    ├── Приложение 3. Отчет АК-ВС/
    ├── Приложение 4. Фиксация переданного дистрибутива/
    ├── Приложение 5. Фиксация собранного дистрибутива/
    └── Приложение 6. Контроль повторяемости сборки/

Примеры:
    python3 scripts/collect-package.py
    python3 scripts/collect-package.py --project PROJ1 --zip
"""

import argparse
import glob
import os
import shutil
import sys
import zipfile

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(BASE_DIR, "results")
UNPACKED_DIR = os.path.join(BASE_DIR, "unpacked")
PACKAGE_ROOT = os.path.join(RESULTS_DIR, "summary", "package")

APPENDIX_DIRS = {
    1: "Приложение 1. КС дистрибутива",
    2: "Приложение 2. КС исходных текстов",
    3: "Приложение 3. Отчет АК-ВС",
    4: "Приложение 4. Фиксация переданного дистрибутива",
    5: "Приложение 5. Фиксация собранного дистрибутива",
    6: "Приложение 6. Контроль повторяемости сборки",
}


def latest_match(pattern):
    matches = glob.glob(pattern)
    if not matches:
        return None
    matches.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return matches[0]


def copy_file_to_dir(src_file, dst_dir, output_name=None):
    os.makedirs(dst_dir, exist_ok=True)
    shutil.copy2(src_file, os.path.join(dst_dir, output_name or os.path.basename(src_file)))


def find_checksum_report(proj_res, project, kind):
    """Сначала ищем новый формат, затем старый плоский."""
    root = os.path.join(proj_res, "checksums")
    found = latest_match(os.path.join(root, kind, "{}_*.txt".format(project)))
    if found:
        return found
    return latest_match(os.path.join(root, "{}_{}_*.txt".format(project, kind)))


def checksum_output_name(project, kind, source_path):
    """В комплекте имя без bin/src, rhash_G называется gostsum."""
    name = os.path.basename(source_path)
    old_prefix = "{}_{}_".format(project, kind)
    new_prefix = "{}_".format(project)
    if name.startswith(old_prefix) and name.endswith(".txt"):
        algorithm = name[len(old_prefix):-4]
    elif name.startswith(new_prefix) and name.endswith(".txt"):
        algorithm = name[len(new_prefix):-4]
    else:
        raise ValueError("Неожиданное имя отчёта: {}".format(name))
    if algorithm == "rhash_G" or algorithm == "gostsum" or algorithm.startswith("gostsum_"):
        algorithm = "gostsum"
    return "{}_{}.txt".format(project, algorithm)


def copy_tree_fresh(src_dir, dst_dir):
    if os.path.exists(dst_dir):
        shutil.rmtree(dst_dir)
    shutil.copytree(src_dir, dst_dir)


def zip_dir(src_dir, zip_path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for dirpath, _, filenames in os.walk(src_dir):
            for name in sorted(filenames):
                full = os.path.join(dirpath, name)
                arc = os.path.relpath(full, src_dir)
                zf.write(full, arc)


def collect_project(project, make_zip=False):
    proj_res = os.path.join(RESULTS_DIR, project)
    pkg_dir = os.path.join(PACKAGE_ROOT, project)
    if os.path.exists(pkg_dir):
        shutil.rmtree(pkg_dir)
    os.makedirs(pkg_dir, exist_ok=True)

    missing = []

    # Приложение 1: КС дистрибутива (bin)
    app1 = find_checksum_report(proj_res, project, "bin")
    if app1:
        copy_file_to_dir(app1, os.path.join(pkg_dir, APPENDIX_DIRS[1]),
                         checksum_output_name(project, "bin", app1))
    else:
        missing.append(APPENDIX_DIRS[1])

    # Приложение 2: КС исходных текстов (src)
    app2 = find_checksum_report(proj_res, project, "src")
    if app2:
        copy_file_to_dir(app2, os.path.join(pkg_dir, APPENDIX_DIRS[2]),
                         checksum_output_name(project, "src", app2))
    else:
        missing.append(APPENDIX_DIRS[2])

    # Приложение 3: Отчет АК-ВС
    akvs_src = os.path.join(proj_res, "akvs")
    if not os.path.isdir(akvs_src):
        akvs_src = os.path.join(RESULTS_DIR, "summary", "akvs", project)
    if os.path.isdir(akvs_src) and os.listdir(akvs_src):
        copy_tree_fresh(akvs_src, os.path.join(pkg_dir, APPENDIX_DIRS[3]))
    else:
        missing.append(APPENDIX_DIRS[3])

    # Приложения 4-6: ВКС (ФИКС 2.0.2)
    vks_map = [
        ("Приложение_4_*.html", 4),
        ("Приложение_5_*.html", 5),
        ("Приложение_6_*.html", 6),
    ]
    for pat, num in vks_map:
        f = latest_match(os.path.join(proj_res, "vks", pat))
        if f:
            copy_file_to_dir(f, os.path.join(pkg_dir, APPENDIX_DIRS[num]))
        else:
            missing.append(APPENDIX_DIRS[num])

    if make_zip:
        zpath = os.path.join(PACKAGE_ROOT, "{}_appendices_1-6.zip".format(project))
        zip_dir(pkg_dir, zpath)

    status = "OK (полный комплект)" if not missing else "НЕПОЛНЫЙ (нет: {})".format(", ".join(missing))
    print("  [{:<20}] {}".format(project, status))
    return len(missing) == 0


def discover_projects():
    names = set()
    if os.path.isdir(UNPACKED_DIR):
        for d in os.listdir(UNPACKED_DIR):
            if os.path.isdir(os.path.join(UNPACKED_DIR, d)):
                names.add(d)
    if os.path.isdir(RESULTS_DIR):
        for d in os.listdir(RESULTS_DIR):
            if d != "summary" and os.path.isdir(os.path.join(RESULTS_DIR, d)):
                names.add(d)
    return sorted(names)


def main():
    parser = argparse.ArgumentParser(description="Сборка комплекта Приложений 1-6")
    parser.add_argument("--project", action="append", default=None,
                        help="Имя проекта (можно несколько раз; по умолчанию все)")
    parser.add_argument("--zip", action="store_true",
                        help="Дополнительно упаковать комплект каждого проекта в .zip")
    args = parser.parse_args()

    projects = args.project or discover_projects()
    if not projects:
        print("[INFO] Проекты не найдены.")
        sys.exit(1)

    print("Сборка комплектов Приложений 1-6 -> {}\n".format(PACKAGE_ROOT))
    full_ok = 0
    for proj in projects:
        if collect_project(proj, make_zip=args.zip):
            full_ok += 1

    print("\nГотово. Полных комплектов: {}/{}".format(full_ok, len(projects)))


if __name__ == "__main__":
    main()
