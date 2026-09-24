#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import shutil
import subprocess
import argparse
import json
from pathlib import Path
from datetime import datetime

# ----------------------------------------------------------------------------
# Переменные для путей к бинарным файлам PVS-Studio (заполняются автоматически)
PVS_BIN_PATH = ""
PVS_ANALYZER = ""
PLOG_CONVERTER = ""
# ----------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent

LIB_ADAPT_PVS = ROOT_DIR / "lib" / "adapt-pvs"
UNPACKED_DIR = ROOT_DIR / "unpacked"
RESULTS_DIR = ROOT_DIR / "results"
LOGS_DIR = ROOT_DIR / "logs"
WORK_TMP = LIB_ADAPT_PVS / "work-tmp"

PVS_LIC = LIB_ADAPT_PVS / "pvs.lic"
DEFAULT_FORMAT = "html"

def save_paths_to_script(pvs_analyzer_path, plog_converter_path):
    script_path = Path(__file__).resolve()
    with open(script_path, 'r', encoding='utf-8') as f:
        content = f.read()

    bin_dir = str(Path(pvs_analyzer_path).parent)
    # Заменяем строки с переменными (используем re, чтобы точно попасть)
    content = re.sub(r'^PVS_BIN_PATH\s*=\s*".*"', 'PVS_BIN_PATH = "{}"'.format(bin_dir), content, flags=re.MULTILINE)
    content = re.sub(r'^PVS_ANALYZER\s*=\s*".*"', 'PVS_ANALYZER = "{}"'.format(pvs_analyzer_path), content, flags=re.MULTILINE)
    content = re.sub(r'^PLOG_CONVERTER\s*=\s*".*"', 'PLOG_CONVERTER = "{}"'.format(plog_converter_path), content, flags=re.MULTILINE)

    with open(script_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("[INFO] Сохранены пути в {}".format(script_path))

def check_and_setup_tools():
    global PVS_ANALYZER, PLOG_CONVERTER, PVS_BIN_PATH

    if PVS_ANALYZER and PLOG_CONVERTER:
        if os.path.isfile(PVS_ANALYZER) and os.path.isfile(PLOG_CONVERTER):
            print("[INFO] Используются сохранённые пути:\n  PVS-Studio: {}\n  plog-converter: {}".format(PVS_ANALYZER, PLOG_CONVERTER))
            return PVS_ANALYZER, PLOG_CONVERTER
        else:
            print("[WARNING] Сохранённые пути недействительны, будет выполнен поиск заново.")

    def which_tool(name):
        try:
            res = subprocess.run(['which', name], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if res.returncode == 0:
                return res.stdout.strip()
        except:
            pass
        return None

    pvs_path = which_tool('pvs-studio-analyzer')
    plog_path = which_tool('plog-converter')

    if pvs_path and plog_path:
        print("[INFO] Инструменты найдены в системе:\n  PVS-Studio: {}\n  plog-converter: {}".format(pvs_path, plog_path))
        save_paths_to_script(pvs_path, plog_path)
        return pvs_path, plog_path

    print("[INFO] Утилиты pvs-studio-analyzer и plog-converter не найдены в системе.")
    while True:
        bin_dir = input("Укажите полный путь к папке bin, содержащей pvs-studio-analyzer и plog-converter: ").strip()
        if not bin_dir:
            print("Путь не может быть пустым. Попробуйте снова.")
            continue
        bin_path = Path(bin_dir)
        if not bin_path.is_dir():
            print("Папка '{}' не существует. Попробуйте снова.".format(bin_dir))
            continue

        pvs_test = bin_path / "pvs-studio-analyzer"
        plog_test = bin_path / "plog-converter"
        if not pvs_test.is_file():
            print("Файл '{}' не найден. Проверьте путь.".format(pvs_test))
            continue
        if not plog_test.is_file():
            print("Файл '{}' не найден. Проверьте путь.".format(plog_test))
            continue
        if not os.access(str(pvs_test), os.X_OK):
            print("Файл '{}' не является исполняемым.".format(pvs_test))
            continue
        if not os.access(str(plog_test), os.X_OK):
            print("Файл '{}' не является исполняемым.".format(plog_test))
            continue

        pvs_path = str(pvs_test)
        plog_path = str(plog_test)
        print("[INFO] Инструменты найдены:\n  PVS-Studio: {}\n  plog-converter: {}".format(pvs_path, plog_path))
        save_paths_to_script(pvs_path, plog_path)
        return pvs_path, plog_path

def run_command(cmd, cwd=None, log_file=None, description=""):
    print("[EXEC] {} ...".format(description))
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write("\n=== {} ===\n".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            f.write("Команда: {}\n".format(' '.join(cmd)))
            f.write("------------------------------------------------------------\n")
            proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
            while True:
                line = proc.stdout.readline()
                if not line:
                    break
                f.write(line)
                sys.stdout.write("[LOG] " + line)
            proc.wait()
            f.write("------------------------------------------------------------\n")
            f.write("Код возврата: {}\n\n".format(proc.returncode))
            return proc.returncode, "", ""
    else:
        result = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        return result.returncode, result.stdout, result.stderr

def find_projects(unpacked_dir):
    projects = []
    if not unpacked_dir.exists():
        return projects
    for item in unpacked_dir.iterdir():
        if item.is_dir() and (item / "src").exists():
            projects.append(item.name)
    return projects

def prepare_workdir(project_src_path):
    if WORK_TMP.exists():
        shutil.rmtree(str(WORK_TMP))
    WORK_TMP.mkdir(parents=True, exist_ok=True)

    adapt_src = LIB_ADAPT_PVS / "adapt.py"
    configs_src = LIB_ADAPT_PVS / "configs"
    if not adapt_src.exists():
        raise FileNotFoundError("Не найден {}".format(adapt_src))
    if not configs_src.exists():
        raise FileNotFoundError("Не найден {}".format(configs_src))

    (WORK_TMP / "adapt.py").symlink_to(adapt_src)
    (WORK_TMP / "configs").symlink_to(configs_src)
    (WORK_TMP / "src").symlink_to(project_src_path)

    return WORK_TMP

def cleanup_workdir():
    if WORK_TMP.exists():
        shutil.rmtree(str(WORK_TMP))

def main():
    parser = argparse.ArgumentParser(description="Обёртка для анализа проектов с помощью PVS-Studio.")
    parser.add_argument("--format", default=DEFAULT_FORMAT,
                        help="Формат отчёта для plog-converter (html, xml, csv, tasklist и т.д.)")
    parser.add_argument("--plog-args", default="",
                        help="Дополнительные аргументы для plog-converter (например, '-a GA:1,2')")
    parser.add_argument("--only", nargs="+", help="Анализировать только указанные проекты (имена папок)")
    parser.add_argument("--no-clean", action="store_true",
                        help="Не удалять временную папку после завершения (для отладки)")
    args = parser.parse_args()

    if not PVS_LIC.exists():
        print("[ERROR] Файл лицензии не найден: {}".format(PVS_LIC))
        sys.exit(1)

    pvs_analyzer, plog_converter = check_and_setup_tools()

    all_projects = find_projects(UNPACKED_DIR)
    if not all_projects:
        print("[ERROR] В папке unpacked нет проектов с подпапкой src.")
        sys.exit(1)

    if args.only:
        projects_to_analyze = [p for p in all_projects if p in args.only]
        if not projects_to_analyze:
            print("[ERROR] Указанные проекты {} не найдены или не содержат src.".format(args.only))
            sys.exit(1)
    else:
        projects_to_analyze = all_projects

    print("[INFO] Найдено проектов: {}".format(len(projects_to_analyze)))
    print("[INFO] Начинаем анализ...")

    success = []
    failed = []
    skipped = []

    for proj_name in projects_to_analyze:
        print("\n=== Обработка проекта: {} ===".format(proj_name))
        proj_src = UNPACKED_DIR / proj_name / "src"
        if not proj_src.exists():
            skipped.append(proj_name)
            continue

        log_dir = LOGS_DIR / "adapt-pvs" / proj_name
        result_dir = RESULTS_DIR / proj_name / "adapt-pvs"
        result_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = log_dir / "{}.log".format(proj_name)

        try:
            work_dir = prepare_workdir(proj_src)
        except Exception as e:
            print("[ERROR] Не удалось подготовить рабочую директорию: {}".format(e))
            failed.append(proj_name)
            continue

        # adapt.py
        adapt_cmd = ["python3", "adapt.py", "--root", "src"]
        print("[INFO] Запуск adapt.py для {}".format(proj_name))
        ret = run_command(adapt_cmd, cwd=str(work_dir), log_file=str(log_file), description="adapt.py")
        if ret[0] != 0:
            print("[ERROR] adapt.py завершился с ошибкой (код {}) для {}".format(ret[0], proj_name))
            failed.append(proj_name)
            cleanup_workdir()
            continue

        # pvs-studio-analyzer
        log_filename = "{}.log".format(proj_name)
        pvs_cmd = [
            pvs_analyzer,
            "analyze",
            "-j", "4",
            "-l", str(PVS_LIC),
            "-o", log_filename
        ]
        print("[INFO] Запуск pvs-studio-analyzer для {}".format(proj_name))
        ret = run_command(pvs_cmd, cwd=str(work_dir), log_file=str(log_file), description="pvs-studio-analyzer")
        if ret[0] != 0:
            print("[ERROR] pvs-studio-analyzer завершился с ошибкой (код {}) для {}".format(ret[0], proj_name))
            failed.append(proj_name)
            cleanup_workdir()
            continue

        # plog-converter
        report_ext = args.format
        report_filename = "{}.{}".format(proj_name, report_ext)
        report_path = result_dir / report_filename

        plog_cmd = [
            plog_converter,
            "-a", "GA:1,2",
            "-t", args.format,
            "-o", str(report_path),
            log_filename
        ]
        if args.plog_args:
            extra = args.plog_args.split()
            plog_cmd = plog_cmd[:-1] + extra + [plog_cmd[-1]]

        print("[INFO] Запуск plog-converter для {}".format(proj_name))
        ret = run_command(plog_cmd, cwd=str(work_dir), log_file=str(log_file), description="plog-converter")
        if ret[0] != 0:
            print("[ERROR] plog-converter завершился с ошибкой (код {}) для {}".format(ret[0], proj_name))
            failed.append(proj_name)
            cleanup_workdir()
            continue

        print("[SUCCESS] Проект {} обработан успешно. Отчёт сохранён в {}".format(proj_name, report_path))
        success.append(proj_name)

        if not args.no_clean:
            cleanup_workdir()
        else:
            print("[INFO] Временная папка оставлена: {} (ключ --no-clean)".format(WORK_TMP))

    print("\n" + "="*60)
    print("СВОДКА:")
    print("  Успешно: {}".format(len(success)))
    for p in success:
        print("    {}".format(p))
    print("  С ошибками: {}".format(len(failed)))
    for p in failed:
        print("    {}".format(p))
    print("  Пропущено (нет src): {}".format(len(skipped)))
    for p in skipped:
        print("    {}".format(p))
    print("="*60)

    if failed:
        print("[WARNING] Некоторые проекты завершились с ошибками. Проверьте логи.")
        sys.exit(1)
    else:
        print("[SUCCESS] Все проекты обработаны.")

if __name__ == "__main__":
    main()