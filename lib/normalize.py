#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
normalize.py — нормализация имён файлов и папок

ОПИСАНИЕ:
    Два режима работы:

    Режим 1 (--project-dir DIR):
        Нормализует имя папки проекта первого уровня:
        - восстанавливает сломанную кодировку
        - транслитерирует кириллицу → латиницу
        - заменяет пробелы и спецсимволы на _
        Используется для переименования папки проекта в src/.

    Режим 2 (--content-dir DIR):
        Рекурсивно нормализует содержимое указанной директории:
        - только восстановление сломанной кодировки
        - без транслитерации (имена файлов не меняются кроме кодировки)
        Используется для нормализации содержимого после cp -a.

ИСПОЛЬЗОВАНИЕ:
    python3 normalize.py --project-dir /path/to/src
    python3 normalize.py --content-dir /path/to/unpacked/PROJ
    python3 normalize.py --dry-run --content-dir /path/to/unpacked/PROJ
"""

import os
import re
import shutil
import sys
import argparse

TRANSLIT_TABLE = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
    'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
    'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
    'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
    'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
    'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'E',
    'Ж': 'ZH', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M',
    'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U',
    'Ф': 'F', 'Х': 'KH', 'Ц': 'TS', 'Ч': 'CH', 'Ш': 'SH', 'Щ': 'SCH',
    'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'YU', 'Я': 'YA'
}

ENCODINGS = ('cp1251', 'koi8-r', 'cp866', 'iso8859-5')


def decode_filename(broken_name):
    """Пытается восстановить читаемую кириллицу если имя сломано."""
    try:
        broken_name.encode('utf-8', 'strict')
        return broken_name
    except UnicodeEncodeError:
        pass
    name_bytes = broken_name.encode('utf-8', 'surrogateescape')
    for enc in ENCODINGS:
        try:
            decoded = name_bytes.decode(enc)
            if decoded.encode(enc) == name_bytes:
                return decoded
        except (UnicodeDecodeError, UnicodeEncodeError):
            continue
    return broken_name


def translit(text):
    return ''.join(TRANSLIT_TABLE.get(ch, ch) for ch in text)


def normalize_project_name(original_name):
    """Полная нормализация для имени папки проекта: транслит + очистка."""
    name = translit(original_name)
    name = name.upper()
    name = name.replace(' ', '_')
    name = re.sub(r'[^A-Z0-9_.-]', '', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    return name


def normalize_content_name(original_name):
    """
    Нормализация для содержимого: только восстановление кодировки.
    Транслитерация не выполняется — имя меняется только если было сломано.
    """
    return decode_filename(original_name)


def get_unique_name(directory, desired_name):
    if not os.path.exists(os.path.join(directory, desired_name)):
        return desired_name
    counter = 1
    while True:
        candidate = f"{desired_name}_{counter}"
        if not os.path.exists(os.path.join(directory, candidate)):
            return candidate
        counter += 1


def normalize_project_dir(target_dir, dry_run=False):
    """
    Режим 1: нормализует папки проекта первого уровня в target_dir.
    Восстанавливает кодировку + транслитерирует кириллицу.
    """
    if not os.path.isdir(target_dir):
        print(f"[ERROR] Директория не существует: {target_dir}")
        sys.exit(1)

    print(f"[normalize] Режим: project-dir → {target_dir}")
    renamed_count = 0

    with os.scandir(target_dir) as entries:
        for entry in entries:
            if not entry.is_dir():
                continue
            raw_name = entry.name
            decoded_name = decode_filename(raw_name)
            new_name = normalize_project_name(decoded_name)

            if not new_name:
                print(f"[WARN] Пустое имя для {raw_name!r}, пропускаем.")
                continue
            if new_name == raw_name:
                continue

            final_name = get_unique_name(target_dir, new_name)
            old_path = entry.path
            new_path = os.path.join(target_dir, final_name)

            print(f"[normalize] {raw_name!r} -> {final_name!r}")
            if not dry_run:
                try:
                    shutil.move(old_path, new_path)
                    renamed_count += 1
                except Exception as e:
                    print(f"[ERROR] Переименование {raw_name!r}: {e}")

    print(f"[normalize] project-dir done. Переименовано: {renamed_count}")
    return renamed_count


def normalize_content_dir(target_dir, dry_run=False):
    """
    Режим 2: рекурсивно нормализует содержимое target_dir.
    Только восстановление сломанной кодировки, без транслитерации.
    Обходит дерево снизу вверх чтобы не потерять пути после переименования.
    """
    if not os.path.isdir(target_dir):
        print(f"[ERROR] Директория не существует: {target_dir}")
        sys.exit(1)

    print(f"[normalize] Режим: content-dir → {target_dir}")
    renamed_count = 0

    # os.walk с topdown=False — сначала дочерние, потом родительские
    for dirpath, dirnames, filenames in os.walk(target_dir, topdown=False):
        # Нормализуем файлы
        for name in filenames:
            decoded = normalize_content_name(name)
            if decoded == name:
                continue
            final = get_unique_name(dirpath, decoded)
            old_path = os.path.join(dirpath, name)
            new_path = os.path.join(dirpath, final)
            print(f"[normalize] file: {name!r} -> {final!r}  (in {dirpath})")
            if not dry_run:
                try:
                    shutil.move(old_path, new_path)
                    renamed_count += 1
                except Exception as e:
                    print(f"[ERROR] {old_path!r}: {e}")

        # Нормализуем подпапки (кроме корневой target_dir)
        for name in dirnames:
            if os.path.join(dirpath, name) == target_dir:
                continue
            decoded = normalize_content_name(name)
            if decoded == name:
                continue
            final = get_unique_name(dirpath, decoded)
            old_path = os.path.join(dirpath, name)
            new_path = os.path.join(dirpath, final)
            print(f"[normalize] dir:  {name!r} -> {final!r}  (in {dirpath})")
            if not dry_run:
                try:
                    shutil.move(old_path, new_path)
                    renamed_count += 1
                except Exception as e:
                    print(f"[ERROR] {old_path!r}: {e}")

    print(f"[normalize] content-dir done. Переименовано: {renamed_count}")
    return renamed_count


def main():
    parser = argparse.ArgumentParser(
        description='Нормализация имён файлов и папок'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--project-dir', metavar='DIR',
        help='Нормализовать папки проекта первого уровня (транслит + кодировка)'
    )
    group.add_argument(
        '--content-dir', metavar='DIR',
        help='Рекурсивно нормализовать содержимое директории (только кодировка)'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Показать что будет переименовано без реального переименования'
    )
    args = parser.parse_args()

    if args.project_dir:
        normalize_project_dir(args.project_dir, dry_run=args.dry_run)
    else:
        normalize_content_dir(args.content_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
