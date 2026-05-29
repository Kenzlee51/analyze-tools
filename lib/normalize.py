#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import shutil
import sys

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
    """Пытается восстановить читаемую кириллицу, если имя сломано."""
    try:
        broken_name.encode('utf-8', 'strict')
        return broken_name   # имя уже корректное
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

def normalize_name(original_name):
    name = translit(original_name)
    name = name.upper()
    name = name.replace(' ', '_')
    name = re.sub(r'[^A-Z0-9_.-]', '', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    return name

def get_unique_name(directory, desired_name):
    if not os.path.exists(os.path.join(directory, desired_name)):
        return desired_name
    counter = 1
    while True:
        candidate = f"{desired_name}_{counter}"
        if not os.path.exists(os.path.join(directory, candidate)):
            return candidate
        counter += 1

def main():
    # Корень проекта – на два уровня выше, чем lib/
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    target_dir = os.path.join(base_dir, 'src')

    if not os.path.isdir(target_dir):
        print(f"Ошибка: директория {target_dir} не существует или не является папкой.")
        sys.exit(1)

    print(f"Работаем с директорией проектов: {target_dir}")
    renamed_count = 0
    with os.scandir(target_dir) as entries:
        for entry in entries:
            if entry.is_dir():   # обрабатываем только папки
                raw_name = entry.name
                original_name = decode_filename(raw_name)
                print(f"Обнаружена папка: {raw_name!r} -> раскодировано как {original_name!r}")

                new_name = normalize_name(original_name)
                if not new_name:
                    print(f"  Предупреждение: для {original_name!r} получено пустое имя, пропускаем.")
                    continue
                if new_name == original_name:
                    continue

                final_name = get_unique_name(target_dir, new_name)
                old_path = entry.path
                new_path = os.path.join(target_dir, final_name)

                try:
                    print(f"  Переименовываем: {raw_name!r} -> {final_name}")
                    shutil.move(old_path, new_path)
                    renamed_count += 1
                except Exception as e:
                    print(f"  Ошибка при переименовании {raw_name!r}: {e}")

    print(f"Готово. Переименовано папок: {renamed_count}")

if __name__ == "__main__":
    main()