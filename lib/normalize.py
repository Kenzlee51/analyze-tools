#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Совместимо с Python 3.5

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


def is_broken(name):
    """True, если имя нельзя закодировать в чистый UTF-8 (битые байты/суррогаты)."""
    try:
        name.encode('utf-8', 'strict')
        return False
    except UnicodeEncodeError:
        return True


def decode_filename(broken_name):
    """Пытается восстановить читаемую кириллицу, если имя сломано.

    Имена, распакованные из архивов cp1251/koi8-r/cp866 в UTF-8-системе,
    Python отдаёт с суррогатами (surrogateescape). Раскодируем обратно в
    правильный UTF-8, перебирая кодировки и проверяя round-trip.
    """
    if not is_broken(broken_name):
        return broken_name  # имя уже корректное
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
    base, ext = os.path.splitext(desired_name)
    counter = 1
    while True:
        candidate = "{}_{}{}".format(base, counter, ext)
        if not os.path.exists(os.path.join(directory, candidate)):
            return candidate
        counter += 1


def repair_tree(content_dir):
    """Рекурсивно чинит битые имена файлов и папок в content_dir.

    Вариант A: чиним ТОЛЬКО кодировку (битые имена -> валидный UTF-8,
    кириллица сохраняется как есть). Корректные имена не трогаем —
    без транслита и приведения к верхнему регистру.

    Обход снизу вверх (сначала самые глубокие пути): переименование
    ребёнка не ломает ещё не обработанный путь родителя.
    """
    if not os.path.isdir(content_dir):
        print("Ошибка: директория {!r} не существует.".format(content_dir))
        return 0

    # Собираем все пути (файлы и папки), сортируем по глубине по убыванию.
    entries = []
    for root, dirs, files in os.walk(content_dir):
        for name in files:
            entries.append(os.path.join(root, name))
        for name in dirs:
            entries.append(os.path.join(root, name))
    entries.sort(key=lambda p: p.count(os.sep), reverse=True)

    renamed = 0
    skipped = 0
    for path in entries:
        parent = os.path.dirname(path)
        name = os.path.basename(path)
        if not is_broken(name):
            continue  # имя нормальное — не трогаем

        new_name = decode_filename(name)
        if is_broken(new_name) or new_name == name:
            # раскодировать не удалось ни одной кодировкой
            print("  [WARN] не удалось раскодировать имя: {!r} — пропускаю".format(name))
            skipped += 1
            continue

        final_name = get_unique_name(parent, new_name)
        old_path = os.path.join(parent, name)
        new_path = os.path.join(parent, final_name)
        try:
            shutil.move(old_path, new_path)
            print("  Починено: {!r} -> {}".format(name, final_name))
            renamed += 1
        except Exception as e:
            print("  [WARN] ошибка переименования {!r}: {}".format(name, e))
            skipped += 1

    print("Готово. Починено имён: {}, пропущено: {}".format(renamed, skipped))
    return renamed


def main():
    # --content-dir DIR — директория для обработки. Если не указана,
    # берём src/ рядом с корнем проекта (обратная совместимость).
    content_dir = None
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--content-dir':
            if i + 1 < len(args):
                content_dir = args[i + 1]
                i += 2
                continue
            print("Ошибка: --content-dir требует путь.")
            sys.exit(1)
        else:
            # позиционный аргумент тоже принимаем как каталог
            content_dir = args[i]
            i += 1
    if content_dir is None:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        content_dir = os.path.join(base_dir, 'src')

    print("Чиню имена (только битые) в: {}".format(content_dir))
    repair_tree(content_dir)


if __name__ == "__main__":
    main()
