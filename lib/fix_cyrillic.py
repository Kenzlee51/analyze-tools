#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Совместимо с Python 3.5

import os
import sys

ENCODINGS = ('cp1251', 'koi8-r', 'cp866', 'iso8859-5')

def is_broken(name):
    """Возвращает True, если имя содержит суррогаты (невалидный UTF-8)."""
    try:
        name.encode('utf-8', 'strict')
        return False
    except UnicodeEncodeError:
        return True

def decode_broken_name(name):
    """Восстанавливает читаемую кириллицу из сломанного имени."""
    name_bytes = name.encode('utf-8', 'surrogateescape')
    for enc in ENCODINGS:
        try:
            decoded = name_bytes.decode(enc)
            # Проверяем, что обратное преобразование даст те же байты
            if decoded.encode(enc) == name_bytes:
                return decoded
        except (UnicodeDecodeError, UnicodeEncodeError):
            continue
    return name   # если не удалось восстановить, возвращаем как есть

def safe_print(msg):
    """Вывод строки с заменой недопустимых символов через repr."""
    # Python 3.5: нет sys.stdout.reconfigure, поэтому используем repr
    print(repr(msg))

def fix_cyrillic_names(root_dir):
    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        for name in filenames + dirnames:
            if not is_broken(name):
                continue
            full_path = os.path.join(dirpath, name)
            new_name = decode_broken_name(name)
            if new_name == name:
                continue
            new_full_path = os.path.join(dirpath, new_name)
            if os.path.exists(new_full_path):
                base, ext = os.path.splitext(new_name)
                counter = 1
                while True:
                    candidate = "{}_{}{}".format(base, counter, ext)
                    candidate_path = os.path.join(dirpath, candidate)
                    if not os.path.exists(candidate_path):
                        new_full_path = candidate_path
                        new_name = candidate
                        break
                    counter += 1
            os.rename(full_path, new_full_path)
            safe_print("FIXED: {!r} -> {!r}".format(full_path, new_name))

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: fix_cyrillic.py <directory>")
        sys.exit(1)
    root = sys.argv[1]
    if not os.path.isdir(root):
        print("Error: {} is not a directory".format(root))
        sys.exit(1)
    fix_cyrillic_names(root)