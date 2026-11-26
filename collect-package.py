#!/usr/bin/env python3
"""Запуск сборщика комплекта приложений из корня репозитория."""
import os
import sys

if __name__ == "__main__":
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts", "collect-package.py")
    os.execv(sys.executable, [sys.executable, script] + sys.argv[1:])
