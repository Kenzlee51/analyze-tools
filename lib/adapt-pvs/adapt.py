#!/usr/bin/env python3
import os
import re
import json
import sys
import shutil
import subprocess
import argparse
from pathlib import Path

CONFIG_DIR = 'configs'

def load_config(compiler_name):
    config_path = Path(CONFIG_DIR) / f'{compiler_name}.json'
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {"defines": [], "include_fake_paths": [], "skip_includes": []}

def load_package_map():
    map_path = Path(CONFIG_DIR) / 'package_map.json'
    if map_path.exists():
        with open(map_path) as f:
            return json.load(f)
    return {}

def get_system_include_paths():
    cmd = ['gcc', '-v', '-E', '-x', 'c++', '/dev/null']
    proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, universal_newlines=True)  # было text=True
    output = proc.communicate()[1]
    paths = []
    in_include = False
    for line in output.splitlines():
        if line.startswith('#include <...>'):
            in_include = True
        elif in_include and line.startswith(' '):
            paths.append(line.strip())
        elif in_include and not line.startswith(' '):
            in_include = False
    return paths

def collect_all_includes(root):
    includes = []
    for ext in ('*.c', '*.cpp', '*.cc', '*.cxx', '*.h', '*.hpp', '*.hxx'):
        for f in Path(root).rglob(ext):
            with open(f, 'r', errors='ignore') as fp:
                for line in fp:
                    m = re.search(r'#include\s*[<"]([^>"]+)[>"]', line)
                    if m:
                        name = m.group(1)
                        typ = 'system' if '<' in line and '>' in line else 'local'
                        includes.append((name, typ, str(f)))
    return includes

def find_missing_includes(includes, include_dirs, system_paths):
    missing = {}
    for name, typ, src_file in includes:
        if name in missing:
            continue
        if typ == 'system':
            found = False
            for sp in system_paths:
                if os.path.exists(os.path.join(sp, name)):
                    found = True
                    break
            if found:
                continue
        found_in_project = False
        for inc_dir in include_dirs:
            if os.path.exists(os.path.join(inc_dir, name)):
                found_in_project = True
                break
        if not found_in_project:
            missing[name] = typ
    return missing

def create_fake_headers(missing_includes, fake_dir):
    Path(fake_dir).mkdir(exist_ok=True)
    created = set()
    for name, typ in missing_includes.items():
        if name in created:
            continue
        if '..' in name or name.startswith('/'):
            continue
        fake_path = Path(fake_dir) / name
        fake_path.parent.mkdir(parents=True, exist_ok=True)
        fake_path.touch()
        created.add(name)
        print("Created fake: {}".format(fake_path))

def detect_required_defines(root):
    macros = set()
    for ext in ('*.c', '*.cpp', '*.cc', '*.cxx', '*.h', '*.hpp', '*.hxx'):
        for f in Path(root).rglob(ext):
            with open(f, 'r', errors='ignore') as fp:
                for line in fp:
                    m = re.search(r'#ifdef\s+(\w+)', line)
                    if m:
                        macros.add(m.group(1))
                    m = re.search(r'#ifndef\s+(\w+)', line)
                    if m:
                        macros.add(m.group(1))
                    if '#error' in line:
                        words = re.findall(r'\b[A-Z_][A-Z0-9_]*\b', line)
                        for w in words:
                            if w not in ['error', 'You', 'must', 'use', 'compatible', 'processor']:
                                macros.add(w)
    return macros

def generate_compile_commands(sources, include_dirs, fake_dir, defines, cwd):
    commands = []
    all_include_dirs = include_dirs + [fake_dir]
    for src in sources:
        compiler = 'g++' if src.endswith(('.cpp', '.cc', '.cxx')) else 'gcc'
        args = [compiler]
        for inc in all_include_dirs:
            if inc:
                args.append('-I{}'.format(inc))  # f-строка заменена
        for d in defines:
            if d:
                args.append('-D{}'.format(d))    # f-строка заменена
        args.append('-c')
        args.append(src)
        obj = os.path.basename(src) + '.o'
        args.extend(['-o', obj])
        commands.append({
            'directory': cwd,
            'file': src,
            'arguments': args,
            'command': ' '.join(args)
        })
    return commands

def detect_compiler(root):
    signs = [
        ('watcom', ['_make7', '_make_all']),
        ('keil', ['*.uvprojx', '*.uvproj']),
        ('iar', ['*.ewp']),
        ('msvc', ['*.sln', '*.vcxproj']),
        ('cmake', ['CMakeLists.txt']),
        ('gcc', ['Makefile', 'configure']),
        ('clang', ['.clang-format'])
    ]
    root_path = Path(root)
    for compiler, patterns in signs:
        for pat in patterns:
            if '*' in pat:
                if list(root_path.rglob(pat)):
                    print("Detected compiler: {} (found {})".format(compiler, pat))
                    return compiler
            else:
                if list(root_path.rglob(pat)):
                    print("Detected compiler: {} (found {})".format(compiler, pat))
                    return compiler
    print("No specific compiler detected, using 'gcc' as default.")
    return 'gcc'

def detect_keil_paths():
    possible_roots = [
        '/usr/Keil',
        '/opt/Keil',
        '/mnt/c/Keil',
        '/mnt/d/Keil',
        '/mnt/e/Keil',
        '/mnt/f/Keil'
    ]
    include_subdirs = [
        'ARM/ARMCC/include',
        'ARM/Pack/ARM/CMSIS/4.2.0/CMSIS/Include',
        'ARM/Pack/ARM/CMSIS/4.2.0/Device/ARM/ARMCM3/Include'
    ]
    found_paths = []
    for root in possible_roots:
        if os.path.isdir(root):
            for sub in include_subdirs:
                p = os.path.join(root, sub)
                if os.path.isdir(p):
                    found_paths.append(p)
            if found_paths:
                print("Keil found at: {}".format(root))
                break
    if not found_paths:
        copied = Path('fake_includes/keil_copied')
        if copied.exists():
            found_paths.append(str(copied / 'ARM/ARMCC/include'))
            found_paths.append(str(copied / 'ARM/Pack/ARM/CMSIS/4.2.0/CMSIS/Include'))
            found_paths.append(str(copied / 'ARM/Pack/ARM/CMSIS/4.2.0/Device/ARM/ARMCM3/Include'))
            found_paths = [p for p in found_paths if os.path.isdir(p)]
            if found_paths:
                print("Using copied Keil headers from fake_includes/keil_copied (fallback).")
            else:
                print("Keil headers not found. Will use empty stubs for missing includes.")
    return found_paths

def recommend_packages(missing_headers, package_map):
    packages = set()
    has_system_include = any(typ == 'system' for typ in missing_headers.values())
    if has_system_include:
        packages.add('build-essential')
        packages.add('libc6-dev')
        packages.add('linux-libc-dev')
        packages.add('libstdc++-dev')
    for header, typ in missing_headers.items():
        if typ == 'system':
            pkg = package_map.get(header)
            if pkg:
                packages.add(pkg)
    return packages

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('compiler', nargs='?', default=None)
    parser.add_argument('--clean', '-c', action='store_true')
    parser.add_argument('--root', '-r', default='./src')
    args = parser.parse_args()

    if args.compiler is None:
        compiler = detect_compiler(args.root)
    else:
        compiler = args.compiler
        print("Using manually specified compiler: {}".format(compiler))

    if args.clean:
        if os.path.exists('fake_includes'):
            shutil.rmtree('fake_includes')
            print("Removed old fake_includes")
        if os.path.exists('compile_commands.json'):
            os.remove('compile_commands.json')
            print("Removed old compile_commands.json")

    PROJECT_ROOT = args.root
    config = load_config(compiler)
    defines_config = config.get('defines', [])
    skip_includes = config.get('skip_includes', [])
    extra_include_paths = config.get('include_fake_paths', [])

    include_dirs = list(extra_include_paths)

    if compiler == 'keil':
        keil_paths = detect_keil_paths()
        include_dirs = keil_paths + include_dirs

    local_includes = [str(p) for p in Path(PROJECT_ROOT).rglob('*') if p.is_dir()]
    local_includes.append(PROJECT_ROOT)
    include_dirs.extend(local_includes)

    seen = set()
    unique_include_dirs = []
    for p in include_dirs:
        if p and p not in seen:
            seen.add(p)
            unique_include_dirs.append(p)
    include_dirs = unique_include_dirs

    includes = collect_all_includes(PROJECT_ROOT)
    print("Found {} include directives.".format(len(includes)))

    sys_paths = get_system_include_paths()
    missing = find_missing_includes(includes, include_dirs, sys_paths)
    print("Found {} missing includes.".format(len(missing)))
    create_fake_headers(missing, 'fake_includes')

    detected = detect_required_defines(PROJECT_ROOT)
    all_defines = list(set(defines_config + list(detected)))

    sources = []
    for ext in ('*.c', '*.cpp', '*.cc', '*.cxx'):
        sources.extend([str(p) for p in Path(PROJECT_ROOT).rglob(ext)])
    print("Found {} source files.".format(len(sources)))

    cwd = os.getcwd()
    commands = generate_compile_commands(sources, include_dirs, 'fake_includes', all_defines, cwd)
    with open('compile_commands.json', 'w') as f:
        json.dump(commands, f, indent=2)
    print("Generated compile_commands.json with {} macros.".format(len(all_defines)))

    package_map = load_package_map()
    recommended = recommend_packages(missing, package_map)
    if recommended:
        print("\n" + "="*60)
        print("Recommended dev packages to install for better analysis quality:")
        print("  sudo apt install {}".format(' '.join(sorted(recommended))))
        print("  (Run this command to install real headers instead of empty stubs.)")
        print("="*60)

    print("\n" + "="*60)
    print("Next steps for PVS-Studio analysis:")
    print("  # License setup (choose one):")
    print("  cp ./pvs.lic ~/.config/PVS-Studio/")
    print("  # or")
    print('  pvs-studio-analyzer credentials "Имя_Пользователя" "Лицензионный_Ключ"')
    print()
    print("  # Run analysis:")
    print("  pvs-studio-analyzer analyze")
    print()
    print("  # Generate reports:")
    print("  plog-converter -t csv --filterSecurityRelatedIssues -o res_filtered.csv PVS-Studio.log")
    print("  plog-converter -t html --filterSecurityRelatedIssues -o res_filtered.html PVS-Studio.log")
    print("  plog-converter -t csv -o res_all.csv PVS-Studio.log")
    print("="*60)

if __name__ == '__main__':
    main()