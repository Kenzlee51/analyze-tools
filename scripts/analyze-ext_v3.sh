#!/bin/bash
#
# =============================================================================
# analyze-expr.sh — Скрипт анализа расширений и исполняемых файлов
# =============================================================================
#
# ОПИСАНИЕ:
#   Анализирует директории unpacked/PROJ/src и unpacked/PROJ/bin:
#     - подсчитывает количество файлов по расширениям (включая архивы)
#     - находит исполняемые файлы в src (по магическим байтам через file)
#     - находит бинарные файлы в src (по расширению из BINARY_EXTENSIONS)
#     - записывает результаты в results/PROJ/expr/
#     - обновляет глобальный агрегированный файл result-expr.json
#
# ИСПОЛЬЗОВАНИЕ:
#   ./analyze-expr.sh [OPTIONS]
#
# ОПЦИИ:
#   --single-project NAME   Обработать только один указанный проект
#   --no-rewrite            Пропустить проект если он уже есть в result-expr.json
#   -j, --parallel N        Количество параллельных проектов (default: 4)
#   -h, --help              Показать справку
#
# ПРИМЕРЫ:
#   ./analyze-expr.sh
#   ./analyze-expr.sh --single-project PROJ1
#   ./analyze-expr.sh --no-rewrite
#   ./analyze-expr.sh -j 2 --single-project PROJ1
#
# ОЖИДАЕМАЯ СТРУКТУРА:
#   BASE_DIR/
#   ├── scripts/
#   │   └── analyze-expr.sh
#   ├── unpacked/
#   │   └── PROJ1/
#   │       ├── src/
#   │       └── bin/
#   ├── logs/
#   │   └── analyze-expr/
#   ├── results/
#   │   └── PROJ1/
#   │       └── expr/
#   │           ├── extensions_src.txt
#   │           ├── extensions_bin.txt
#   │           ├── extensions_src.json
#   │           ├── extensions_bin.json
#   │           └── binaries_in_src.txt
#   └── result-expr.json
#
# ЗАВИСИМОСТИ:
#   bash, find, file, sort, awk, python3 (для JSON)
# =============================================================================

set -uo pipefail

# =============================================================================
# НАСТРАИВАЕМЫЕ ПУТИ
# =============================================================================
BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
UNPACKED_DIR="$BASE_DIR/unpacked"
RESULTS_DIR="$BASE_DIR/results"
LOG_DIR="$BASE_DIR/logs/analyze-expr"
GLOBAL_JSON="$BASE_DIR/result-expr.json"
# =============================================================================

# =============================================================================
# РАСШИРЕНИЯ БИНАРНЫХ/СКОМПИЛИРОВАННЫХ ФАЙЛОВ
# Файлы с этими расширениями добавляются в binaries_in_src.txt
# наряду с исполняемыми найденными по магическим байтам (file(1))
# =============================================================================
declare -A BINARY_EXTENSIONS=(
    # Java/JVM
    [".class"]=1  [".jar"]=1  [".war"]=1  [".jmod"]=1
    [".aar"]=1    [".ear"]=1  [".sar"]=1
    # Python скомпилированный
    [".pyc"]=1    [".pyd"]=1  [".pyo"]=1  [".egg"]=1  [".whl"]=1
    # C/C++ скомпилированный
    [".so"]=1     [".dll"]=1  [".exe"]=1  [".a"]=1    [".o"]=1
    [".lib"]=1    [".obj"]=1  [".pdb"]=1  [".ko"]=1
    # Web/Node скомпилированный
    # .map убран — source map это текстовый JSON, не бинарник;
    # файлы локализации *.map тоже не бинарники
    # .tsbuildinfo убран — это JSON кэш TypeScript компилятора, не бинарник
    [".node"]=1   [".wasm"]=1 [".swf"]=1
    # Rust
    [".rlib"]=1
    # Android/iOS
    [".dex"]=1    [".apk"]=1  [".ipa"]=1
    # .NET
    [".nupkg"]=1
    # Linux пакеты
    [".rpm"]=1
)
# =============================================================================

# Размер батча для вызова file(1) — сколько файлов за раз
FILE_BATCH_SIZE=400

# --- Параметры по умолчанию ---
SINGLE_PROJECT=""
NO_REWRITE=false
MAX_PARALLEL=1

# --- Разбор аргументов ---
while [[ $# -gt 0 ]]; do
    case $1 in
        --single-project)
            if [[ -n "${2:-}" ]]; then
                SINGLE_PROJECT="$2"
                shift 2
            else
                echo "[ERROR] --single-project requires a project name"
                exit 1
            fi
            ;;
        --no-rewrite)
            NO_REWRITE=true
            shift
            ;;
        -j|--parallel)
            if [[ -n "${2:-}" && "${2:-}" =~ ^[0-9]+$ ]]; then
                MAX_PARALLEL="$2"
                shift 2
            else
                echo "[ERROR] -j/--parallel requires a number argument"
                exit 1
            fi
            ;;
        -h|--help)
            grep "^#" "$0" | head -60 | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "[ERROR] Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

mkdir -p "$LOG_DIR"

RUN_LOG="$LOG_DIR/run_$(date '+%Y%m%d_%H%M%S').log"

log() {
    echo "$1" | tee -a "$RUN_LOG"
}

log_error() {
    echo "[ERROR] $1" | tee -a "$RUN_LOG"
}

# =============================================================================
# Возвращает расширение файла в нижнем регистре.
# =============================================================================
get_extension() {
    local file="$1"
    local base="${file##*/}"
    local lower="${base,,}"

    case "$lower" in
        *.tar.gz)   echo ".tar.gz";   return ;;
        *.tar.bz2)  echo ".tar.bz2";  return ;;
        *.tar.xz)   echo ".tar.xz";   return ;;
        *.tar.zst)  echo ".tar.zst";  return ;;
        *.tar.z)    echo ".tar.Z";    return ;;
        *.tar.lz4)  echo ".tar.lz4";  return ;;
        *.tar.lzma) echo ".tar.lzma"; return ;;
    esac

    local ext="${lower##*.}"
    if [[ "$ext" == "$lower" ]]; then
        echo "no_extension"
    else
        echo ".${ext}"
    fi
}

# =============================================================================
# Проверяет является ли расширение бинарным (по BINARY_EXTENSIONS)
# =============================================================================
is_binary_extension() {
    local file="$1"
    local ext
    ext=$(get_extension "$file")
    [[ -n "${BINARY_EXTENSIONS[$ext]:-}" ]]
}

# =============================================================================
# Подсчёт расширений в директории.
# =============================================================================
count_extensions() {
    local dir="$1"
    local tmpfile
    tmpfile=$(mktemp)
    local total=0

    while IFS= read -r -d '' f; do
        get_extension "$f"
        (( total++ )) || true
    done < <(find "$dir" -type f -print0 2>/dev/null) >> "$tmpfile"

    sort "$tmpfile" | uniq -c | sort -rn | awk '{print $1, $2}'
    echo "TOTAL $total"
    rm -f "$tmpfile"
}

# =============================================================================
# Батчинг file(1) для поиска исполняемых файлов.
# =============================================================================
run_file_batch() {
    local -a files=("$@")
    [[ ${#files[@]} -eq 0 ]] && return
    file "${files[@]}" 2>/dev/null | while IFS= read -r line; do
        local path="${line%%: *}"
        local ftype="${line#*: }"
        printf '%s\t%s\n' "$path" "$ftype"
    done
}

# =============================================================================
# Проверяет является ли тип исполняемым (по выводу file)
# =============================================================================
is_executable_by_magic() {
    local type="$1"
    [[ "$type" == ELF*executable* ]]       && return 0
    [[ "$type" == ELF*shared\ object* ]]   && return 0
    [[ "$type" == ELF*pie\ executable* ]]  && return 0
    [[ "$type" == *"PE32 executable"* ]]   && return 0
    [[ "$type" == *"PE32+ executable"* ]]  && return 0
    [[ "$type" == *"MS-DOS executable"* ]] && return 0
    return 1
}

# =============================================================================
# Записывает TXT файл расширений
# =============================================================================
write_extensions_txt() {
    local output_file="$1"
    shift
    local -a counts=("$@")

    {
        printf "%-10s %s\n" "COUNT" "EXTENSION"
        printf "%-10s %s\n" "----------" "---------"
        for line in "${counts[@]}"; do
            local cnt ext
            cnt="${line%% *}"
            ext="${line#* }"
            printf "%-10s %s\n" "$cnt" "$ext"
        done
    } > "$output_file"
}

# =============================================================================
# Записывает JSON файл расширений.
# =============================================================================
write_extensions_json() {
    local output_file="$1"
    local project_name="$2"
    local subdir="$3"
    local subdir_path="$4"
    local total="$5"
    local analyzed_at="$6"
    shift 6
    local -a counts=("$@")

    local tmp_data
    tmp_data=$(mktemp)
    for line in "${counts[@]}"; do
        local cnt ext
        cnt="${line%% *}"
        ext="${line#* }"
        printf '%s\t%s\n' "$ext" "$cnt"
    done > "$tmp_data"

    python3 - "$tmp_data" "$output_file" \
        "$project_name" "$subdir" "$subdir_path" \
        "$total" "$analyzed_at" << 'PYEOF'
import sys, json

tmp_data, output_file, project_name, subdir, subdir_path, total, analyzed_at = sys.argv[1:]

extensions = {}
with open(tmp_data) as f:
    for line in f:
        line = line.rstrip('\n')
        if not line:
            continue
        parts = line.split('\t', 1)
        if len(parts) == 2:
            extensions[parts[0]] = int(parts[1])

data = {
    'project':     project_name,
    'subdir':      subdir,
    'path':        subdir_path,
    'analyzed_at': analyzed_at,
    'total_files': int(total),
    'extensions':  extensions,
}

with open(output_file, 'w') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)
PYEOF

    rm -f "$tmp_data"
}

# =============================================================================
# Обновляет глобальный result-expr.json.
# =============================================================================
update_global_json() {
    local project_name="$1"
    local project_path="$2"
    local analyzed_at="$3"
    local src_total="$4"
    local bin_total="$5"
    local src_ext_json="$6"
    local bin_ext_json="$7"

    {
        flock 200

        python3 - "$GLOBAL_JSON" "$src_ext_json" "$bin_ext_json" \
            "$project_name" "$project_path" "$analyzed_at" \
            "$src_total" "$bin_total" << 'PYEOF'
import sys, json
from datetime import datetime

global_json, src_ext_json, bin_ext_json, \
    project_name, project_path, analyzed_at, \
    src_total, bin_total = sys.argv[1:]

data = {}
try:
    with open(global_json) as f:
        data = json.load(f)
except Exception:
    pass

data.setdefault('generated_at', '')
data.setdefault('projects', {})

src_extensions = {}
bin_extensions = {}

try:
    with open(src_ext_json) as f:
        src_extensions = json.load(f).get('extensions', {})
except Exception:
    pass

try:
    with open(bin_ext_json) as f:
        bin_extensions = json.load(f).get('extensions', {})
except Exception:
    pass

data['projects'][project_name] = {
    'path':        project_path,
    'analyzed_at': analyzed_at,
    'src': {
        'total_files': int(src_total),
        'extensions':  src_extensions,
    },
    'bin': {
        'total_files': int(bin_total),
        'extensions':  bin_extensions,
    },
}

data['generated_at'] = datetime.now().isoformat()

tmp = global_json + '.tmp'
with open(tmp, 'w') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

import os
os.replace(tmp, global_json)
PYEOF

    } 200>"${GLOBAL_JSON}.lock"
}

# =============================================================================
# Проверяет есть ли проект в result-expr.json
# =============================================================================
project_exists_in_global() {
    local project_name="$1"
    [[ ! -f "$GLOBAL_JSON" ]] && return 1
    python3 - "$GLOBAL_JSON" "$project_name" << 'PYEOF'
import sys, json
try:
    with open(sys.argv[1]) as f:
        data = json.load(f)
    sys.exit(0 if sys.argv[2] in data.get('projects', {}) else 1)
except Exception:
    sys.exit(1)
PYEOF
}

# =============================================================================
# Расширения файлов которые заведомо не являются исполняемыми.
# Файлы с этими расширениями не попадают в батч file(1) — экономия времени
# на проектах с сотнями тысяч файлов.
# =============================================================================
is_safe_extension() {
    local ext="$1"
    case "$ext" in
        # Исходный код
        .java|.kt|.scala|.groovy|.clj|.cljs)          return 0 ;;
        .py|.rb|.php|.pl|.pm|.lua|.tcl|.r)            return 0 ;;
        .js|.ts|.jsx|.tsx|.mjs|.cjs|.coffee)          return 0 ;;
        .c|.cc|.cpp|.cxx|.h|.hh|.hpp|.hxx)            return 0 ;;
        .cs|.vb|.fs|.fsx|.swift|.m|.mm)               return 0 ;;
        .go|.rs|.dart|.ex|.exs|.erl|.hrl)             return 0 ;;
        .sh|.bash|.zsh|.fish|.ksh|.csh|.tcsh)         return 0 ;;
        .ps1|.psm1|.psd1|.bat|.cmd)                   return 0 ;;
        # Разметка и конфиги
        .xml|.xsd|.xsl|.xslt|.dtd|.wsdl|.pom)        return 0 ;;
        .json|.jsonc|.json5)                           return 0 ;;
        .yaml|.yml|.toml|.ini|.cfg|.conf|.config)     return 0 ;;
        .properties|.env|.env.*)                       return 0 ;;
        .html|.htm|.xhtml|.vue|.svelte)               return 0 ;;
        .css|.scss|.sass|.less|.styl)                 return 0 ;;
        # Текст и документация
        .txt|.md|.rst|.adoc|.textile|.wiki)           return 0 ;;
        .pdf|.doc|.docx|.xls|.xlsx|.ppt|.pptx|.odt)  return 0 ;;
        .csv|.tsv|.sql|.log|.diff|.patch)             return 0 ;;
        # Картинки
        .jpg|.jpeg|.png|.gif|.bmp|.svg|.ico|.webp)   return 0 ;;
        .tiff|.tif|.psd|.ai|.eps|.raw)               return 0 ;;
        # Медиа
        .mp3|.mp4|.wav|.avi|.mkv|.mov|.flac|.ogg)    return 0 ;;
        # Шрифты
        .ttf|.otf|.woff|.woff2|.eot)                 return 0 ;;
        # Прочее заведомо безопасное
        .gradle|.maven|.sbt|.lock|.sum)               return 0 ;;
        .gitignore|.gitattributes|.editorconfig)      return 0 ;;
        .tsbuildinfo|.map)                            return 0 ;;
        *)                                             return 1 ;;
    esac
}

# =============================================================================
# Поиск исполняемых и бинарных файлов в директории.
# Результат пишется в output_file.
#
# Три корзины за ОДИН проход по файлам:
#   1. Расширение в BINARY_EXTENSIONS  → BINARY_EXT, file(1) не нужен
#   2. Расширение "безопасное"          → пропустить, file(1) не нужен
#   3. Остальные (нет расш. / неизв.)   → батч для file(1)
#
# Колонка TYPE:
#   ELF / PE32 / MSDOS  — исполняемый по магическим байтам
#   BINARY_EXT          — бинарный по расширению (из BINARY_EXTENSIONS)
# =============================================================================
find_executables() {
    local dir="$1"
    local project_dir="$2"
    local output_file="$3"
    local _bin_count=0

    # Временный файл для BINARY_EXT результатов (пишем сразу)
    local tmp_binary_ext tmp_magic
    tmp_binary_ext=$(mktemp)
    tmp_magic=$(mktemp)

    # Батч для file(1) — накапливаем файлы требующие проверки магических байт
    local -a magic_batch=()
    local magic_batch_size=0

    flush_magic_batch() {
        [[ ${#magic_batch[@]} -eq 0 ]] && return
        while IFS=$'\t' read -r fpath ftype; do
            if is_executable_by_magic "$ftype"; then
                local short_type="ELF"
                [[ "$ftype" == *"PE32"* ]]   && short_type="PE32"
                [[ "$ftype" == *"MS-DOS"* ]] && short_type="MSDOS"
                printf "%-12s  %s\n" "$short_type" "${fpath#$project_dir/}" >> "$tmp_magic"
                (( _bin_count++ )) || true
            fi
        done < <(run_file_batch "${magic_batch[@]}")
        magic_batch=()
        magic_batch_size=0
    }

    # Один проход по всем файлам — сортируем по корзинам
    while IFS= read -r -d '' f; do
        local ext
        ext=$(get_extension "$f")

        if [[ -n "${BINARY_EXTENSIONS[$ext]:-}" ]]; then
            # Корзина 1: бинарный по расширению — сразу пишем, file(1) не нужен
            printf "%-12s  %s\n" "BINARY_EXT" "${f#$project_dir/}" >> "$tmp_binary_ext"
            (( _bin_count++ )) || true

        elif is_safe_extension "$ext"; then
            # Корзина 2: заведомо не исполняемый — пропускаем
            true

        else
            # Корзина 3: неизвестное расширение или нет расширения — в батч file(1)
            magic_batch+=("$f")
            (( magic_batch_size++ )) || true
            if (( magic_batch_size >= FILE_BATCH_SIZE )); then
                flush_magic_batch
            fi
        fi

    done < <(find "$dir" -type f -print0 2>/dev/null)

    # Сбрасываем остаток батча
    flush_magic_batch

    # Собираем итоговый файл
    {
        printf "%-12s  %s\n" "TYPE" "PATH"
        printf "%-12s  %s\n" "------------" "----"
        cat "$tmp_binary_ext"
        cat "$tmp_magic"
    } > "$output_file"

    rm -f "$tmp_binary_ext" "$tmp_magic"

    echo "$_bin_count"
}

# =============================================================================
# Обработка одного проекта
# =============================================================================
process_project() {
    local project_name="$1"
    local project_dir="$UNPACKED_DIR/$project_name"

    log "=== Processing project: $project_name ==="

    if [[ "$NO_REWRITE" == true ]]; then
        if project_exists_in_global "$project_name"; then
            log "[$project_name] [INFO] Already in result-expr.json, skipping (--no-rewrite)"
            return 0
        fi
    fi

    local src_dir="$project_dir/src"
    local bin_dir="$project_dir/bin"
    local expr_dir="$RESULTS_DIR/$project_name/expr"
    mkdir -p "$expr_dir"

    local analyzed_at
    analyzed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

    # =========================================================================
    # SRC — подсчёт расширений
    # =========================================================================
    local src_total=0
    local src_counts=()

    if [[ -d "$src_dir" ]]; then
        log "[$project_name] [INFO] Counting extensions in src..."
        while IFS= read -r line; do
            if [[ "$line" == TOTAL\ * ]]; then
                src_total="${line#TOTAL }"
            else
                src_counts+=("$line")
            fi
        done < <(count_extensions "$src_dir")

        log "[$project_name] [INFO] src total files: $src_total"

        write_extensions_txt "$expr_dir/extensions_src.txt" \
            "${src_counts[@]+"${src_counts[@]}"}"
        log "[$project_name] [INFO] Written: extensions_src.txt"

        write_extensions_json \
            "$expr_dir/extensions_src.json" \
            "$project_name" "src" "$src_dir" \
            "$src_total" "$analyzed_at" \
            "${src_counts[@]+"${src_counts[@]}"}"
        log "[$project_name] [INFO] Written: extensions_src.json"
    else
        log "[$project_name] [WARN] src directory not found: $src_dir"
        echo -e "COUNT      EXTENSION\n---------- ---------" > "$expr_dir/extensions_src.txt"
        echo '{"project":"'"$project_name"'","subdir":"src","path":"'"$src_dir"'","analyzed_at":"'"$analyzed_at"'","total_files":0,"extensions":{}}' \
            > "$expr_dir/extensions_src.json"
    fi

    # =========================================================================
    # BIN — подсчёт расширений
    # =========================================================================
    local bin_total=0
    local bin_counts=()

    if [[ -d "$bin_dir" ]]; then
        log "[$project_name] [INFO] Counting extensions in bin..."
        while IFS= read -r line; do
            if [[ "$line" == TOTAL\ * ]]; then
                bin_total="${line#TOTAL }"
            else
                bin_counts+=("$line")
            fi
        done < <(count_extensions "$bin_dir")

        log "[$project_name] [INFO] bin total files: $bin_total"

        write_extensions_txt "$expr_dir/extensions_bin.txt" \
            "${bin_counts[@]+"${bin_counts[@]}"}"
        log "[$project_name] [INFO] Written: extensions_bin.txt"

        write_extensions_json \
            "$expr_dir/extensions_bin.json" \
            "$project_name" "bin" "$bin_dir" \
            "$bin_total" "$analyzed_at" \
            "${bin_counts[@]+"${bin_counts[@]}"}"
        log "[$project_name] [INFO] Written: extensions_bin.json"
    else
        log "[$project_name] [WARN] bin directory not found: $bin_dir"
        echo -e "COUNT      EXTENSION\n---------- ---------" > "$expr_dir/extensions_bin.txt"
        echo '{"project":"'"$project_name"'","subdir":"bin","path":"'"$bin_dir"'","analyzed_at":"'"$analyzed_at"'","total_files":0,"extensions":{}}' \
            > "$expr_dir/extensions_bin.json"
    fi

    # =========================================================================
    # SRC — поиск исполняемых и бинарных файлов
    # =========================================================================
    local binaries_file="$expr_dir/binaries_in_src.txt"
    local bin_count=0

    if [[ -d "$src_dir" ]]; then
        log "[$project_name] [INFO] Searching for executables and binaries in src..."
        bin_count=$(find_executables "$src_dir" "$project_dir" "$binaries_file")
        log "[$project_name] [INFO] Executables/binaries found in src: $bin_count"
        log "[$project_name] [INFO] Written: binaries_in_src.txt"
    else
        printf "%-12s  %s\n%-12s  %s\n" "TYPE" "PATH" "------------" "----" \
            > "$binaries_file"
        log "[$project_name] [WARN] src not found, binaries_in_src.txt is empty"
    fi

    # =========================================================================
    # Обновляем глобальный result-expr.json
    # =========================================================================
    log "[$project_name] [INFO] Updating result-expr.json..."
    update_global_json \
        "$project_name" \
        "$project_dir" \
        "$analyzed_at" \
        "$src_total" \
        "$bin_total" \
        "$expr_dir/extensions_src.json" \
        "$expr_dir/extensions_bin.json"

    log "=== Done: $project_name ==="
}

# =============================================================================
# ОСНОВНОЙ ЦИКЛ
# =============================================================================

if [[ ! -d "$UNPACKED_DIR" ]]; then
    echo "[ERROR] Unpacked directory not found: $UNPACKED_DIR"
    exit 1
fi

projects=()

if [[ -n "$SINGLE_PROJECT" ]]; then
    if [[ -d "$UNPACKED_DIR/$SINGLE_PROJECT" ]]; then
        projects=("$SINGLE_PROJECT")
    else
        echo "[ERROR] Project not found: $UNPACKED_DIR/$SINGLE_PROJECT"
        exit 1
    fi
else
    for dir in "$UNPACKED_DIR"/*/; do
        [[ -d "$dir" ]] || continue
        projects+=("$(basename "$dir")")
    done

    if [[ ${#projects[@]} -eq 0 ]]; then
        echo "[ERROR] No projects found in: $UNPACKED_DIR"
        exit 1
    fi
fi

log "[INFO] Projects to analyze: ${#projects[@]}"
log "[INFO] Projects: ${projects[*]}"
log "[INFO] Log: $RUN_LOG"

pids=()
exit_code=0

for project_name in "${projects[@]}"; do
    if [[ ${#pids[@]} -ge $MAX_PARALLEL ]]; then
        log "[INFO] Reached max parallel jobs ($MAX_PARALLEL), waiting..."
        wait -n 2>/dev/null || wait
        new_pids=()
        for pid in "${pids[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                new_pids+=("$pid")
            fi
        done
        pids=("${new_pids[@]}")
    fi

    log "[INFO] Starting: $project_name (running: ${#pids[@]}/$MAX_PARALLEL)"
    process_project "$project_name" &
    pids+=($!)
done

log "[INFO] Waiting for all remaining projects to complete..."
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
        log "[ERROR] Process $pid failed"
        exit_code=1
    fi
done

rm -f "${GLOBAL_JSON}.lock"

log ""
log "=========================================="
log "Analysis complete!"
log "=========================================="
log "Results : $RESULTS_DIR"
log "Global  : $GLOBAL_JSON"
log "Log     : $RUN_LOG"

exit "$exit_code"
