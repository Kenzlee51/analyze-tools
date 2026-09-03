#!/bin/bash
#
# =============================================================================
# unpack.sh — Скрипт рекурсивной распаковки архивов
# =============================================================================
#
# ОПИСАНИЕ:
#   Рекурсивно распаковывает все архивы в директории src/PROJ
#   в директорию unpacked/PROJ. Поддерживает вложенные архивы любой глубины.
#   Каждый архив распаковывается в директорию АРХИВ_dir рядом с ним.
#
# ИСПОЛЬЗОВАНИЕ:
#   ./unpack.sh [OPTIONS]
#
# ОПЦИИ:
#   -p, --single-project NAME   Обработать только один указанный проект
#   -a, --all                   Распаковывать все архивы включая исключения
#                               (по умолчанию .jar не распаковывается)
#   -j, --parallel N            Количество параллельных проектов (default: 1)
#                               Рекомендации: HDD → 1, SSD → 2-4, NVMe → 4-8
#   -b, --batch-size N          Файлов в один вызов file(1) (default: 500)
#   -s, --skip-path PATTERN     Regex для пропуска путей (можно несколько раз)
#                               Пример: -s node_modules -s /m2_repo
#   -f, --full-file-check       Проверять все файлы через file(1) независимо
#                               от расширения (медленно но надёжно)
#   -c, --clean                 Распаковывать без суффикса _dir, удалять архивы
#   --filter LANG[,LANG...]     Оставить только исходные тексты указанных языков
#                               Пример: --filter python,java
#                               Список доступных языков см. ниже
#   --filter-bin LANG[,LANG...] Оставить только бинарные файлы указанных языков
#                               Пример: --filter-bin cpp,go
#                               Список доступных языков см. ниже
#   -h, --help                  Показать эту справку
#
# ЗАВИСИМОСТИ ПРОВЕРЯЮТСЯ АВТОМАТИЧЕСКИ:
#   При каждом запуске скрипт сам проверяет наличие всех необходимых утилит
#   и сообщает, каких не хватает (для обязательных — с остановкой запуска,
#   для опциональных — только предупреждением).
#
# ПРИМЕРЫ:
#   ./unpack.sh
#   ./unpack.sh -p PROJ1
#   ./unpack.sh --single-project PROJ1
#   ./unpack.sh -j 4 -p PROJ1
#   ./unpack.sh -a
#   ./unpack.sh -s node_modules
#   ./unpack.sh -f -j 2
#   ./unpack.sh --clean --filter python
#   ./unpack.sh --filter cpp --filter-bin cpp
#   ./unpack.sh --filter js,java --filter-bin cpp,go
#
# ОЖИДАЕМАЯ СТРУКТУРА:
#   BASE_DIR/
#   ├── scripts/
#   │   └── unpack.sh
#   ├── src/
#   │   ├── PROJ1/          ← исходные архивы
#   │   └── PROJ2/
#   ├── unpacked/
#   │   ├── PROJ1/          ← результат распаковки
#   │   └── PROJ2/
#   └── logs/
#       └── unpack/
#           ├── PROJ1/
#           │   └── PROJ1.log
#           └── PROJ2/
#               └── PROJ2.log
#
# ЗАВИСИМОСТИ:
#   Обязательные : bash 4+, file, find, stat, sort, grep, awk, date, bc
#                  tar, gzip, bzip2, xz, unzip, p7zip-full
#   Опциональные : unrar, zstd, lz4, rpm2cpio+cpio, dpkg-deb,
#                  cabextract, msitools, squashfs-tools, libarchive-tools
#
# =============================================================================

set -euo pipefail

# =============================================================================
# НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
# =============================================================================
UNPACK_ALL=false
MAX_PARALLEL=1
UNPACK_CLEAN=false
FILTER_LANGS=""
FILTER_BIN_LANGS=""

# Расширения которые НЕ распаковываются по умолчанию.
# При --all эти ограничения снимаются.
SKIP_EXTENSIONS=(jar)
FILE_BATCH_SIZE=500
SKIP_PATH_PATTERN=""
FULL_FILE_CHECK=false
SINGLE_PROJECT=""

AMBIGUOUS_EXTENSIONS=(
    bin dat img image raw dump pak
    data bak old orig tmp temp
)

# =============================================================================
# ТАБЛИЦЫ РАСШИРЕНИЙ ДЛЯ ФИЛЬТРОВ
# =============================================================================

# Исходные тексты (source code)
declare -A SOURCE_EXTS=(
    ["python"]=".py .pyx .pxd .pyi"
    ["java"]=".java .groovy .kt .scala"
    ["cpp"]=".c .h .cpp .cc .cxx .hpp .hh .hxx .c++ .h++"
    ["csharp"]=".cs .csx"
    ["go"]=".go"
    ["rust"]=".rs"
    ["javascript"]=".js .mjs .cjs .jsx"
    ["typescript"]=".ts .tsx"
    ["php"]=".php .php3 .php4 .php5 .php7 .phtml"
    ["ruby"]=".rb .rake .gemspec .ru"
    ["swift"]=".swift"
    ["kotlin"]=".kt .kts"
    ["scala"]=".scala .sc"
    ["perl"]=".pl .pm .t .pod"
    ["lua"]=".lua"
    ["r"]=".r .rdata .rds"
    ["matlab"]=".m .mat"
    ["sql"]=".sql .ddl .dml"
    ["html"]=".html .htm .xhtml .xml .svg"
    ["css"]=".css .scss .sass .less .styl"
    ["shell"]=".sh .bash .zsh .fish .csh .ksh"
    ["make"]="Makefile .mk"
    ["cmake"]="CMakeLists.txt .cmake"
    ["docker"]="Dockerfile .dockerignore"
    ["yaml"]=".yaml .yml"
    ["json"]=".json .jsonl"
    ["toml"]=".toml"
    ["ini"]=".ini .cfg .conf"
    ["markdown"]=".md .markdown .mdown"
    ["tex"]=".tex .cls .sty .bib .bst"
    ["fortran"]=".f .for .f90 .f95 .f03 .f08"
    ["vb"]=".vb .bas .cls"
    ["powershell"]=".ps1 .psm1 .psd1"
    ["julia"]=".jl"
    ["erlang"]=".erl .hrl .escript"
    ["elixir"]=".ex .exs"
    ["clojure"]=".clj .cljs .cljc .edn"
    ["haskell"]=".hs .lhs .cabal"
    ["dart"]=".dart"
    ["nim"]=".nim .nimble"
    ["crystal"]=".cr"
    ["zig"]=".zig"
    ["v"]=".v"
    ["ada"]=".adb .ads"
    ["pascal"]=".pas .pp .inc"
    ["delphi"]=".pas .dpr .dpk"
    ["objectivec"]=".m .mm .h"
    ["vue"]=".vue"
    ["svelte"]=".svelte"
    ["webassembly"]=".wat .wast"
    ["protobuf"]=".proto"
    ["thrift"]=".thrift"
    ["graphql"]=".graphql .gql"
    ["terraform"]=".tf .tfvars"
    ["vim"]=".vim .vimrc"
    ["emacs"]=".el .emacs"
)

# Бинарные/скомпилированные файлы
declare -A BINARY_EXTS=(
    ["cpp"]=".o .obj .a .so .dylib .dll .exe"
    ["c"]=".o .obj .a .so .dylib .dll .exe"
    ["rust"]=".rlib .rmeta .a .so .dylib .dll .exe"
    ["go"]=".a .so .dylib .dll .exe"
    ["java"]=".class .jar .war .ear .jmod"
    ["kotlin"]=".class .jar .klib"
    ["scala"]=".class .jar .war"
    ["csharp"]=".dll .exe .pdb"
    ["python"]=".pyc .pyo .pyd .so"
    ["haskell"]=".o .hi .a .so .dylib .dll"
    ["fortran"]=".o .mod .a .so .dylib .dll .exe"
    ["swift"]=".swiftmodule .o .a .so .dylib .dll .exe"
    ["zig"]=".o .obj .a .so .dylib .dll .exe"
    ["nim"]=".o .a .so .dylib .dll .exe"
    ["d"]=".o .obj .a .so .dll .exe"
    ["v"]=".o .a .so .dylib .dll .exe"
    ["crystal"]=".o .a .so .dylib .dll .exe"
    ["ada"]=".o .ali .a .so .dylib .dll .exe"
    ["pascal"]=".o .ppu .a .so .dylib .dll .exe"
    ["delphi"]=".dcu .dcp .bpl .dll .exe"
    ["ocaml"]=".cmo .cmx .cma .cmxa .o .a .so"
    ["erlang"]=".beam .app"
    ["elixir"]=".beam .app"
    ["clojure"]=".class .jar"
    ["julia"]=".ji .so .dylib .dll"
    ["lua"]=".so .dylib .dll"
    ["perl"]=".so .dylib .dll"
    ["ruby"]=".so .bundle .dylib .dll"
    ["php"]=".so .dylib .dll"
    ["r"]=".so .dylib .dll .rdx"
    ["matlab"]=".mex .mexw64 .mexmac"
    ["dart"]=".aot .snapshot .so .dylib .dll .exe"
    ["webassembly"]=".wasm"
)

# =============================================================================

# =============================================================================
# ПРОВЕРКА ЗАВИСИМОСТЕЙ
# =============================================================================

# Обязательные утилиты: утилита → пакет для установки
declare -A REQUIRED_TOOLS=(
    ["file"]="file"
    ["find"]="findutils"
    ["stat"]="coreutils"
    ["tar"]="tar"
    ["gunzip"]="gzip"
    ["bunzip2"]="bzip2"
    ["unxz"]="xz-utils"
    ["unzip"]="unzip"
    ["7z"]="p7zip-full"
    ["bc"]="bc"
)

# Опциональные утилиты: утилита → пакет
declare -A OPTIONAL_TOOLS=(
    ["unrar"]="unrar"
    ["unzstd"]="zstd"
    ["lz4"]="lz4"
    ["rpm2cpio"]="rpm2cpio"
    ["cpio"]="cpio"
    ["dpkg-deb"]="dpkg"
    ["cabextract"]="cabextract"
    ["msiextract"]="msitools"
    ["unsquashfs"]="squashfs-tools"
    ["bsdtar"]="libarchive-tools"
)

check_dependencies() {
    local missing_required=()
    local missing_optional=()
    local missing_pkgs_required=()
    local missing_pkgs_optional=()

    echo "[DEPS] Checking required dependencies..."
    for tool in "${!REQUIRED_TOOLS[@]}"; do
        if ! command -v "$tool" &>/dev/null; then
            missing_required+=("$tool")
            missing_pkgs_required+=("${REQUIRED_TOOLS[$tool]}")
        fi
    done

    echo "[DEPS] Checking optional dependencies..."
    for tool in "${!OPTIONAL_TOOLS[@]}"; do
        if ! command -v "$tool" &>/dev/null; then
            missing_optional+=("$tool")
            missing_pkgs_optional+=("${OPTIONAL_TOOLS[$tool]}")
        fi
    done

    local has_errors=false

    if [[ ${#missing_required[@]} -gt 0 ]]; then
        echo ""
        echo "[DEPS] [ERROR] Missing REQUIRED tools: ${missing_required[*]}"
        echo "[DEPS] Install with:"
        local unique_pkgs
        unique_pkgs=$(printf '%s\n' "${missing_pkgs_required[@]}" | sort -u | tr '\n' ' ')
        echo ""
        echo "    sudo apt install ${unique_pkgs}"
        echo ""
        has_errors=true
    else
        echo "[DEPS] [OK] All required tools are present."
    fi

    if [[ ${#missing_optional[@]} -gt 0 ]]; then
        echo ""
        echo "[DEPS] [WARN] Missing OPTIONAL tools: ${missing_optional[*]}"
        echo "[DEPS] These are needed only for specific archive formats."
        echo "[DEPS] Install with:"
        local unique_opt_pkgs
        unique_opt_pkgs=$(printf '%s\n' "${missing_pkgs_optional[@]}" | sort -u | tr '\n' ' ')
        echo ""
        echo "    sudo apt install ${unique_opt_pkgs}"
        echo ""
    else
        echo "[DEPS] [OK] All optional tools are present."
    fi

    if [[ "$has_errors" == true ]]; then
        return 1
    fi
    return 0
}

# =============================================================================
# Regex архивных расширений
# =============================================================================
_ARCHIVE_NAMES=(
    "*.tar.gz" "*.tar.bz2" "*.tar.xz" "*.tar.zst"
    "*.tar.z"  "*.tar.lz4" "*.tar.lzma"
    "*.tgz" "*.tbz2" "*.txz"
    "*.zip" "*.jar" "*.war" "*.ear" "*.whl" "*.egg"
    "*.apk" "*.ipa" "*.xpi" "*.crx" "*.nupkg" "*.epub" "*.aar"
    "*.gz" "*.bz2" "*.xz" "*.zst" "*.lz4" "*.lzma" "*.z"
    "*.rar" "*.7z" "*.tar"
    "*.iso" "*.rpm" "*.deb" "*.cab" "*.msi" "*.squashfs" "*.pkg"
)

_build_find_name_args() {
    local -a args=()
    local first=true
    for pat in "${_ARCHIVE_NAMES[@]}"; do
        if $first; then
            args+=( -iname "$pat" )
            first=false
        else
            args+=( -o -iname "$pat" )
        fi
    done
    for ext in "${AMBIGUOUS_EXTENSIONS[@]}"; do
        args+=( -o -iname "*.$ext" )
    done
    printf '%s\0' "${args[@]}"
}

mapfile -d '' _FIND_NAME_ARGS < <(_build_find_name_args)

SCRIPT_START_TS=$(date +%s%N)

format_duration() {
    local ns=$1
    local ms=$(( ns / 1000000 ))
    if (( ms < 1000 )); then
        echo "${ms}ms"
    elif (( ms < 60000 )); then
        LC_ALL=C printf "%.2fs" "$(LC_ALL=C echo "scale=2; $ms/1000" | bc)"
    else
        local s=$(( ms / 1000 ))
        printf "%dm%02ds" "$(( s / 60 ))" "$(( s % 60 ))"
    fi
}

now_ns() { date +%s%N; }

# =============================================================================
# РАЗБОР АРГУМЕНТОВ
# =============================================================================
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--single-project)
            if [[ -n "${2:-}" ]]; then
                SINGLE_PROJECT="$2"
                echo "[INFO] Single project mode: $SINGLE_PROJECT"
                shift 2
            else
                echo "[ERROR] -p/--single-project requires a project name"
                exit 1
            fi
            ;;
        -a|--all)
            UNPACK_ALL=true
            echo "[INFO] Mode: --all (unpacking everything including ${SKIP_EXTENSIONS[*]})"
            shift
            ;;
        -j|--parallel)
            if [[ -n "${2:-}" && "${2:-}" =~ ^[0-9]+$ ]]; then
                MAX_PARALLEL="$2"
                echo "[INFO] Max parallel projects: $MAX_PARALLEL"
                shift 2
            else
                echo "[ERROR] -j/--parallel requires a number argument"
                exit 1
            fi
            ;;
        -b|--batch-size)
            if [[ -n "${2:-}" && "${2:-}" =~ ^[0-9]+$ ]]; then
                FILE_BATCH_SIZE="$2"
                echo "[INFO] File batch size: $FILE_BATCH_SIZE"
                shift 2
            else
                echo "[ERROR] -b/--batch-size requires a number argument"
                exit 1
            fi
            ;;
        -s|--skip-path)
            if [[ -n "${2:-}" ]]; then
                if [[ -z "$SKIP_PATH_PATTERN" ]]; then
                    SKIP_PATH_PATTERN="$2"
                else
                    SKIP_PATH_PATTERN="$SKIP_PATH_PATTERN|$2"
                fi
                echo "[INFO] Skip path pattern: $2"
                shift 2
            else
                echo "[ERROR] -s/--skip-path requires a pattern argument"
                exit 1
            fi
            ;;
        -f|--full-file-check)
            FULL_FILE_CHECK=true
            echo "[INFO] Full file check enabled"
            shift
            ;;
        -c|--clean)
            UNPACK_CLEAN=true
            echo "[INFO] Clean mode enabled (no _dir suffix, remove archives)"
            shift
            ;;
        --filter)
            if [[ -n "${2:-}" ]]; then
                FILTER_LANGS="$2"
                echo "[INFO] Filter source languages: $FILTER_LANGS"
                shift 2
            else
                echo "[ERROR] --filter requires language list argument"
                exit 1
            fi
            ;;
        --filter-bin)
            if [[ -n "${2:-}" ]]; then
                FILTER_BIN_LANGS="$2"
                echo "[INFO] Filter binary languages: $FILTER_BIN_LANGS"
                shift 2
            else
                echo "[ERROR] --filter-bin requires language list argument"
                exit 1
            fi
            ;;
        -h|--help)
            # Показываем основную справку из комментариев (первые 85 строк)
            grep "^#" "$0" | grep -v "^#!" | sed 's/^# \{0,1\}//' | head -85
            echo ""
            echo "AVAILABLE LANGUAGES FOR --filter (source files):"
            langs=($(printf '%s\n' "${!SOURCE_EXTS[@]}" | sort))
            printf '%s' "  "
            for i in "${!langs[@]}"; do
                if [[ $i -eq $((${#langs[@]} - 1)) ]]; then
                    printf '%s' "${langs[$i]}"
                else
                    printf '%s, ' "${langs[$i]}"
                fi
            done
            echo ""
            echo ""
            echo "AVAILABLE LANGUAGES FOR --filter-bin (binary files):"
            langs=($(printf '%s\n' "${!BINARY_EXTS[@]}" | sort))
            printf '%s' "  "
            for i in "${!langs[@]}"; do
                if [[ $i -eq $((${#langs[@]} - 1)) ]]; then
                    printf '%s' "${langs[$i]}"
                else
                    printf '%s, ' "${langs[$i]}"
                fi
            done
            echo ""
            echo ""
            echo "NOTE: Languages with the same extensions (e.g., c/cpp for binaries)"
            echo "      may have overlapping definitions."
            exit 0
            ;;
        *)
            echo "[ERROR] Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# --- Проверка зависимостей при старте ---
echo "=========================================="
echo "unpack.sh — рекурсивная распаковка архивов"
echo "=========================================="
if ! check_dependencies; then
    echo "[ERROR] Required dependencies missing. Install them and retry."
    exit 1
fi
echo ""

# --- Пути ---
BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PROJECTS_DIR="$BASE_DIR/src"
UNPACKED_DIR="$BASE_DIR/unpacked"
LOG_DIR="$BASE_DIR/logs/unpack"
NORMALIZE_PY="$BASE_DIR/lib/normalize.py"

if [[ ! -d "$PROJECTS_DIR" ]]; then
    echo "[ERROR] Projects directory not found: $PROJECTS_DIR"
    exit 1
fi

mkdir -p "$UNPACKED_DIR"
mkdir -p "$LOG_DIR"

# Возвращает путь к логу конкретного проекта и гарантирует существование директории
init_project_log() {
    local project_name="$1"
    local log_path="$LOG_DIR/$project_name/$project_name.log"
    mkdir -p "$(dirname "$log_path")"
    > "$log_path"
    echo "$log_path"
}

log_to_file() {
    local log_path="$1"
    local msg="${2:-}"
    if [[ -z "$msg" && ! -t 0 ]]; then
        msg="$(cat)"
    fi
    printf '%s\n' "$msg" >> "$log_path"
    # ВАЖНО: пишем дублирующий вывод в stderr, а не в stdout.
    # extract_archive() возвращает результат (путь к директории) именно
    # через stdout и захватывается через $(...) — если сюда попадёт
    # что-то кроме самого пути, вызывающий код перестанет узнавать
    # успешные распаковки (new_dir перестаёт быть валидной директорией).
    echo "$msg" >&2
}

log_header() {
    local project="$1"
    local log_path="$2"
    {
        printf "==============================\n"
        printf "%s\n" "$project"
        printf "==============================\n\n"
    } >> "$log_path"
    echo "=== Processing project: $project ==="
}

log_summary() {
    local project="$1"
    local log_path="$2"
    local processed="$3"
    local extracted="$4"
    local removed="$5"
    local kept="$6"
    local duration="$7"
    {
        echo ""
        echo "=== SUMMARY ==="
        echo "Processed files: $processed"
        echo "Extracted archives: $extracted"
        if [[ -n "$FILTER_LANGS" || -n "$FILTER_BIN_LANGS" ]]; then
            echo "Filtered removed: $removed"
            echo "Filtered kept: $kept"
        fi
        echo "Total time: $(format_duration $duration)"
        echo "=============================="
    } >> "$log_path"
}

# =============================================================================
# ФУНКЦИИ ФИЛЬТРАЦИИ
# =============================================================================

# Получить расширения для языков из указанной таблицы
# Использование: get_extensions_for_langs "python,java" SOURCE_EXTS
get_extensions_for_langs() {
    local langs="$1"
    local -n ext_table="$2"
    local result=()
    
    IFS=',' read -ra lang_array <<< "$langs"
    for lang in "${lang_array[@]}"; do
        lang="${lang,,}"  # to lowercase
        if [[ -n "${ext_table[$lang]:-}" ]]; then
            # Разбиваем строку расширений и добавляем в массив
            local exts="${ext_table[$lang]}"
            for ext in $exts; do
                result+=("$ext")
            done
        else
            echo "[WARN] Unknown language: $lang" >&2
        fi
    done
    
    # Выводим уникальные расширения через пробел
    printf '%s\n' "${result[@]}" | sort -u | tr '\n' ' '
}

# Применяет фильтры к распакованной директории
apply_filters() {
    local unpack_dir="$1"
    local log_path="$2"
    local project_name="$3"
    
    # Если фильтры не указаны - ничего не делаем
    if [[ -z "$FILTER_LANGS" && -z "$FILTER_BIN_LANGS" ]]; then
        return 0
    fi
    
    local keep_exts=()
    local ext_str=""
    
    # Собираем расширения из фильтров
    if [[ -n "$FILTER_LANGS" ]]; then
        ext_str=$(get_extensions_for_langs "$FILTER_LANGS" SOURCE_EXTS)
        keep_exts+=($ext_str)
        log_to_file "$log_path" "[FILTER] Source extensions kept: $ext_str"
    fi
    
    if [[ -n "$FILTER_BIN_LANGS" ]]; then
        ext_str=$(get_extensions_for_langs "$FILTER_BIN_LANGS" BINARY_EXTS)
        keep_exts+=($ext_str)
        log_to_file "$log_path" "[FILTER] Binary extensions kept: $ext_str"
    fi
    
    # Удаляем дубликаты
    keep_exts=($(printf '%s\n' "${keep_exts[@]}" | sort -u))
    
    if [[ ${#keep_exts[@]} -eq 0 ]]; then
        log_to_file "$log_path" "[FILTER] WARNING: No valid extensions found, skipping filter"
        return 0
    fi
    
    log_to_file "$log_path" "[FILTER] Keeping extensions: ${keep_exts[*]}"

    # .nimcache — целиком директория с бинарниками Nim. Если nim указан
    # среди --filter-bin языков, сохраняем всё её содержимое целиком,
    # независимо от расширений отдельных файлов внутри.
    local keep_nimcache=false
    if [[ -n "$FILTER_BIN_LANGS" ]]; then
        local -a bin_lang_array=()
        IFS=',' read -ra bin_lang_array <<< "$FILTER_BIN_LANGS"
        local blang
        for blang in "${bin_lang_array[@]}"; do
            if [[ "${blang,,}" == "nim" ]]; then
                keep_nimcache=true
                break
            fi
        done
    fi

    local total_files=0
    local kept_files=0
    local removed_files=0

    # ВАЖНО: обходим только ФАЙЛЫ (-type f), а не директории.
    # У директории нет расширения, поэтому раньше она всегда попадала в
    # "не подходит под фильтр" и удалялась через `rm -rf` — это уничтожало
    # всё её содержимое, включая файлы, которые должны были быть сохранены,
    # ещё до того как они успевали пройти индивидуальную проверку.
    # Теперь удаляем только конкретные файлы, а опустевшие директории
    # подчищаем отдельным шагом после основного прохода.
    while IFS= read -r -d '' file; do
        (( total_files++ )) || true

        if [[ "$keep_nimcache" == true && "$file" == */.nimcache/* ]]; then
            (( kept_files++ )) || true
            continue
        fi

        local keep=false
        local filename
        filename=$(basename "$file")

        # Проверяем по расширению
        for ext in "${keep_exts[@]}"; do
            # Для специальных файлов (Makefile, Dockerfile) проверяем точное совпадение
            if [[ "$ext" == "Makefile" || "$ext" == "CMakeLists.txt" || "$ext" == "Dockerfile" ]]; then
                if [[ "$filename" == "$ext" ]]; then
                    keep=true
                    break
                fi
            else
                # Обычное расширение
                if [[ "$filename" == *"$ext" ]]; then
                    keep=true
                    break
                fi
            fi
        done

        if [[ "$keep" == true ]]; then
            (( kept_files++ )) || true
        else
            rm -f "$file"
            (( removed_files++ )) || true
        fi
    done < <(find "$unpack_dir" -mindepth 1 -type f -print0 2>/dev/null || true)

    # Удаляем директории, опустевшие после фильтрации (снизу вверх,
    # -depth гарантирует что дети обрабатываются раньше родителей)
    find "$unpack_dir" -mindepth 1 -depth -type d -empty -delete 2>/dev/null || true

    log_to_file "$log_path" "[FILTER] Total: $total_files files, Kept: $kept_files, Removed: $removed_files"
    echo "[FILTER] [$project_name] Kept: $kept_files, Removed: $removed_files"
}

# =============================================================================
# ФУНКЦИИ РАСПАКОВКИ
# =============================================================================

# Возвращает 0 если расширение файла в списке SKIP_EXTENSIONS и --all не задан
is_skipped_ext() {
    [[ "$UNPACK_ALL" == true ]] && return 1
    local base lower ext
    base=$(basename "$1")
    lower="${base,,}"
    ext="${lower##*.}"
    local skip_ext
    for skip_ext in "${SKIP_EXTENSIONS[@]}"; do
        [[ "$ext" == "$skip_ext" ]] && return 0
    done
    return 1
}

is_archive_by_ext() {
    is_skipped_ext "$1" && return 1
    local base lower
    base=$(basename "$1")
    lower="${base,,}"
    case "$lower" in
        *.tar.gz|*.tar.bz2|*.tar.xz|*.tar.zst|*.tar.z|*.tar.lz4|*.tar.lzma) return 0 ;;
        *.tgz|*.tbz2|*.txz)                                                   return 0 ;;
        *.zip|*.jar|*.war|*.ear|*.whl|*.egg|*.apk|*.ipa)                      return 0 ;;
        *.xpi|*.crx|*.nupkg|*.epub|*.aar)                                     return 0 ;;
        *.rar|*.7z|*.tar|*.gz|*.bz2|*.xz|*.zst|*.z|*.lz4|*.lzma)            return 0 ;;
        *.iso|*.rpm|*.deb|*.cab|*.msi|*.squashfs|*.pkg)                       return 0 ;;
        *)                                                                     return 1 ;;
    esac
}

is_archive_by_magic() {
    local type="$1"
    local file="$2"
    is_skipped_ext "$file" && return 1
    [[ "$type" == *"Zip archive"* ]]               && return 0
    [[ "$type" == *"RAR archive"* ]]               && return 0
    [[ "$type" == *"7-zip archive"* ]]             && return 0
    [[ "$type" == *"tar archive"* ]]               && return 0
    [[ "$type" == *"gzip compressed"* ]]           && return 0
    [[ "$type" == *"bzip2 compressed"* ]]          && return 0
    [[ "$type" == *"XZ compressed"* ]]             && return 0
    [[ "$type" == *"Zstandard compressed"* ]]      && return 0
    [[ "$type" == *"compress'd data"* ]]           && return 0
    [[ "$type" == *"cpio archive"* ]]              && return 0
    [[ "$type" == *"ISO 9660"* ]]                  && return 0
    [[ "$type" == *"RPM"* ]]                       && return 0
    [[ "$type" == *"Debian binary package"* ]]     && return 0
    [[ "$type" == *"Microsoft Cabinet archive"* ]] && return 0
    [[ "$type" == *"MSI Installer"* ]]             && return 0
    [[ "$type" == *"Squashfs filesystem"* ]]       && return 0
    [[ "$type" == *"Apple pkg archive"* ]]         && return 0
    return 1
}

needs_file_check() {
    [[ "$FULL_FILE_CHECK" == true ]] && return 0
    local base lower ext
    base=$(basename "$1")
    lower="${base,,}"
    if [[ "$lower" != *.* ]]; then
        return 0
    fi
    ext="${lower##*.}"
    local amb
    for amb in "${AMBIGUOUS_EXTENSIONS[@]}"; do
        [[ "$ext" == "$amb" ]] && return 0
    done
    return 1
}

declare -gA FILE_TYPE_CACHE=()

run_batch_file() {
    local -a files=("$@")
    [[ ${#files[@]} -eq 0 ]] && return
    while IFS= read -r line; do
        local path ftype
        path="${line%%: *}"
        ftype="${line#*: }"
        FILE_TYPE_CACHE["$path"]="$ftype"
    done < <(file "${files[@]}" 2>/dev/null || true)
}

# --- Таблица методов распаковки -------------------------------------------

_candidate_methods_for_ext() {
    local lower="$1"
    case "$lower" in
        *.tar.gz|*.tgz)   echo "tar_gz gzip_plain" ;;
        *.tar.bz2|*.tbz2) echo "tar_bz2 bzip2_plain" ;;
        *.tar.xz|*.txz)   echo "tar_xz xz_plain" ;;
        *.tar.zst)        echo "tar_zst zstd_plain" ;;
        *.tar.z)          echo "tar_z compress" ;;
        *.tar.lz4)        echo "tar_lz4" ;;
        *.tar.lzma)       echo "tar_lzma" ;;
        *.tar)            echo "tar_plain" ;;
        *.gz)             echo "gzip_plain tar_gz" ;;
        *.bz2)            echo "bzip2_plain tar_bz2" ;;
        *.xz)             echo "xz_plain tar_xz" ;;
        *.zst)            echo "zstd_plain tar_zst" ;;
        *.z)              echo "compress" ;;
        *.zip|*.whl|*.egg|*.jar|*.war|*.ear|*.apk|*.ipa|*.xpi|*.crx|*.nupkg|*.epub|*.aar)
                          echo "zip" ;;
        *.rar)            echo "rar" ;;
        *.7z)             echo "7z" ;;
        *.iso)            echo "iso" ;;
        *.rpm)            echo "rpm" ;;
        *.deb)            echo "deb" ;;
        *.cab)            echo "cab" ;;
        *.msi)            echo "msi" ;;
        *.squashfs)       echo "squashfs" ;;
        *.pkg)            echo "pkg" ;;
        *)                echo "" ;;
    esac
}

_candidate_methods_for_magic() {
    local ftype="$1"
    case "$ftype" in
        *"Zip archive"*)               echo "zip" ;;
        *"RAR archive"*)               echo "rar" ;;
        *"7-zip archive"*)             echo "7z" ;;
        *"tar archive"*)               echo "tar_plain" ;;
        *"gzip compressed"*)           echo "tar_gz gzip_plain" ;;
        *"bzip2 compressed"*)          echo "tar_bz2 bzip2_plain" ;;
        *"XZ compressed"*)             echo "tar_xz xz_plain" ;;
        *"Zstandard compressed"*)      echo "tar_zst zstd_plain" ;;
        *"compress'd data"*)           echo "compress" ;;
        *"cpio archive"*)              echo "cpio" ;;
        *"ISO 9660"*)                  echo "iso" ;;
        *"RPM"*)                       echo "rpm" ;;
        *"Debian binary package"*)     echo "deb" ;;
        *"Microsoft Cabinet archive"*) echo "cab" ;;
        *"MSI Installer"*)             echo "msi" ;;
        *"Squashfs filesystem"*)       echo "squashfs" ;;
        *"Apple pkg archive"*)         echo "pkg" ;;
        *)                              echo "" ;;
    esac
}

# Составные ("двойные") расширения архивов — их нужно срезать целиком,
# а не только последний "._ext", иначе получится "name.tar" вместо "name".
_COMPOUND_ARCHIVE_EXTS=(
    ".tar.gz" ".tar.bz2" ".tar.xz" ".tar.zst" ".tar.z" ".tar.lz4" ".tar.lzma"
)

# Возвращает (в stdout) путь без архивного расширения — с учётом составных
# расширений вида .tar.gz. Обычные одиночные расширения (.zip, .7z, ...)
# срезаются штатным ${file%.*}.
_strip_archive_ext() {
    local file="$1"
    local lower="${file,,}"
    local ce
    for ce in "${_COMPOUND_ARCHIVE_EXTS[@]}"; do
        if [[ "$lower" == *"$ce" ]]; then
            echo "${file:0:$(( ${#file} - ${#ce} ))}"
            return 0
        fi
    done
    echo "${file%.*}"
}

# Находит свободное имя директории, добавляя суффикс _2, _3, ... если
# директория с таким именем уже существует и не пуста (коллизия имён
# в режиме --clean, например foo.zip и foo.tar.gz распаковались бы в
# одну и ту же папку "foo").
_resolve_clean_dir_collision() {
    local base_dir="$1"
    local log_path="$2"
    local dir="$base_dir"
    local n=2
    while [[ -e "$dir" && -n "$(ls -A "$dir" 2>/dev/null)" ]]; do
        dir="${base_dir}_${n}"
        (( n++ )) || true
    done
    if [[ "$dir" != "$base_dir" ]]; then
        log_to_file "$log_path" "[CLEAN] Directory name collision for '$base_dir', using '$dir' instead"
    fi
    echo "$dir"
}

_run_extract_method() {
    local method="$1" file="$2" dir="$3" base="$4"
    case "$method" in
        tar_gz)      tar -xzf "$file" -C "$dir" ;;
        tar_bz2)     tar -xjf "$file" -C "$dir" ;;
        tar_xz)      tar -xJf "$file" -C "$dir" ;;
        tar_zst)     tar -x --zstd -f "$file" -C "$dir" ;;
        tar_z)       tar -xZf "$file" -C "$dir" ;;
        tar_lz4)     lz4 -d "$file" -c | tar -x -C "$dir" ;;
        tar_lzma)    tar -x --lzma -f "$file" -C "$dir" ;;
        tar_plain)   tar -xf "$file" -C "$dir" ;;
        # Срез суффикса регистронезависимый (символьные классы [gG][zZ] и
        # т.п.) — кандидат-метод выбирается case-insensitive (по "lower"),
        # а $base приходит в исходном регистре файла. Например, для файла
        # "archive.z" (маленькая z) срез только по ".Z" не сработал бы,
        # и результат назывался бы "archive.z" вместо "archive".
        gzip_plain)  gunzip -c "$file" > "$dir/${base%.[gG][zZ]}" ;;
        bzip2_plain) bunzip2 -c "$file" > "$dir/${base%.[bB][zZ]2}" ;;
        xz_plain)    unxz -c "$file" > "$dir/${base%.[xX][zZ]}" ;;
        zstd_plain)  unzstd -q -o "$dir/${base%.[zZ][sS][tT]}" "$file" ;;
        compress)    uncompress -c "$file" > "$dir/${base%.[zZ]}" ;;
        zip)         unzip -q "$file" -d "$dir" ;;
        rar)         unrar x -o+ "$file" "$dir/" ;;
        7z)          7z x -y "$file" -o"$dir" ;;
        iso)         7z x -y "$file" -o"$dir" ;;
        rpm)         rpm2cpio "$file" | (cd "$dir" && cpio -idm) ;;
        deb)         dpkg-deb -R "$file" "$dir" ;;
        cab)         cabextract -d "$dir" "$file" ;;
        msi)         msiextract -C "$dir" "$file" ;;
        squashfs)    unsquashfs -d "$dir" "$file" ;;
        pkg)         bsdtar -xf "$file" -C "$dir" ;;
        cpio)        (cd "$dir" && cpio -idm < "$file") ;;
        *)           return 127 ;;
    esac
}

extract_archive() {
    local file="$1"
    local log_path="$2"
    local project_name="$3"
    local dir
    
    # Определяем директорию для распаковки
    if [[ "$UNPACK_CLEAN" == true ]]; then
        # Распаковываем прямо в папку с архивом (удаляем расширение архива,
        # включая составные вида .tar.gz/.tar.bz2/.tar.xz/.tar.zst/.tar.z/
        # .tar.lz4/.tar.lzma)
        local clean_base_dir
        clean_base_dir="$(_strip_archive_ext "$file")"
        dir="$(_resolve_clean_dir_collision "$clean_base_dir" "$log_path")"
        mkdir -p "$dir"
    else
        dir="${file}_dir"
        mkdir -p "$dir"
    fi
    
    log_to_file "$log_path" "[EXTRACT] Extracting: $file -> $dir"

    local base lower
    base=$(basename "$file")
    lower="${base,,}"

    local -a tried=()
    local combined_err=""
    local ok=0
    local ftype=""

    # --- Попытка №1: по расширению файла ---
    local -a candidates=()
    read -ra candidates <<< "$(_candidate_methods_for_ext "$lower")"

    local m out rc
    for m in "${candidates[@]}"; do
        [[ -z "$m" ]] && continue
        tried+=("$m")
        out=$(_run_extract_method "$m" "$file" "$dir" "$base" 2>&1); rc=$?
        if [[ $rc -eq 0 ]]; then
            ok=1
            break
        fi
        combined_err+=$'\n'"--- method '$m' (by extension) failed, rc=$rc ---"$'\n'"$out"
    done

    # --- Попытка №2: magic ---
    if [[ $ok -eq 0 ]]; then
        ftype="${FILE_TYPE_CACHE[$file]:-}"
        if [[ -z "$ftype" ]]; then
            ftype=$(file -b "$file" 2>/dev/null || echo "")
        fi

        local -a magic_candidates=()
        read -ra magic_candidates <<< "$(_candidate_methods_for_magic "$ftype")"

        for m in "${magic_candidates[@]}"; do
            [[ -z "$m" ]] && continue
            local already=0 tm
            for tm in "${tried[@]}"; do
                [[ "$tm" == "$m" ]] && already=1 && break
            done
            [[ $already -eq 1 ]] && continue

            tried+=("$m")
            out=$(_run_extract_method "$m" "$file" "$dir" "$base" 2>&1); rc=$?
            if [[ $rc -eq 0 ]]; then
                ok=1
                break
            fi
            combined_err+=$'\n'"--- method '$m' (by magic: ${ftype:-unknown}) failed, rc=$rc ---"$'\n'"$out"
        done

        if [[ ${#magic_candidates[@]} -eq 0 || -z "${magic_candidates[0]}" ]]; then
            combined_err+=$'\n'"file(1) did not match any known archive signature: ${ftype:-<file -b returned nothing>}"
        fi
    fi

    if [[ $ok -eq 1 ]]; then
        log_to_file "$log_path" "[EXTRACT] Done: $file"

        # Если clean mode - удаляем архив после успешной распаковки
        if [[ "$UNPACK_CLEAN" == true ]]; then
            rm -f "$file"
            log_to_file "$log_path" "[CLEAN] Removed archive: $file"
        fi
        
        echo "$dir"
    else
        {
            echo "Failed to extract: $file"
            echo "  extension-guessed methods tried: ${tried[*]:-<none>}"
            echo "  detected type (file -b): ${ftype:-<not checked>}"
            echo "  --- combined output ---"
            printf '%s\n' "$combined_err"
            echo ""
        } | log_to_file "$log_path"
        rm -rf "$dir"
        echo ""
    fi
}

# Считает файлы в директории (непусто ли)
count_files_in_dir() {
    local dir="$1"
    if [[ ! -d "$dir" ]]; then
        echo 0
        return
    fi
    find "$dir" -maxdepth 1 -mindepth 1 | wc -l
}

# Спрашивает пользователя интерактивно (только если stdin — терминал)
ask_repack() {
    local subdir="$1"
    local count="$2"
    local answer
    if [[ -t 0 ]]; then
        read -r -p "[?] $subdir уже содержит $count элементов. Перераспаковать? [y/N]: " answer
    else
        answer="n"
    fi
    [[ "${answer,,}" == "y" || "${answer,,}" == "yes" ]]
}

# =============================================================================
# ОСНОВНАЯ ФУНКЦИЯ ОБРАБОТКИ ПРОЕКТА
# =============================================================================

process_project() {
    local project="$1"
    local project_name
    project_name=$(basename "$project")
    local proj_start_ts
    proj_start_ts=$(now_ns)

    local log_path
    log_path=$(init_project_log "$project_name")

    log_header "$project_name" "$log_path"

    local unpack_dir="$UNPACKED_DIR/${project_name}"

    # Проверяем src/ и bin/ подпапки в unpacked/PROJ/
    local do_src=true
    local do_bin=true

    local src_unpacked="$unpack_dir/src"
    local bin_unpacked="$unpack_dir/bin"

    local src_count bin_count
    src_count=$(count_files_in_dir "$src_unpacked")
    bin_count=$(count_files_in_dir "$bin_unpacked")

    if (( src_count > 0 )); then
        if ask_repack "unpacked/$project_name/src" "$src_count"; then
            log_to_file "$log_path" "[INFO] Перераспаковываем src/"
            rm -rf "$src_unpacked"
        else
            log_to_file "$log_path" "[INFO] Пропускаем src/ (уже распакован)"
            do_src=false
        fi
    fi

    if (( bin_count > 0 )); then
        if ask_repack "unpacked/$project_name/bin" "$bin_count"; then
            log_to_file "$log_path" "[INFO] Перераспаковываем bin/"
            rm -rf "$bin_unpacked"
        else
            log_to_file "$log_path" "[INFO] Пропускаем bin/ (уже распакован)"
            do_bin=false
        fi
    fi

    if [[ "$do_src" == false && "$do_bin" == false ]]; then
        log_to_file "$log_path" "[INFO] Ничего не распаковывается — оба каталога уже есть."
        return 0
    fi

    local t0
    t0=$(now_ns)

    mkdir -p "$unpack_dir"
    if [[ "$do_src" == true && -d "$project/src" ]]; then
        log_to_file "$log_path" "[INFO] Copying $project/src -> $src_unpacked"
        cp -a "$project/src" "$src_unpacked"
    fi
    if [[ "$do_bin" == true && -d "$project/bin" ]]; then
        log_to_file "$log_path" "[INFO] Copying $project/bin -> $bin_unpacked"
        cp -a "$project/bin" "$bin_unpacked"
    fi
    if [[ ! -d "$project/src" && ! -d "$project/bin" ]]; then
        log_to_file "$log_path" "[INFO] Copying $project -> $unpack_dir (no src/bin split)"
        rm -rf "$unpack_dir"
        cp -a "$project" "$unpack_dir"
    fi

    log_to_file "$log_path" "[TIME] cp -a: $(format_duration $(( $(now_ns) - t0 )))"

    # Нормализация имён файлов и папок ДО распаковки
    if [[ -f "$NORMALIZE_PY" ]]; then
        t0=$(now_ns)
        log_to_file "$log_path" "[INFO] Normalizing filenames..."
        python3 "$NORMALIZE_PY" --content-dir "$unpack_dir" 2>&1 | log_to_file "$log_path"
        log_to_file "$log_path" "[TIME] normalize: $(format_duration $(( $(now_ns) - t0 )))"
    else
        log_to_file "$log_path" "[WARN] normalize.py не найден: $NORMALIZE_PY"
        log_to_file "$log_path" "[WARN] Без нормализации файлы с некорректными именами могут не распаковаться."
        local answer_norm
        if [[ -t 0 ]]; then
            read -r -p "[?] Продолжить без нормализации? [y/N]: " answer_norm
        else
            answer_norm="n"
        fi
        if [[ "${answer_norm,,}" != "y" && "${answer_norm,,}" != "yes" ]]; then
            log_to_file "$log_path" "[INFO] Отменено пользователем."
            return 1
        fi
        log_to_file "$log_path" "[INFO] Продолжаем без нормализации."
    fi

    t0=$(now_ns)
    local -a queue=()
    if [[ -n "$SKIP_PATH_PATTERN" ]]; then
        while IFS= read -r -d '' skipped_file; do
            log_to_file "$log_path" "[SKIP-PATH] $skipped_file"
        done < <(
            find "$unpack_dir" -type f \( "${_FIND_NAME_ARGS[@]}" \) -print0 \
                | grep -zZE "$SKIP_PATH_PATTERN" 2>/dev/null || true
        )
    fi
    while IFS= read -r -d '' f; do
        queue+=("$f")
    done < <(
        if [[ -n "$SKIP_PATH_PATTERN" ]]; then
            find "$unpack_dir" -type f \( "${_FIND_NAME_ARGS[@]}" \) -print0 \
                | grep -zZvE "$SKIP_PATH_PATTERN" 2>/dev/null || true
        else
            find "$unpack_dir" -type f \( "${_FIND_NAME_ARGS[@]}" \) -print0
        fi
    )
    local total_files=${#queue[@]}
    log_to_file "$log_path" "[TIME] initial find ($total_files files): $(format_duration $(( $(now_ns) - t0 )))"

    local total_extracted=0
    local processed=0
    local file_calls=0
    FILE_TYPE_CACHE=()

    log_to_file "$log_path" "[INFO] Initial queue: $total_files files"

    t0=$(now_ns)
    local head=0
    local batch_num=0
    local PROGRESS_EVERY=10

    while [[ $head -lt ${#queue[@]} ]]; do
        local -a current_batch=()
        local -a unknown_ext_batch=()
        local batch_end=$(( head + FILE_BATCH_SIZE ))
        if (( batch_end > ${#queue[@]} )); then
            batch_end=${#queue[@]}
        fi

        local i
        for (( i = head; i < batch_end; i++ )); do
            local f="${queue[$i]}"
            current_batch+=("$f")
            if [[ -f "$f" ]] && ! is_archive_by_ext "$f"; then
                if needs_file_check "$f" && [[ -z "${FILE_TYPE_CACHE[$f]+x}" ]]; then
                    unknown_ext_batch+=("$f")
                fi
            fi
        done
        head=$batch_end
        (( batch_num++ )) || true

        if (( batch_num % PROGRESS_EVERY == 0 )); then
            local remaining=$(( ${#queue[@]} - head ))
            log_to_file "$log_path" "[PROGRESS] $(date +%H:%M:%S) | batch: $batch_num | extracted: $total_extracted | queue remaining: $remaining"
        fi

        if [[ ${#unknown_ext_batch[@]} -gt 0 ]]; then
            run_batch_file "${unknown_ext_batch[@]}"
            (( file_calls++ )) || true
        fi

        local -a new_dirs_batch=()

        for phys_file in "${current_batch[@]}"; do
            [[ -f "$phys_file" ]] || continue
            (( processed++ )) || true

            local is_arch=false
            if is_archive_by_ext "$phys_file"; then
                is_arch=true
            else
                local ftype="${FILE_TYPE_CACHE[$phys_file]:-}"
                if [[ -n "$ftype" ]] && is_archive_by_magic "$ftype" "$phys_file"; then
                    is_arch=true
                fi
            fi

            if [[ "$is_arch" == true ]]; then
                local fsize
                fsize=$(stat -c%s "$phys_file" 2>/dev/null || echo 0)
                if (( fsize < 22 )); then
                    log_to_file "$log_path" "[SKIP] too small (${fsize}b): $(basename "$phys_file")"
                    continue
                fi
                local t_extract
                t_extract=$(now_ns)
                local new_dir
                new_dir=$(extract_archive "$phys_file" "$log_path" "$project_name")
                log_to_file "$log_path" "[TIME] extract $(basename "$phys_file"): $(format_duration $(( $(now_ns) - t_extract )))"

                if [[ -n "$new_dir" && -d "$new_dir" ]]; then
                    (( total_extracted++ )) || true
                    new_dirs_batch+=("$new_dir")
                fi
            fi
        done

        if [[ ${#new_dirs_batch[@]} -gt 0 ]]; then
            local t_find queue_before
            t_find=$(now_ns)
            queue_before=${#queue[@]}
            if [[ -n "$SKIP_PATH_PATTERN" ]]; then
                while IFS= read -r -d '' skipped_file; do
                    log_to_file "$log_path" "[SKIP-PATH] $skipped_file"
                done < <(
                    find "${new_dirs_batch[@]}" -type f \( "${_FIND_NAME_ARGS[@]}" \) -print0 2>/dev/null \
                        | grep -zZE "$SKIP_PATH_PATTERN" || true
                )
            fi
            while IFS= read -r -d '' new_file; do
                queue+=("$new_file")
            done < <(
                if [[ -n "$SKIP_PATH_PATTERN" ]]; then
                    find "${new_dirs_batch[@]}" -type f \( "${_FIND_NAME_ARGS[@]}" \) -print0 2>/dev/null \
                        | grep -zZvE "$SKIP_PATH_PATTERN" || true
                else
                    find "${new_dirs_batch[@]}" -type f \( "${_FIND_NAME_ARGS[@]}" \) -print0 2>/dev/null
                fi
            )
            local added=$(( ${#queue[@]} - queue_before ))
            local remaining=$(( ${#queue[@]} - head ))
            log_to_file "$log_path" "[TIME] batched find (${#new_dirs_batch[@]} dirs → +${added} new files): $(format_duration $(( $(now_ns) - t_find )))"
            log_to_file "$log_path" "[INFO] extracted: $total_extracted | queue remaining: $remaining | total queued: ${#queue[@]}"
        fi
    done

    local t_loop_dur=$(( $(now_ns) - t0 ))
    log_to_file "$log_path" "[TIME] main loop: $(format_duration $t_loop_dur) | file(1) calls: $file_calls"

    # Удаляем битые симлинки (-xtype l матчит именно те симлинки, чья цель
    # не резолвится — валидные симлинки, указывающие на существующие файлы,
    # не трогаем; -type l удалял бы вообще ВСЕ симлинки, включая рабочие)
    t0=$(now_ns)
    find "$unpack_dir" -xtype l -delete 2>/dev/null || true
    log_to_file "$log_path" "[TIME] symlink cleanup: $(format_duration $(( $(now_ns) - t0 )))"

    # Применяем фильтры, если они указаны
    local removed_count=0
    local kept_count=0
    if [[ -n "$FILTER_LANGS" || -n "$FILTER_BIN_LANGS" ]]; then
        # Перед фильтрацией подсчитываем количество файлов
        local before_count
        before_count=$(find "$unpack_dir" -type f | wc -l)
        log_to_file "$log_path" "[FILTER] Files before filtering: $before_count"
        
        apply_filters "$unpack_dir" "$log_path" "$project_name"
        
        local after_count
        after_count=$(find "$unpack_dir" -type f | wc -l)
        removed_count=$(( before_count - after_count ))
        kept_count=$after_count
        log_to_file "$log_path" "[FILTER] Files after filtering: $after_count"
    fi

    local proj_total=$(( $(now_ns) - proj_start_ts ))
    log_summary "$project_name" "$log_path" "$processed" "$total_extracted" "$removed_count" "$kept_count" "$proj_total"

    echo "=== Done: $project_name | processed: $processed | extracted: $total_extracted | time: $(format_duration $proj_total) ==="
}

# =============================================================================
# ОСНОВНОЙ ЦИКЛ
# =============================================================================
projects=()

if [[ -n "$SINGLE_PROJECT" ]]; then
    local_dir="$PROJECTS_DIR/$SINGLE_PROJECT"
    if [[ -d "$local_dir" ]]; then
        projects=("$local_dir")
    else
        echo "[ERROR] Project not found: $local_dir"
        exit 1
    fi
else
    for dir in "$PROJECTS_DIR"/*/; do
        [[ -d "$dir" ]] || continue
        projects+=("${dir%/}")
    done
    if [[ ${#projects[@]} -eq 0 ]]; then
        echo "[ERROR] No projects found in: $PROJECTS_DIR"
        exit 1
    fi
fi

echo "[INFO] Projects to process: ${#projects[@]}"
echo "[INFO] Projects: $(basename -a "${projects[@]}" | tr '\n' ' ')"
[[ -n "$SINGLE_PROJECT" ]] && echo "[INFO] Mode: single project"
[[ "$UNPACK_ALL" == true ]] && echo "[INFO] Mode: --all (unpacking everything)"
[[ "$UNPACK_ALL" == false ]] && echo "[INFO] Skipping extensions by default: ${SKIP_EXTENSIONS[*]}"
[[ "$FULL_FILE_CHECK" == true ]] && echo "[INFO] Mode: --full-file-check"
[[ -n "$SKIP_PATH_PATTERN" ]] && echo "[INFO] Skip pattern: $SKIP_PATH_PATTERN"
[[ "$UNPACK_CLEAN" == true ]] && echo "[INFO] Mode: --clean (no _dir suffix, remove archives)"
[[ -n "$FILTER_LANGS" ]] && echo "[INFO] Filter source languages: $FILTER_LANGS"
[[ -n "$FILTER_BIN_LANGS" ]] && echo "[INFO] Filter binary languages: $FILTER_BIN_LANGS"
echo ""

# pids/pid_names — строго параллельные массивы в порядке FIFO (кто раньше
# запущен — тот раньше и в начале массива). Раньше здесь использовался
# `wait -n 2>/dev/null || wait` для освобождения слота: код возврата
# завершившегося процесса нигде не проверялся, а сам pid тут же вычищался
# из "pids" по `kill -0` — то есть при сбое проекта, "реапнутого" именно
# в этой throttling-ветке (а не в самом последнем финальном цикле), ошибка
# полностью терялась и итоговый exit_code скрипта мог остаться 0 даже при
# реальном падении одного из проектов.
#
# Теперь вместо "жди кого угодно" явно ждём САМЫЙ СТАРЫЙ из отслеживаемых
# процессов (pids[0]) — это детерминированно и гарантированно сохраняет
# его код возврата, не зависит от версии bash (в отличие от `wait -n`,
# которая появилась только в bash 4.3+) и позволяет точно сопоставить
# упавший pid с именем проекта для внятного сообщения об ошибке.
pids=()
pid_names=()
exit_code=0

for project_dir in "${projects[@]}"; do
    if [[ ${#pids[@]} -ge $MAX_PARALLEL ]]; then
        echo "[INFO] Reached max parallel jobs ($MAX_PARALLEL), waiting..."
        oldest_pid="${pids[0]}"
        oldest_name="${pid_names[0]}"
        if ! wait "$oldest_pid"; then
            echo "[ERROR] Project '$oldest_name' (pid $oldest_pid) failed"
            exit_code=1
        fi
        pids=("${pids[@]:1}")
        pid_names=("${pid_names[@]:1}")
    fi

    echo "[INFO] Starting: $(basename "$project_dir") (running: ${#pids[@]}/$MAX_PARALLEL)"
    process_project "$project_dir" &
    pids+=($!)
    pid_names+=("$(basename "$project_dir")")
done

echo "[INFO] Waiting for all remaining projects to complete..."
for i in "${!pids[@]}"; do
    if ! wait "${pids[$i]}"; then
        echo "[ERROR] Project '${pid_names[$i]}' (pid ${pids[$i]}) failed"
        exit_code=1
    fi
done

SCRIPT_TOTAL=$(( $(now_ns) - SCRIPT_START_TS ))
echo ""
echo "=================================================="
echo "Unpacking complete."
echo "Total time : $(format_duration $SCRIPT_TOTAL)"
echo "Output     : $UNPACKED_DIR"
echo "Logs       : $LOG_DIR/<project>/<project>.log"
echo "=================================================="

exit "$exit_code"