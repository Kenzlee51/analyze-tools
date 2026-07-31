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
#                               (по умолчанию .jar и .deb не распаковываются)
#   -j, --parallel N            Количество параллельных проектов (default: 1)
#                               Рекомендации: HDD → 1, SSD → 2-4, NVMe → 4-8
#   -b, --batch-size N          Файлов в один вызов file(1) (default: 500)
#   -s, --skip-path PATTERN     Regex для пропуска путей (можно несколько раз)
#                               Пример: -s node_modules -s /m2_repo
#   -f, --full-file-check       Проверять все файлы через file(1) независимо
#                               от расширения (медленно но надёжно)
#   -c, --check-deps            Только проверить наличие зависимостей и выйти
#   -h, --help                  Показать эту справку
#
# ПРИМЕРЫ:
#   ./unpack.sh
#   ./unpack.sh -p PROJ1
#   ./unpack.sh --single-project PROJ1
#   ./unpack.sh -j 4 -p PROJ1
#   ./unpack.sh -a
#   ./unpack.sh -s node_modules
#   ./unpack.sh -f -j 2
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
#           ├── errors_unpack.txt
#           └── skipped_archives.txt
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

# Расширения которые НЕ распаковываются по умолчанию.
# При --all эти ограничения снимаются.
#SKIP_EXTENSIONS=(jar deb)
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
        # Убираем дубликаты пакетов
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
            args+=( -name "$pat" )
            first=false
        else
            args+=( -o -name "$pat" )
        fi
    done
    for ext in "${AMBIGUOUS_EXTENSIONS[@]}"; do
        args+=( -o -name "*.$ext" )
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
# Разбор аргументов
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
        -c|--check-deps)
            check_dependencies
            exit $?
            ;;
        -h|--help)
            grep "^#" "$0" | head -75 | sed 's/^# \{0,1\}//'
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
# Ошибки теперь пишутся per-project: $LOG_DIR/<project_name>/error.log
SKIPPED_LOG="$LOG_DIR/skipped_archives.txt"
NORMALIZE_PY="$BASE_DIR/lib/normalize.py"

if [[ ! -d "$PROJECTS_DIR" ]]; then
    echo "[ERROR] Projects directory not found: $PROJECTS_DIR"
    exit 1
fi

mkdir -p "$UNPACKED_DIR"
mkdir -p "$LOG_DIR"
> "$SKIPPED_LOG"

# Возвращает путь к error.log конкретного проекта и гарантирует
# существование директории. Файл на этот запуск обнуляется.
init_project_error_log() {
    local project_name="$1"
    local log_path="$LOG_DIR/$project_name/error.log"
    mkdir -p "$(dirname "$log_path")"
    > "$log_path"
    echo "$log_path"
}

log_header() {
    local project="$1"
    local log_path="$2"
    printf "==============================\n%s\n==============================\n\n" "$project" >> "$log_path"
    echo "=== Processing project: $project ==="
}

# Пишет блок ошибки в error.log проекта. Принимает текст либо аргументом,
# либо через stdin (для многострочного вывода команд).
# Использование: log_error "$error_log" "однострочное сообщение"
#            или: cmd_output | log_error "$error_log"
log_error() {
    local log_path="$1"
    local msg="${2:-}"
    if [[ -z "$msg" && ! -t 0 ]]; then
        msg="$(cat)"
    fi
    printf '%s\n' "$msg" >> "$log_path"
    echo "[ERROR] $(printf '%s\n' "$msg" | head -1)"
}

log_skipped() {
    { flock 200; echo "$1"; } 200>"$SKIPPED_LOG.lock" >> "$SKIPPED_LOG"
}

# Возвращает 0 если расширение файла в списке SKIP_EXTENSIONS и --all не задан
is_skipped_ext() {
    [[ "$UNPACK_ALL" == true ]] && return 1
    local base lower ext
    base=$(basename "$1")
    lower="${base,,}"
    # Убираем составные расширения типа .tar.gz — нас интересует только последнее
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
# Каждый "метод" — это одна конкретная команда. Составные случаи (gzip и
# т.п. могут быть как plain-файлом, так и tar-потоком внутри) реализованы
# как ДВА метода: сначала пробуем как tar, потом как обычное сжатие.

# Возвращает (в stdout, через пробел) список методов-кандидатов на основе
# расширения файла, в порядке приоритета.
_candidate_methods_for_ext() {
    local lower="$1"
    case "$lower" in
        *.tar.gz|*.tgz)   echo "tar_gz" ;;
        *.tar.bz2|*.tbz2) echo "tar_bz2" ;;
        *.tar.xz|*.txz)   echo "tar_xz" ;;
        *.tar.zst)        echo "tar_zst" ;;
        *.tar.z)          echo "tar_z" ;;
        *.tar.lz4)        echo "tar_lz4" ;;
        *.tar.lzma)       echo "tar_lzma" ;;
        *.tar)            echo "tar_plain" ;;
        *.gz)             echo "tar_gz gzip_plain" ;;
        *.bz2)            echo "tar_bz2 bzip2_plain" ;;
        *.xz)             echo "tar_xz xz_plain" ;;
        *.zst)            echo "tar_zst zstd_plain" ;;
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

# То же самое, но на основе реального типа файла (вывод `file -b`).
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

# Выполняет один конкретный метод распаковки. НЕ подавляет stderr —
# вызывающий код сам решает, что делать с выводом (обычно захватывает его
# через `$(... 2>&1)`).
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
        gzip_plain)  gunzip -c "$file" > "$dir/${base%.gz}" ;;
        bzip2_plain) bunzip2 -c "$file" > "$dir/${base%.bz2}" ;;
        xz_plain)    unxz -c "$file" > "$dir/${base%.xz}" ;;
        zstd_plain)  unzstd -q -o "$dir/${base%.zst}" "$file" ;;
        compress)    uncompress -c "$file" > "$dir/${base%.Z}" ;;
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
    local error_log="$2"
    local dir="${file}_dir"
    mkdir -p "$dir"
    echo "Extracting: $file" >&2

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

    # --- Попытка №2: расширение не помогло (или файла с такой ext-логикой
    # нет вовсе) — определяем реальный формат по magic-байтам и пробуем
    # подходящий инструмент, даже если расширение указывало на другое ---
    if [[ $ok -eq 0 ]]; then
        ftype="${FILE_TYPE_CACHE[$file]:-}"
        if [[ -z "$ftype" ]]; then
            ftype=$(file -b "$file" 2>/dev/null || echo "")
        fi

        local -a magic_candidates=()
        read -ra magic_candidates <<< "$(_candidate_methods_for_magic "$ftype")"

        for m in "${magic_candidates[@]}"; do
            [[ -z "$m" ]] && continue
            # не повторяем метод, который уже пробовали на шаге 1
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
        echo "Done: $file" >&2
        echo "$dir"
    else
        {
            echo "Failed to extract: $file"
            echo "  extension-guessed methods tried: ${tried[*]:-<none>}"
            echo "  detected type (file -b): ${ftype:-<not checked, extension attempt succeeded... n/a>}"
            echo "  --- combined output ---"
            printf '%s\n' "$combined_err"
            echo ""
        } | log_error "$error_log"
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

process_project() {
    local project="$1"
    local project_name
    project_name=$(basename "$project")
    local proj_start_ts
    proj_start_ts=$(now_ns)

    local error_log
    error_log=$(init_project_error_log "$project_name")

    log_header "$project_name" "$error_log"

    local unpack_dir="$UNPACKED_DIR/${project_name}"

    # Проверяем src/ и bin/ подпапки в unpacked/PROJ/
    # Для каждой: если уже содержит что-то — спрашиваем перераспаковывать ли
    local do_src=true
    local do_bin=true

    local src_unpacked="$unpack_dir/src"
    local bin_unpacked="$unpack_dir/bin"

    local src_count bin_count
    src_count=$(count_files_in_dir "$src_unpacked")
    bin_count=$(count_files_in_dir "$bin_unpacked")

    if (( src_count > 0 )); then
        if ask_repack "unpacked/$project_name/src" "$src_count"; then
            echo "[INFO] [$project_name] Перераспаковываем src/"
            rm -rf "$src_unpacked"
        else
            echo "[INFO] [$project_name] Пропускаем src/ (уже распакован)"
            do_src=false
        fi
    fi

    if (( bin_count > 0 )); then
        if ask_repack "unpacked/$project_name/bin" "$bin_count"; then
            echo "[INFO] [$project_name] Перераспаковываем bin/"
            rm -rf "$bin_unpacked"
        else
            echo "[INFO] [$project_name] Пропускаем bin/ (уже распакован)"
            do_bin=false
        fi
    fi

    # Если оба пропускаем — ничего не делаем
    if [[ "$do_src" == false && "$do_bin" == false ]]; then
        echo "[INFO] [$project_name] Ничего не распаковывается — оба каталога уже есть."
        return 0
    fi

    local t0
    t0=$(now_ns)

    # Копируем только нужные подкаталоги
    mkdir -p "$unpack_dir"
    if [[ "$do_src" == true && -d "$project/src" ]]; then
        echo "Copying $project/src -> $src_unpacked"
        cp -a "$project/src" "$src_unpacked"
    fi
    if [[ "$do_bin" == true && -d "$project/bin" ]]; then
        echo "Copying $project/bin -> $bin_unpacked"
        cp -a "$project/bin" "$bin_unpacked"
    fi
    # Если нет разделения src/bin — копируем всё как раньше
    if [[ ! -d "$project/src" && ! -d "$project/bin" ]]; then
        echo "Copying $project -> $unpack_dir (no src/bin split)"
        rm -rf "$unpack_dir"
        cp -a "$project" "$unpack_dir"
    fi

    echo "[TIME] [$project_name] cp -a: $(format_duration $(( $(now_ns) - t0 )))"

    # Нормализация имён файлов и папок ДО распаковки
    # Только восстановление сломанной кодировки — без транслитерации
    if [[ -f "$NORMALIZE_PY" ]]; then
        t0=$(now_ns)
        echo "[INFO] [$project_name] Normalizing filenames..."
        python3 "$NORMALIZE_PY" --content-dir "$unpack_dir"
        echo "[TIME] [$project_name] normalize: $(format_duration $(( $(now_ns) - t0 )))"
    else
        echo ""
        echo "[WARN] [$project_name] normalize.py не найден: $NORMALIZE_PY"
        echo "[WARN] Без нормализации файлы с некорректными именами могут не распаковаться."
        local answer_norm
        if [[ -t 0 ]]; then
            read -r -p "[?] Продолжить без нормализации? [y/N]: " answer_norm
        else
            answer_norm="n"
        fi
        if [[ "${answer_norm,,}" != "y" && "${answer_norm,,}" != "yes" ]]; then
            echo "[INFO] [$project_name] Отменено пользователем."
            return 1
        fi
        echo "[INFO] [$project_name] Продолжаем без нормализации."
    fi

    t0=$(now_ns)
    local -a queue=()
    if [[ -n "$SKIP_PATH_PATTERN" ]]; then
        while IFS= read -r -d '' skipped_file; do
            { flock 200; echo "$skipped_file"; } 200>"$SKIPPED_LOG.lock" >> "$SKIPPED_LOG"
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
    echo "[TIME] [$project_name] initial find ($total_files files): $(format_duration $(( $(now_ns) - t0 )))"

    local total_extracted=0
    local processed=0
    local file_calls=0
    FILE_TYPE_CACHE=()

    echo "[INFO] [$project_name] Initial queue: $total_files files"

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
            echo "[PROGRESS] [$project_name] $(date +%H:%M:%S) | batch: $batch_num | extracted: $total_extracted | queue remaining: $remaining"
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
                    echo "[SKIP] [$project_name] too small (${fsize}b): $(basename "$phys_file")"
                    { flock 200; echo "$phys_file  [too small: ${fsize}b]"; } 200>"$SKIPPED_LOG.lock" >> "$SKIPPED_LOG"
                    continue
                fi
                local t_extract
                t_extract=$(now_ns)
                local new_dir
                new_dir=$(extract_archive "$phys_file" "$error_log")
                echo "[TIME] [$project_name] extract $(basename "$phys_file"): $(format_duration $(( $(now_ns) - t_extract )))"

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
                    { flock 200; echo "$skipped_file"; } 200>"$SKIPPED_LOG.lock" >> "$SKIPPED_LOG"
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
            echo "[TIME] [$project_name] batched find (${#new_dirs_batch[@]} dirs → +${added} new files): $(format_duration $(( $(now_ns) - t_find )))"
            echo "[INFO] [$project_name] extracted: $total_extracted | queue remaining: $remaining | total queued: ${#queue[@]}"
        fi
    done

    local t_loop_dur=$(( $(now_ns) - t0 ))
    echo "[TIME] [$project_name] main loop: $(format_duration $t_loop_dur) | file(1) calls: $file_calls"

    t0=$(now_ns)
    find "$unpack_dir" -type l -delete 2>/dev/null || true
    echo "[TIME] [$project_name] symlink cleanup: $(format_duration $(( $(now_ns) - t0 )))"

    local proj_total=$(( $(now_ns) - proj_start_ts ))
    local skipped_count
    skipped_count=$(grep -c "/$project_name/" "$SKIPPED_LOG" 2>/dev/null || echo 0)
    echo "=== Done: $project_name | processed: $processed | extracted: $total_extracted | skipped: $skipped_count | time: $(format_duration $proj_total) ==="
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
echo ""

pids=()
exit_code=0

for project_dir in "${projects[@]}"; do
    if [[ ${#pids[@]} -ge $MAX_PARALLEL ]]; then
        echo "[INFO] Reached max parallel jobs ($MAX_PARALLEL), waiting..."
        wait -n 2>/dev/null || wait
        new_pids=()
        for pid in "${pids[@]}"; do
            kill -0 "$pid" 2>/dev/null && new_pids+=("$pid")
        done
        pids=("${new_pids[@]}")
    fi

    echo "[INFO] Starting: $(basename "$project_dir") (running: ${#pids[@]}/$MAX_PARALLEL)"
    process_project "$project_dir" &
    pids+=($!)
done

echo "[INFO] Waiting for all remaining projects to complete..."
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
        echo "[ERROR] Process $pid failed"
        exit_code=1
    fi
done

rm -f "$SKIPPED_LOG.lock"

SCRIPT_TOTAL=$(( $(now_ns) - SCRIPT_START_TS ))
echo ""
echo "=================================================="
echo "Unpacking complete."
echo "Total time : $(format_duration $SCRIPT_TOTAL)"
echo "Output     : $UNPACKED_DIR"
echo "Errors     : $LOG_DIR/<project>/error.log (по одному файлу на проект)"
echo "Skipped    : $SKIPPED_LOG"
echo "=================================================="

exit "$exit_code"
