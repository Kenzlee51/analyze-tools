#!/bin/bash
#
# =============================================================================
# generate_json.sh — Скрипт генерации JSON с метаданными и хэшами файлов
# =============================================================================
#
# ОПИСАНИЕ:
#   Обходит распакованные директории проектов, вычисляет хэши всех файлов
#   и формирует JSON с метаданными для последующего анализа избыточности.
#   Обрабатывает папки src и bin каждого проекта одновременно.
#
#   Перед запуском выполняет самотест: снимает хэш с тестового файла
#   BASE_DIR/test/*.js и сравнивает с эталоном из BASE_DIR/test/*.txt.
#
# ИСПОЛЬЗОВАНИЕ:
#   ./generate_json.sh [OPTIONS]
#
# ОПЦИИ:
#   -j, --parallel N          Количество проектов обрабатываемых параллельно (default: 1)
#   -t, --threads N           Количество потоков хэширования внутри проекта (default: 1)
#   --single-project NAME     Обработать только один указанный проект
#   --ram                     Хэшировать через tmpfs (файлы <1GB копируются в RAM,
#                             файлы >=1GB хэшируются на месте). Требует sudo.
#   -v, --verbose             Подробный вывод: лог по каждому файлу.
#                             Без флага выводятся только основные шаги.
#   -h, --help                Показать справку
#
# ПРИМЕРЫ:
#   ./generate_json.sh                          # все проекты
#   ./generate_json.sh -j 1 -t 6               # один проект, все ядра
#   ./generate_json.sh --single-project proj1   # только proj1
#   ./generate_json.sh --ram                    # с tmpfs-ускорением
#   ./generate_json.sh --ram -v                 # с tmpfs + подробный лог
#   ./generate_json.sh --ram --single-project proj1
#
# ОЖИДАЕМАЯ СТРУКТУРА:
#   $BASE_DIR/
#   ├── scripts/
#   │   └── generate_json.sh
#   ├── test/
#   │   ├── test_file.js
#   │   └── test_file.txt
#   ├── unpacked/
#   │   ├── proj1/
#   │   │   ├── src/
#   │   │   └── bin/
#   │   └── proj2/
#   ├── logs/
#   │   └── generate-json/
#   │       ├── proj1/
#   │       │   ├── proj1.log        ← основной лог проекта
#   │       │   └── error_hash.log   ← ошибки хэширования
#   │       └── proj2/
#   └── results/
#       ├── proj1/
#       │   └── sources/
#       │       ├── proj1_src.json
#       │       └── proj1_bin.json
#       └── proj2/
#
# ЗАВИСИМОСТИ:
#   bash, find, stat, awk, mktemp, flock, xargs, timeout
#   + утилита хэширования заданная в HASH_CMD
#   + sudo mount (только при --ram)
# =============================================================================

set -uo pipefail

# =============================================================================
# КОМАНДА ХЭШИРОВАНИЯ
# =============================================================================
HASH_CMD="rhash -G"
# =============================================================================

# =============================================================================
# НАСТРАИВАЕМЫЕ ПУТИ
# =============================================================================
BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
UNPACKED_DIR="$BASE_DIR/unpacked"
RESULTS_DIR="$BASE_DIR/results"
LOGS_DIR="$BASE_DIR/logs/generate-json"
TEST_DIR="$BASE_DIR/test"

# Путь для монтирования tmpfs при --ram
TMPFS_MOUNT="$BASE_DIR/.tmpfs_work"

# Имена обрабатываемых подпапок проекта
SOURCE_SUBDIRS=("src" "bin")

# Порог размера файла: больше этого — хэшируется на месте (в байтах), 1 ГБ
LARGE_FILE_THRESHOLD=$(( 1 * 1024 * 1024 * 1024 ))
# =============================================================================

# --- Параметры по умолчанию ---
MAX_PARALLEL=1
HASH_THREADS=1
# Потоки хэширования внутри RAM-режима (по умолчанию = кол-во ядер CPU)
RAM_HASH_THREADS=$(nproc 2>/dev/null || echo 1)
SINGLE_PROJECT=""
USE_RAM=0
VERBOSE=0

# Функция логирования:
# log_info    — всегда выводится (основные шаги), с временной меткой
# log_verbose — только при -v (лог по каждому файлу), с временной меткой
# ts          — возвращает текущее время [HH:MM:SS]
ts() { date '+%H:%M:%S'; }
log_info() {
    echo "[$(ts)] $*"
}
log_verbose() {
    [[ $VERBOSE -eq 1 ]] && echo "[$(ts)] $*" || true
}

while [[ $# -gt 0 ]]; do
    case $1 in
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
        -t|--threads)
            if [[ -n "${2:-}" && "${2:-}" =~ ^[0-9]+$ ]]; then
                HASH_THREADS="$2"
                echo "[INFO] Hash threads per project: $HASH_THREADS"
                shift 2
            else
                echo "[ERROR] -t/--threads requires a number argument"
                exit 1
            fi
            ;;
        --single-project)
            if [[ -n "${2:-}" ]]; then
                SINGLE_PROJECT="$2"
                echo "[INFO] Single project mode: $SINGLE_PROJECT"
                shift 2
            else
                echo "[ERROR] --single-project requires a project name"
                exit 1
            fi
            ;;
        --ram)
            USE_RAM=1
            echo "[INFO] RAM mode enabled: small files will be hashed via tmpfs"
            shift
            ;;
        -v|--verbose)
            VERBOSE=1
            echo "[INFO] Verbose mode enabled"
            shift
            ;;
        -h|--help)
            head -70 "$0" | grep "^#" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

mkdir -p "$LOGS_DIR"

# =============================================================================
# УПРАВЛЕНИЕ TMPFS
# =============================================================================

# Глобальная переменная — путь к смонтированной tmpfs (пусто если не смонтирована)
TMPFS_MOUNTED=""

setup_tmpfs() {
    # Определяем сколько RAM доступно (в байтах)
    local mem_available_kb
    mem_available_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
    local mem_available_bytes=$(( mem_available_kb * 1024 ))

    # Берём 50% от доступной RAM
    local tmpfs_size_bytes=$(( mem_available_bytes / 2 ))
    local tmpfs_size_mb=$(( tmpfs_size_bytes / 1024 / 1024 ))

    echo "[RAM] Available RAM   : $(( mem_available_bytes / 1024 / 1024 )) MB"
    echo "[RAM] tmpfs size (50%): ${tmpfs_size_mb} MB"

    if [[ $tmpfs_size_mb -lt 512 ]]; then
        echo "[RAM] [WARN] Less than 512MB available for tmpfs — RAM mode may not help"
    fi

    mkdir -p "$TMPFS_MOUNT"

    # Если уже смонтирован — размонтируем и перемонтируем с актуальным размером
    if mount | grep -q "$TMPFS_MOUNT"; then
        echo "[RAM] tmpfs already mounted at $TMPFS_MOUNT, remounting..."
        sudo umount "$TMPFS_MOUNT" || {
            echo "[RAM] [ERROR] Failed to unmount existing tmpfs at $TMPFS_MOUNT"
            return 1
        }
    fi

    echo "[RAM] Mounting tmpfs (${tmpfs_size_mb}M) at $TMPFS_MOUNT ..."
    if ! sudo mount -t tmpfs -o "size=${tmpfs_size_mb}M" tmpfs "$TMPFS_MOUNT"; then
        echo "[RAM] [ERROR] Failed to mount tmpfs. Check sudo permissions."
        return 1
    fi

    TMPFS_MOUNTED="$TMPFS_MOUNT"
    echo "[RAM] tmpfs mounted successfully at $TMPFS_MOUNTED"
    return 0
}

teardown_tmpfs() {
    if [[ -n "$TMPFS_MOUNTED" ]] && mount | grep -q "$TMPFS_MOUNTED"; then
        echo "[RAM] Unmounting tmpfs at $TMPFS_MOUNTED ..."
        sudo umount "$TMPFS_MOUNTED" || echo "[RAM] [WARN] Failed to unmount tmpfs (may need manual cleanup)"
        TMPFS_MOUNTED=""
    fi
    # Удаляем точку монтирования если пустая
    rmdir "$TMPFS_MOUNT" 2>/dev/null || true
}

# Размонтировать при выходе (в том числе по Ctrl+C)
cleanup_on_exit() {
    teardown_tmpfs
}
trap cleanup_on_exit EXIT INT TERM

# =============================================================================
# САМОТЕСТ
# =============================================================================
run_selftest() {
    echo "[SELFTEST] Starting hash utility self-test..."
    echo "[SELFTEST] HASH_CMD: $HASH_CMD"
    echo "[SELFTEST] Test dir: $TEST_DIR"

    if [[ ! -d "$TEST_DIR" ]]; then
        echo "[SELFTEST] [ERROR] Test directory not found: $TEST_DIR"
        return 1
    fi

    local test_file
    test_file=$(find "$TEST_DIR" -maxdepth 1 -name "*.js" | head -1)
    if [[ -z "$test_file" ]]; then
        echo "[SELFTEST] [ERROR] No .js file found in $TEST_DIR"
        return 1
    fi

    local base="${test_file%.js}"
    local hash_file="${base}.txt"
    if [[ ! -f "$hash_file" ]]; then
        hash_file=$(find "$TEST_DIR" -maxdepth 1 -name "*.txt" | head -1)
    fi

    if [[ -z "$hash_file" || ! -f "$hash_file" ]]; then
        echo "[SELFTEST] [ERROR] No .txt reference hash file found in $TEST_DIR"
        return 1
    fi

    echo "[SELFTEST] Test file   : $test_file"
    echo "[SELFTEST] Reference   : $hash_file"

    local expected_hash
    expected_hash=$(grep -v '^\s*$' "$hash_file" | head -1 | awk '{print $1}')
    if [[ -z "$expected_hash" ]]; then
        echo "[SELFTEST] [ERROR] Reference hash file is empty: $hash_file"
        return 1
    fi
    echo "[SELFTEST] Expected    : $expected_hash"

    local actual_hash
    if ! actual_hash=$(${HASH_CMD} "$test_file" 2>/dev/null | awk '{print $1}'); then
        echo "[SELFTEST] [ERROR] HASH_CMD failed to run: $HASH_CMD"
        return 1
    fi

    if [[ -z "$actual_hash" ]]; then
        echo "[SELFTEST] [ERROR] HASH_CMD returned empty output for: $test_file"
        return 1
    fi
    echo "[SELFTEST] Actual      : $actual_hash"

    if [[ "${actual_hash,,}" == "${expected_hash,,}" ]]; then
        echo "[SELFTEST] [OK] Hash matches reference. Proceeding."
        return 0
    else
        echo "[SELFTEST] [MISMATCH] Hash does NOT match reference!"
        echo "[SELFTEST]   Expected : $expected_hash"
        echo "[SELFTEST]   Actual   : $actual_hash"
        return 1
    fi
}

if ! run_selftest; then
    echo ""
    echo "[SELFTEST] Hash utility verification FAILED."
    echo -n "[SELFTEST] Continue anyway? Results may be incorrect. [y/N]: "
    read -r answer
    if [[ "${answer,,}" != "y" && "${answer,,}" != "yes" ]]; then
        echo "[INFO] Aborted by user."
        exit 1
    fi
    echo "[WARN] Continuing despite failed self-test. Results may be incorrect."
else
    echo ""
fi

hash_bin="${HASH_CMD%% *}"
if ! command -v "$hash_bin" &> /dev/null; then
    echo "[ERROR] Hash utility not found: $hash_bin"
    exit 1
fi

if [[ ! -d "$UNPACKED_DIR" ]]; then
    echo "[ERROR] Unpacked directory not found: $UNPACKED_DIR"
    exit 1
fi

# Монтируем tmpfs если нужно
if [[ $USE_RAM -eq 1 ]]; then
    if ! setup_tmpfs; then
        echo "[RAM] [ERROR] Failed to setup tmpfs. Falling back to normal mode."
        USE_RAM=0
    fi
fi

# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

log_error_hash() {
    { flock 200; echo "$1" >> "$ERROR_HASH_LOG"; } 200>"$ERROR_HASH_LOG.lock"
    echo "[ERROR HASH] $1"
}

json_escape() {
    local s="$1"
    s="${s//\\/\\\\}"
    s="${s//\"/\\\"}"
    s="${s//$'\n'/\\n}"
    s="${s//$'\r'/\\r}"
    s="${s//$'\t'/\\t}"
    printf '"%s"' "$s"
}

get_virtual_path() {
    # Записывает результат в глобальную RESULT_VIRTUAL_PATH (без subshell)
    local real_path="$1"
    local root_dir="$2"
    local project_name="$3"
    local subdir="$4"

    local rel_path="${real_path#$root_dir/}"
    local virtual_path="$project_name/$subdir/$rel_path"
    RESULT_VIRTUAL_PATH="${virtual_path//_dir\//\/}"
}

get_versioned_filename() {
    local filepath="$1"
    if [[ ! -f "$filepath" ]]; then
        echo "$filepath"
        return
    fi
    local dir="${filepath%/*}"
    local filename="${filepath##*/}"
    local base="${filename%.json}"
    local version=1
    while [[ -f "$dir/${base}_v${version}.json" ]]; do
        (( version++ ))
    done
    echo "$dir/${base}_v${version}.json"
}

build_parents_info() {
    local file_path="$1"
    local root_dir="$2"

    local dir_path="${file_path%/*}"
    local nearest_hash="0"
    local chain_inner=""
    local first=1

    while [[ "$dir_path" != "$root_dir" && "$dir_path" != "/" ]]; do
        if [[ "$dir_path" == *_dir ]]; then
            local archive_path="${dir_path%_dir}"
            # Проверяем по hash_cache — без обращения к диску
            local parent_hash="${hash_cache[$archive_path]:-}"
            if [[ -n "$parent_hash" ]]; then
                if [[ $first -eq 1 ]]; then
                    nearest_hash="$parent_hash"
                fi
                local comma=""
                [[ $first -eq 0 ]] && comma=","
                first=0
                chain_inner="${chain_inner}${comma}\"${parent_hash}\""
            fi
        fi
        dir_path="${dir_path%/*}"
    done

    RESULT_PARENTS_HASH="$nearest_hash"
    RESULT_PARENTS_CHAIN="[${chain_inner}]"
}

write_sig() {
    local virtual_path="$1"
    local hash="$2"
    local archive="$3"
    local parents_hash="$4"
    local parents_chain="$5"
    local tmp_sigs="$6"
    local first_entry_ref="$7"

    local comma=""
    [[ ${!first_entry_ref} -eq 0 ]] && comma=","
    printf -v "$first_entry_ref" '%d' 0

    # json_escape без subshell — inline подстановки
    local ep="$virtual_path"
    ep="${ep//\\/\\\\}"
    ep="${ep//\"/\\\"}"
    ep="${ep//$'\n'/\\n}"
    ep="${ep//$'\r'/\\r}"
    ep="${ep//$'\t'/\\t}"

    printf '%s\n        {\n            "path": "%s",\n            "hash": "%s",\n            "archive": %d,\n            "parents_hash": "%s",\n            "parents_chain": %s\n        }' \
        "$comma" "$ep" "$hash" "$archive" "$parents_hash" "$parents_chain" >> "$tmp_sigs"
}

# =============================================================================
# ВОРКЕР хэширования (для xargs)
# =============================================================================
hash_worker() {
    local file="$1"
    local cache_file="$2"
    local cache_lock="$3"
    local counter_file="$4"
    local counter_lock="$5"
    local total="$6"
    local project_name="$7"
    local subdir="$8"
    local error_log="$9"
    local large_threshold="${10}"
    local hash_cmd="${11}"

    local file_size
    file_size=$(stat -c%s "$file" 2>/dev/null || echo 0)

    local current
    { flock 202
        current=$(cat "$counter_file" 2>/dev/null || echo 0)
        current=$(( current + 1 ))
        echo "$current" > "$counter_file"
    } 202>"$counter_lock"

    if [[ $file_size -ge $large_threshold ]]; then
        log_info "[$project_name/$subdir] [WARN] Large file: ${file##*/} ($(( file_size / 1024 / 1024 )) MB)"
    fi

    local timeout_sec=$(( file_size / 1024 / 1024 / 100 * 60 + 120 ))

    log_verbose "[$project_name/$subdir] [Pass 1] Hashing $current/$total: ${file##*/} ($(( file_size / 1024 / 1024 )) MB)"

    local hash
    if ! hash=$(timeout "$timeout_sec" ${hash_cmd} "$file" 2>/dev/null | awk '{print $1}'); then
        local exit_code=$?
        if [[ $exit_code -eq 124 ]]; then
            { flock 200; echo "TIMEOUT: $file (>${timeout_sec}s)" >> "$error_log"; } 200>"$error_log.lock"
            log_info "[$project_name/$subdir] [WARN] hash timed out: $file"
        else
            { flock 200; echo "FAILED (exit $exit_code): $file" >> "$error_log"; } 200>"$error_log.lock"
            log_info "[$project_name/$subdir] [ERROR] hash failed: $file"
        fi
        return 1
    fi

    if [[ -z "$hash" ]]; then
        { flock 200; echo "EMPTY_HASH: $file" >> "$error_log"; } 200>"$error_log.lock"
        log_info "[$project_name/$subdir] [ERROR] empty hash: $file"
        return 1
    fi

    local is_arc=0
    [[ -d "${file}_dir" ]] && is_arc=1

    { flock 202; printf '%s\t%s\t%d\n' "$file" "$hash" "$is_arc" >> "$cache_file"; } 202>"$cache_lock"
}

export -f hash_worker
export VERBOSE
export -f log_info
export -f log_verbose

# =============================================================================
# RAM-режим: хэширование чанками через tmpfs
# =============================================================================
# Хэширует список файлов из файла-списка.
# Маленькие файлы копируются в tmpfs, большие хэшируются на месте.
# Результаты пишутся в cache_file (формат: путь TAB хэш TAB is_arc)
# =============================================================================
hash_via_ram() {
    local subdir_path="$1"    # путь к subdir (откуда берём файлы)
    local cache_file="$2"     # TSV-кэш: оригинальный_путь TAB хэш
    local total="$3"
    local project_name="$4"
    local subdir="$5"
    local tmpfs_dir="$6"      # точка монтирования tmpfs

    # -------------------------------------------------------------------------
    # Определяем размер чанка — 70% от свободного места в tmpfs
    # (90% было мало — tar+inode overhead на мелких файлах достигает 30-50%)
    # -------------------------------------------------------------------------
    local tmpfs_avail_kb
    tmpfs_avail_kb=$(df -k "$tmpfs_dir" | awk 'NR==2 {print $4}')
    local chunk_max_bytes=$(( tmpfs_avail_kb * 1024 * 7 / 10 ))

    log_info "[$project_name/$subdir] [RAM] tmpfs available: $(( tmpfs_avail_kb / 1024 )) MB"
    log_info "[$project_name/$subdir] [RAM] chunk max size : $(( chunk_max_bytes / 1024 / 1024 )) MB (70% of tmpfs)"

    local chunk_dir="$tmpfs_dir/${project_name}_${subdir}_chunk"
    local chunk_num=0
    local processed=0

    # -------------------------------------------------------------------------
    # Получаем список файлов с размерами за ОДИН проход find
    # Формат строки: размер_байт TAB путь
    # Это единственный stat-эквивалент — без отдельных вызовов stat
    # -------------------------------------------------------------------------
    local sizelist
    sizelist=$(mktemp)
    find "$subdir_path" -type f -not -path "*/.queue.*" \
        -printf "%s\t%p\n" > "$sizelist"

    log_info "[$project_name/$subdir] [RAM] File list ready, starting chunked processing..."

    # -------------------------------------------------------------------------
    # Основной цикл: читаем sizelist, набираем чанк, копируем+хэшируем вместе
    # -------------------------------------------------------------------------
    # Массивы текущего чанка
    local chunk_orig=()    # оригинальные пути
    local chunk_dest=()    # пути в tmpfs
    local chunk_sizes=()   # размеры (для таймаута)
    local chunk_bytes=0

    # Вспомогательная функция — обработать накопленный чанк
    _flush_chunk() {
        [[ ${#chunk_orig[@]} -eq 0 ]] && return
        chunk_num=$(( chunk_num + 1 ))

        # Проверяем свободное место прямо перед копированием
        # С учётом overhead на inode/блоки для мелких файлов умножаем на 1.4
        local avail_now_kb
        avail_now_kb=$(df -k "$tmpfs_dir" | awk 'NR==2 {print $4}')
        local needed_bytes=$(( chunk_bytes * 14 / 10 ))
        local avail_bytes=$(( avail_now_kb * 1024 ))

        if [[ $needed_bytes -gt $avail_bytes ]]; then
            log_info "[$project_name/$subdir] [RAM] [WARN] Chunk $chunk_num: not enough tmpfs space (need $(( needed_bytes / 1024 / 1024 )) MB, have $(( avail_bytes / 1024 / 1024 )) MB) — hashing in place"
            # Хэшируем оригиналы на HDD без копирования
            local idx
            for idx in "${!chunk_orig[@]}"; do
                local orig="${chunk_orig[$idx]}"
                local fsize="${chunk_sizes[$idx]}"
                local timeout_sec=$(( fsize / 1024 / 1024 / 100 * 60 + 120 ))
                local hash
                if hash=$(timeout "$timeout_sec" ${HASH_CMD} "$orig" 2>/dev/null | awk '{print $1}') && [[ -n "$hash" ]]; then
                    local is_arc=0
                    [[ -d "${orig}_dir" ]] && is_arc=1
                    printf '%s\t%s\t%d\n' "$orig" "$hash" "$is_arc" >> "$cache_file"
                else
                    echo "FAILED: $orig" >> "$ERROR_HASH_LOG"
                fi
            done
            processed=$(( processed + ${#chunk_orig[@]} ))
            chunk_orig=(); chunk_dest=(); chunk_sizes=(); chunk_bytes=0
            return
        fi

        log_info "[$project_name/$subdir] [RAM] Chunk $chunk_num: copying ${#chunk_orig[@]} files ($(( chunk_bytes / 1024 / 1024 )) MB) to tmpfs..."

        # Копируем через tar pipe — один проход по HDD, минимум seekов
        local filelist_tmp
        filelist_tmp=$(mktemp)
        local idx
        for idx in "${!chunk_orig[@]}"; do
            echo "${chunk_orig[$idx]}"
        done > "$filelist_tmp"

        mkdir -p "$chunk_dir"
        if ! tar -c -T "$filelist_tmp" 2>/dev/null | tar -x -C "$chunk_dir/" 2>/dev/null; then
            log_info "[$project_name/$subdir] [RAM] [WARN] tar copy had errors — некоторые файлы будут хэшированы с HDD"
        fi
        rm -f "$filelist_tmp"

        log_info "[$project_name/$subdir] [RAM] Chunk $chunk_num: copy done. Hashing (${RAM_HASH_THREADS} threads)..."

        # Строим маппинг: src_path TAB orig_path TAB fsize
        local mapfile_tmp
        mapfile_tmp=$(mktemp)
        for idx in "${!chunk_orig[@]}"; do
            local orig="${chunk_orig[$idx]}"
            local fsize="${chunk_sizes[$idx]}"
            local rel="${orig#/}"
            local src="$chunk_dir/$rel"
            [[ -f "$src" ]] || src="$orig"   # fallback если tar не скопировал
            printf '%s\t%s\t%s\n' "$src" "$orig" "$fsize"
        done > "$mapfile_tmp"

        local chunk_cache_tmp
        chunk_cache_tmp=$(mktemp)
        local chunk_err_tmp
        chunk_err_tmp=$(mktemp)

        # Параллельное хэширование через xargs -P с -d '\n' —
        # строки читаются как данные (не подставляются в команду),
        # поэтому спецсимволы в путях не ломают разбор
        export HASH_CMD
        local chunk_cache_tmp_xargs
        chunk_cache_tmp_xargs=$(mktemp)
        local chunk_err_tmp_xargs
        chunk_err_tmp_xargs=$(mktemp)

        # worker_out нужен наблюдателю — используем единый выходной файл
        local worker_out=("$chunk_cache_tmp_xargs")
        local worker_err=("$chunk_err_tmp_xargs")

        # Фоновый наблюдатель — запускаем после того как worker_out заполнен
        local chunk_total=${#chunk_orig[@]}
        local sentinel_file
        sentinel_file=$(mktemp)
        local captured_worker_out=("${worker_out[@]}")
        (
            local last=0
            while [[ -f "$sentinel_file" ]]; do
                sleep 5
                local done_now=0
                local wout
                for wout in "${captured_worker_out[@]}"; do
                    done_now=$(( done_now + $(wc -l < "$wout" 2>/dev/null || echo 0) ))
                done
                if [[ $done_now -ge $(( last + 10000 )) ]]; then
                    local global_est=$(( processed + done_now ))
                    log_info "[$project_name/$subdir] [RAM] Chunk $chunk_num progress: $done_now/$chunk_total (total: $global_est/$total)"
                    last=$done_now
                fi
            done
        ) &
        local monitor_pid=$!

        # Экранируем $ в путях перед передачей в xargs -I
        # чтобы bash не интерпретировал $1, $2 и т.д. в именах файлов
        local mapfile_escaped
        mapfile_escaped=$(mktemp)
        sed 's/\$/__DOLLAR__/g' "$mapfile_tmp" > "$mapfile_escaped"

        xargs -d '\n' -P "$RAM_HASH_THREADS" -I'__MAPLINE__' \
            bash -c '
                line="__MAPLINE__"
                line="${line//__DOLLAR__/$}"
                IFS=$'"'"'\t'"'"' read -r src orig fsize <<< "$line"
                timeout_sec=$(( fsize / 1024 / 1024 / 100 * 60 + 120 ))
                hash=$(timeout "$timeout_sec" $HASH_CMD "$src" 2>/dev/null | awk '"'"'{print $1}'"'"')
                if [[ -n "$hash" ]]; then
                    is_arc=0
                    [[ -d "${orig}_dir" ]] && is_arc=1
                    printf "%s\t%s\t%d\n" "$orig" "$hash" "$is_arc"
                else
                    printf "FAILED: %s\n" "$orig" >&2
                fi
            ' < "$mapfile_escaped" >> "$chunk_cache_tmp_xargs" 2>> "$chunk_err_tmp_xargs"

        rm -f "$mapfile_escaped"

        # Останавливаем наблюдатель
        rm -f "$sentinel_file" "$mapfile_tmp"
        kill "$monitor_pid" 2>/dev/null
        wait "$monitor_pid" 2>/dev/null

        # Переносим результаты в основной кэш
        cat "$chunk_cache_tmp_xargs" >> "$chunk_cache_tmp"
        if [[ -s "$chunk_err_tmp_xargs" ]]; then
            cat "$chunk_err_tmp_xargs" >> "$chunk_err_tmp"
        fi
        rm -f "$chunk_cache_tmp_xargs" "$chunk_err_tmp_xargs"

        # Переносим результаты в основной кэш
        cat "$chunk_cache_tmp" >> "$cache_file"

        if [[ -s "$chunk_err_tmp" ]]; then
            cat "$chunk_err_tmp" >> "$ERROR_HASH_LOG"
            log_info "[$project_name/$subdir] [RAM] [WARN] Some hashes failed, see error_hash.log"
        fi

        local chunk_hashed
        chunk_hashed=$(wc -l < "$chunk_cache_tmp")
        processed=$(( processed + chunk_hashed ))
        rm -f "$chunk_cache_tmp" "$chunk_err_tmp"

        log_info "[$project_name/$subdir] [RAM] Chunk $chunk_num: done ($chunk_hashed hashed, $processed/$total total). Cleaning tmpfs..."
        rm -rf "$chunk_dir"

        # Сбрасываем массивы чанка
        chunk_orig=()
        chunk_dest=()
        chunk_sizes=()
        chunk_bytes=0
    }

    # Читаем sizelist — размер и путь уже известны, stat больше не нужен
    while IFS=$'\t' read -r fsize filepath; do

        # Большие файлы (>= LARGE_FILE_THRESHOLD) — хэшируем на месте без копирования
        if [[ $fsize -ge $LARGE_FILE_THRESHOLD ]]; then
            (( processed++ )) || true
            log_info "[$project_name/$subdir] [RAM] [LARGE] Hashing $processed/$total in place: ${filepath##*/} ($(( fsize / 1024 / 1024 )) MB)"
            local timeout_sec=$(( fsize / 1024 / 1024 / 100 * 60 + 120 ))
            local hash
            if hash=$(timeout "$timeout_sec" ${HASH_CMD} "$filepath" 2>/dev/null | awk '{print $1}') && [[ -n "$hash" ]]; then
                local is_arc=0
                [[ -d "${filepath}_dir" ]] && is_arc=1
                printf '%s\t%s\t%d\n' "$filepath" "$hash" "$is_arc" >> "$cache_file"
            else
                echo "FAILED (large): $filepath" >> "$ERROR_HASH_LOG"
                log_info "[$project_name/$subdir] [RAM] [ERROR] Failed to hash: ${filepath##*/}"
            fi
            continue
        fi

        # Файл не влезает даже в пустой чанк — хэшируем на месте
        if [[ $fsize -ge $chunk_max_bytes ]]; then
            log_info "[$project_name/$subdir] [RAM] [WARN] File exceeds chunk limit, hashing in place: ${filepath##*/}"
            (( processed++ )) || true
            local timeout_sec=$(( fsize / 1024 / 1024 / 100 * 60 + 120 ))
            local hash
            if hash=$(timeout "$timeout_sec" ${HASH_CMD} "$filepath" 2>/dev/null | awk '{print $1}') && [[ -n "$hash" ]]; then
                local is_arc=0
                [[ -d "${filepath}_dir" ]] && is_arc=1
                printf '%s\t%s\t%d\n' "$filepath" "$hash" "$is_arc" >> "$cache_file"
            else
                echo "FAILED: $filepath" >> "$ERROR_HASH_LOG"
            fi
            continue
        fi

        # Чанк переполнится — сбрасываем текущий и начинаем новый
        if [[ $(( chunk_bytes + fsize )) -gt $chunk_max_bytes && ${#chunk_orig[@]} -gt 0 ]]; then
            _flush_chunk
        fi

        # Добавляем файл в чанк
        local rel="${filepath#$UNPACKED_DIR/}"
        chunk_orig+=("$filepath")
        chunk_dest+=("$chunk_dir/$rel")
        chunk_sizes+=("$fsize")
        chunk_bytes=$(( chunk_bytes + fsize ))

    done < "$sizelist"

    # Сбрасываем последний чанк если что-то осталось
    _flush_chunk

    rm -f "$sizelist"

    log_info "[$project_name/$subdir] [RAM] All done. Total chunks: $chunk_num, processed: $processed/$total"
}

export -f hash_via_ram

# =============================================================================
# Обработка одной подпапки проекта (src или bin)
# =============================================================================
process_subdir() {
    set +e

    local project_name="$1"
    local subdir="$2"
    local subdir_path="$UNPACKED_DIR/$project_name/$subdir"

    echo "--- Processing: $project_name/$subdir ---"

    local output_dir="$RESULTS_DIR/$project_name/sources"
    mkdir -p "$output_dir"

    local json_base="$output_dir/${project_name}_${subdir}.json"
    local json_file
    json_file=$(get_versioned_filename "$json_base")

    if [[ "$json_file" != "$json_base" ]]; then
        log_info "[$project_name/$subdir] [WARN] File exists, writing to: ${json_file##*/}"
    fi

    local cache_file tmp_sigs
    cache_file=$(mktemp)
    tmp_sigs=$(mktemp)
    # counter_file/lock нужны только для обычного режима (xargs параллельный)
    local counter_file counter_lock cache_lock
    counter_file=$(mktemp)
    cache_lock="${cache_file}.lock"
    counter_lock="${counter_file}.lock"
    echo "0" > "$counter_file"

    # =========================================================================
    # ПРОХОД 1 — хэширование
    # =========================================================================
    log_info "[$project_name/$subdir] [Pass 1] Counting files..."
    local total
    total=$(find "$subdir_path" -type f -not -path "*/.queue.*" | wc -l)
    log_info "[$project_name/$subdir] [Pass 1] Total files: $total"

    if [[ $USE_RAM -eq 1 && -n "$TMPFS_MOUNTED" ]]; then
        log_info "[$project_name/$subdir] [Pass 1] Mode: RAM (tmpfs at $TMPFS_MOUNTED)"

        hash_via_ram \
            "$subdir_path" \
            "$cache_file" \
            "$total" "$project_name" "$subdir" \
            "$TMPFS_MOUNTED"

    else
        log_info "[$project_name/$subdir] [Pass 1] Mode: normal (HDD/SSD)"
        log_info "[$project_name/$subdir] [Pass 1] Starting parallel hashing ($HASH_THREADS threads)..."
        log_info "[$project_name/$subdir] [Pass 1] HASH_CMD: $HASH_CMD"

        find "$subdir_path" -type f -not -path "*/.queue.*" -print0 | \
            xargs -0 -P "$HASH_THREADS" -I{} \
            bash -c 'hash_worker "$@"' _ {} \
                "$cache_file" "$cache_lock" \
                "$counter_file" "$counter_lock" \
                "$total" "$project_name" "$subdir" \
                "$ERROR_HASH_LOG" "$LARGE_FILE_THRESHOLD" \
                "$HASH_CMD"
    fi

    log_info "[$project_name/$subdir] [Pass 1] Hashing complete."

    # =========================================================================
    # ПРОХОД 2 — построение JSON из кэша
    # Шаг 2а: загружаем только hash_cache (путь→хэш) для lookup родителей
    # Шаг 2б: читаем кэш построчно → сразу пишем в JSON через открытый fd
    # =========================================================================
    log_info "[$project_name/$subdir] [Pass 2] Loading hash_cache for parent lookup..."

    declare -A hash_cache=()
    declare -A parents_cache=()   # кэш родителей: dir_path → "hash|chain"
    while IFS=$'\t' read -r fpath fhash _farc; do
        hash_cache["$fpath"]="$fhash"
    done < "$cache_file"

    log_info "[$project_name/$subdir] [Pass 2] hash_cache loaded: ${#hash_cache[@]} entries"
    log_info "[$project_name/$subdir] [Pass 2] Building JSON..."

    local total2
    total2=$(wc -l < "$cache_file")
    local current2=0
    local first_entry=1

    # Открываем JSON файл и пишем заголовок
    local esc_dir="$project_name/$subdir"
    esc_dir="${esc_dir//\\/\\\\}"
    esc_dir="${esc_dir//\"/\\\"}"
    printf '{\n    "directory": "%s",\n    "signatures": [' "$esc_dir" > "$json_file"

    # Открываем файловый дескриптор для записи signatures — один раз на весь проход
    exec 4>> "$json_file"

    # Построчно читаем кэш — один проход, без итерации по bash-массиву
    while IFS=$'\t' read -r fpath fhash farc; do
        (( current2++ )) || true
        if (( current2 % 10000 == 0 )); then
            log_info "[$project_name/$subdir] [Pass 2] Progress: $current2/$total2"
        fi
        log_verbose "[$project_name/$subdir] [Pass 2] $current2: ${fpath##*/}"

        # get_virtual_path inline
        local rel_path="${fpath#$subdir_path/}"
        local vpath="$project_name/$subdir/$rel_path"
        RESULT_VIRTUAL_PATH="${vpath//_dir\//\/}"

        # build_parents_info с кэшем директорий
        local dir_path="${fpath%/*}"
        if [[ -n "${parents_cache[$dir_path]:-}" ]]; then
            IFS='|' read -r RESULT_PARENTS_HASH RESULT_PARENTS_CHAIN \
                <<< "${parents_cache[$dir_path]}"
        else
            local dp="$dir_path"
            local nearest_hash="0"
            local chain_inner=""
            local pfirst=1
            while [[ "$dp" != "$subdir_path" && "$dp" != "/" ]]; do
                if [[ "$dp" == *_dir ]]; then
                    local archive_path="${dp%_dir}"
                    local parent_hash="${hash_cache[$archive_path]:-}"
                    if [[ -n "$parent_hash" ]]; then
                        [[ $pfirst -eq 1 ]] && nearest_hash="$parent_hash"
                        local comma=""
                        [[ $pfirst -eq 0 ]] && comma=","
                        pfirst=0
                        chain_inner="${chain_inner}${comma}\"${parent_hash}\""
                    fi
                fi
                dp="${dp%/*}"
            done
            RESULT_PARENTS_HASH="$nearest_hash"
            RESULT_PARENTS_CHAIN="[${chain_inner}]"
            parents_cache[$dir_path]="${RESULT_PARENTS_HASH}|${RESULT_PARENTS_CHAIN}"
        fi

        # write_sig inline через открытый fd 4
        local comma=""
        [[ $first_entry -eq 0 ]] && comma=","
        first_entry=0

        local ep="$RESULT_VIRTUAL_PATH"
        ep="${ep//\\/\\\\}"
        ep="${ep//\"/\\\"}"
        ep="${ep//$'\n'/\\n}"
        ep="${ep//$'\r'/\\r}"
        ep="${ep//$'\t'/\\t}"

        printf '%s\n        {\n            "path": "%s",\n            "hash": "%s",\n            "archive": %s,\n            "parents_hash": "%s",\n            "parents_chain": %s\n        }' \
            "$comma" "$ep" "$fhash" "$farc" \
            "$RESULT_PARENTS_HASH" "$RESULT_PARENTS_CHAIN" >&4

    done < "$cache_file"

    # Закрываем fd и завершаем JSON
    exec 4>&-
    printf '\n    ]\n}\n' >> "$json_file"

    log_info "[$project_name/$subdir] [Pass 2] JSON entries written: $current2"

    local write_status=$?
    rm -f "$tmp_sigs" "$cache_file" "$cache_lock" "$counter_file" "$counter_lock"

    if [[ $write_status -ne 0 ]]; then
        log_info "[ERROR] Failed to write JSON: $json_file"
        return 1
    fi

    echo "--- Done: $project_name/$subdir → $json_file ---"
}

# =============================================================================
# Обработка одного проекта
# =============================================================================
process_project() {
    set +e

    local project_name="$1"
    local project_dir="$UNPACKED_DIR/$project_name"

    # Создаём папку логов для проекта и устанавливаем путь к error-логу
    local project_log_dir="$LOGS_DIR/$project_name"
    mkdir -p "$project_log_dir"
    ERROR_HASH_LOG="$project_log_dir/error_hash.log"
    export ERROR_HASH_LOG
    > "$ERROR_HASH_LOG"

    echo "=== Processing project: $project_name ==="
    echo "=== Logs: $project_log_dir ==="

    local project_exit=0

    for subdir in "${SOURCE_SUBDIRS[@]}"; do
        local subdir_path="$project_dir/$subdir"

        if [[ ! -d "$subdir_path" ]]; then
            echo "[$project_name] [INFO] Subdir not found, skipping: $subdir"
            continue
        fi

        if ! process_subdir "$project_name" "$subdir"; then
            echo "[ERROR] Subdir processing failed: $project_name/$subdir"
            project_exit=1
        fi
    done

    if [[ $project_exit -eq 0 ]]; then
        echo "=== Done: $project_name ==="
    else
        echo "=== Failed: $project_name ==="
    fi

    return $project_exit
}

# =============================================================================
# ОСНОВНОЙ ЦИКЛ
# =============================================================================

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
        projects+=("${dir%/}")
        projects[-1]="${projects[-1]##*/}"
    done

    if [[ ${#projects[@]} -eq 0 ]]; then
        echo "[ERROR] No projects found in: $UNPACKED_DIR"
        exit 1
    fi
fi

echo "[INFO] Projects to process: ${#projects[@]}"
echo "[INFO] Projects: ${projects[*]}"
echo "[INFO] HASH_CMD: $HASH_CMD"
echo "[INFO] RAM mode : $( [[ $USE_RAM -eq 1 ]] && echo 'ON' || echo 'OFF' )"
echo "[INFO] RAM threads: $RAM_HASH_THREADS (CPU cores: $(nproc 2>/dev/null || echo '?'))"
echo "[INFO] Verbose  : $( [[ $VERBOSE -eq 1 ]] && echo 'ON (per-file logging)' || echo 'OFF (steps only)' )"
export RAM_HASH_THREADS

mkdir -p "$RESULTS_DIR"

# Время старта всего скрипта
GLOBAL_START=$(date '+%Y-%m-%d %H:%M:%S')
GLOBAL_START_TS=$(date +%s)

pids=()
pid_projects=()   # соответствие pid → project_name
declare -A project_start_ts=()
declare -A project_elapsed=()
exit_code=0

for project_name in "${projects[@]}"; do
    if [[ ${#pids[@]} -ge $MAX_PARALLEL ]]; then
        echo "[INFO] Reached max parallel jobs ($MAX_PARALLEL), waiting..."
        wait -n 2>/dev/null || wait
        new_pids=()
        new_pid_projects=()
        for i in "${!pids[@]}"; do
            if kill -0 "${pids[$i]}" 2>/dev/null; then
                new_pids+=("${pids[$i]}")
                new_pid_projects+=("${pid_projects[$i]}")
            else
                # Процесс завершился — фиксируем время
                finished_proj="${pid_projects[$i]}"
                if [[ -n "${project_start_ts[$finished_proj]:-}" ]]; then
                    elapsed=$(( $(date +%s) - project_start_ts[$finished_proj] ))
                    h=$(( elapsed / 3600 ))
                    m=$(( (elapsed % 3600) / 60 ))
                    s=$(( elapsed % 60 ))
                    project_elapsed[$finished_proj]=$(printf '%02d:%02d:%02d' $h $m $s)
                fi
            fi
        done
        pids=("${new_pids[@]}")
        pid_projects=("${new_pid_projects[@]}")
    fi

    local_log="$LOGS_DIR/${project_name}/${project_name}.log"
    mkdir -p "$LOGS_DIR/${project_name}"
    echo "[INFO] Starting project: $project_name (log: $local_log)"
    project_start_ts[$project_name]=$(date +%s)
    process_project "$project_name" 2>&1 | tee -a "$local_log" &
    pids+=($!)
    pid_projects+=("$project_name")
done

echo "[INFO] Waiting for all remaining projects to complete..."
for i in "${!pids[@]}"; do
    pid="${pids[$i]}"
    proj="${pid_projects[$i]}"
    if ! wait "$pid"; then
        echo "[ERROR] Process $pid ($proj) failed"
        exit_code=1
    fi
    # Фиксируем время завершения
    if [[ -n "${project_start_ts[$proj]:-}" ]]; then
        elapsed=$(( $(date +%s) - project_start_ts[$proj] ))
        h=$(( elapsed / 3600 ))
        m=$(( (elapsed % 3600) / 60 ))
        s=$(( elapsed % 60 ))
        project_elapsed[$proj]=$(printf '%02d:%02d:%02d' $h $m $s)
    fi
done

GLOBAL_END=$(date '+%Y-%m-%d %H:%M:%S')
GLOBAL_ELAPSED=$(( $(date +%s) - GLOBAL_START_TS ))
GH=$(( GLOBAL_ELAPSED / 3600 ))
GM=$(( (GLOBAL_ELAPSED % 3600) / 60 ))
GS=$(( GLOBAL_ELAPSED % 60 ))
GLOBAL_ELAPSED_FMT=$(printf '%02d:%02d:%02d' $GH $GM $GS)

echo ""
echo "=========================================="
echo "JSON generation complete!"
echo "=========================================="
echo "Output  : $RESULTS_DIR"
echo "Logs    : $LOGS_DIR"
echo "HASH_CMD: $HASH_CMD"
echo ""
echo "--- Время выполнения по проектам ---"
for project_name in "${projects[@]}"; do
    printf "  %-30s: %s\n" "$project_name" "${project_elapsed[$project_name]:-unknown}"
done
echo ""
echo "--- Итого ---"
echo "  Начало : $GLOBAL_START"
echo "  Конец  : $GLOBAL_END"
echo "  Всего  : $GLOBAL_ELAPSED_FMT"
echo "=========================================="
exit "$exit_code"
