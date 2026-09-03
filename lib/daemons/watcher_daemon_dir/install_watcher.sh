#!/bin/bash
# ============================================================
# INSTALLER FOR DIRECTORY WATCHER DAEMON
# Version: 1.0
# ============================================================
#
# DESCRIPTION:
#   This script installs and configures a daemon that automatically
#   creates 'src' and 'bin' subdirectories inside any new folder
#   created in the ./src directory.
#
# INSTALLATION:
#   1. Make this script executable:
#      chmod +x install_watcher.sh
#
#   2. Run the installer:
#      ./install_watcher.sh
#
#   3. The script will:
#      - Detect your OS and install inotify-tools if needed
#      - Create directory structure (lib/daemons/, logs/)
#      - Generate daemon script in lib/daemons/watcher_daemon_dir.sh
#      - Create configuration file in lib/daemons/watcher.conf
#      - Setup systemd service (if supported)
#      - Start the daemon immediately
#
# USAGE AFTER INSTALLATION:
#   Start daemon:
#     ./lib/daemons/watcher_daemon_dir.sh start
#
#   Stop daemon:
#     ./lib/daemons/watcher_daemon_dir.sh stop
#
#   Restart daemon:
#     ./lib/daemons/watcher_daemon_dir.sh restart
#
#   Check status:
#     ./lib/daemons/watcher_daemon_dir.sh status
#
#   View logs:
#     tail -f logs/watcher.log
#
#   Edit configuration:
#     nano lib/daemons/watcher.conf
#
# SYSTEMD SERVICE (if available):
#   Enable autostart:
#     sudo systemctl enable analyze-watcher
#
#   Start/stop:
#     sudo systemctl start analyze-watcher
#     sudo systemctl stop analyze-watcher
#     sudo systemctl status analyze-watcher
#
#   Disable autostart:
#     sudo systemctl disable analyze-watcher
#
# TESTING:
#   Create a test folder:
#     mkdir src/TEST/0001-01
#
#   Check that subfolders were created:
#     ls -la src/TEST/0001-01/
#     # Should show: bin/  src/
#
#   Check logs:
#     tail -f logs/watcher.log
#
# UNINSTALL:
#   1. Stop the daemon:
#      ./lib/daemons/watcher_daemon_dir.sh stop
#
#   2. Disable systemd service (if used):
#      sudo systemctl disable analyze-watcher
#      sudo rm /etc/systemd/system/analyze-watcher.service
#      sudo systemctl daemon-reload
#
#   3. Remove files:
#      rm -rf lib/daemons/
#      rm -f watcher.pid
#      rm -f logs/watcher.log
#
# TROUBLESHOOTING:
#   - If inotify-tools installation fails, install manually:
#     Ubuntu/Debian: sudo apt-get install inotify-tools
#     CentOS/RHEL:   sudo yum install inotify-tools
#     Arch:          sudo pacman -S inotify-tools
#
#   - If daemon doesn't start, check logs:
#     tail -f logs/watcher.log
#
#   - If daemon doesn't see new folders via FTP, ensure:
#     * Daemon is running with correct user permissions
#     * FTP user has write permissions in ./src
#     * The directory is on the same filesystem (not NFS)
#
#   - To change watched directory, edit:
#     lib/daemons/watcher.conf
#     and restart daemon
#
# ============================================================

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAEMON_DIR="$PROJECT_ROOT/lib/daemons"
DAEMON_SCRIPT="$DAEMON_DIR/watcher_daemon_dir.sh"
LOG_DIR="$PROJECT_ROOT/logs"
WATCH_BASE="$PROJECT_ROOT/src"

# --- FUNCTIONS ---

detect_os() {
    if [[ -f /etc/os-release ]]; then
        . /etc/os-release
        echo "$ID"
    else
        echo "unknown"
    fi
}

install_inotify() {
    local os=$(detect_os)
    
    if command -v inotifywait &> /dev/null; then
        echo "[OK] inotify-tools already installed"
        return 0
    fi
    
    echo "[INFO] Installing inotify-tools..."
    
    case "$os" in
        ubuntu|debian)
            sudo apt-get update -qq && sudo apt-get install -y inotify-tools
            ;;
        centos|rhel|fedora)
            sudo yum install -y inotify-tools
            ;;
        arch|manjaro)
            sudo pacman -S --noconfirm inotify-tools
            ;;
        alpine)
            sudo apk add inotify-tools
            ;;
        *)
            echo "[WARN] Unknown OS: $os"
            echo "Please install inotify-tools manually:"
            echo "  Ubuntu/Debian: sudo apt-get install inotify-tools"
            echo "  CentOS/RHEL:   sudo yum install inotify-tools"
            echo "  Arch:          sudo pacman -S inotify-tools"
            return 1
            ;;
    esac
    
    if command -v inotifywait &> /dev/null; then
        echo "[OK] inotify-tools installed successfully"
        return 0
    else
        echo "[ERROR] Failed to install inotify-tools"
        return 1
    fi
}

create_directories() {
    echo "[INFO] Creating directory structure..."
    
    mkdir -p "$DAEMON_DIR"
    mkdir -p "$LOG_DIR"
    mkdir -p "$WATCH_BASE"
    
    echo "[OK] Directories created:"
    echo "  - $DAEMON_DIR"
    echo "  - $LOG_DIR"
    echo "  - $WATCH_BASE"
}

create_daemon_script() {
    echo "[INFO] Creating daemon: $DAEMON_SCRIPT"
    
    cat > "$DAEMON_SCRIPT" << 'EOF'
#!/bin/bash
# ============================================================
# DIRECTORY WATCHER DAEMON
# Automatically creates src/ and bin/ subdirectories
# Installed: 2026-09-03
# ============================================================
#
# USAGE:
#   $0 start   - Start the daemon in background
#   $0 stop    - Stop the daemon
#   $0 restart - Restart the daemon
#   $0 status  - Check if daemon is running
#
# CONFIGURATION:
#   Edit ../watcher.conf (relative to this script location)
#   Or set environment variables:
#     PROJECT_ROOT - Project root directory
#     WATCH_BASE   - Directory to watch
#     MAX_DEPTH    - Maximum depth to process
#     EXCLUDE_DIRS - Array of directory names to exclude
#
# LOGS:
#   $PROJECT_ROOT/logs/watcher.log
#
# ============================================================

PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
WATCH_BASE="${WATCH_BASE:-$PROJECT_ROOT/src}"
EXCLUDE_DIRS=("src" "bin" "lib" "logs" "results" "test")
LOG_DIR="$PROJECT_ROOT/logs"
LOG_FILE="$LOG_DIR/watcher.log"
PID_FILE="$PROJECT_ROOT/watcher.pid"
MAX_DEPTH=1

# --- FUNCTIONS ---

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "$msg" >> "$LOG_FILE"
    echo "$msg"
}

is_excluded() {
    local dir_name=$(basename "$1")
    for excluded in "${EXCLUDE_DIRS[@]}"; do
        [[ "$dir_name" == "$excluded" ]] && return 0
    done
    return 1
}

check_depth() {
    local target_path="$1"
    local relative_path="${target_path#$WATCH_BASE/}"
    local depth=$(echo "$relative_path" | tr -cd '/' | wc -c)
    [[ $depth -ge $MAX_DEPTH ]]
}

create_subdirs() {
    local new_dir="$1"
    
    [[ ! -d "$new_dir" ]] && return 1
    is_excluded "$new_dir" && return 1
    check_depth "$new_dir" && return 1
    
    if [[ -d "$new_dir/src" ]] && [[ -d "$new_dir/bin" ]]; then
        return 0
    fi
    
    mkdir -p "$new_dir/src" "$new_dir/bin" 2>/dev/null
    
    if [[ $? -eq 0 ]]; then
        log "[OK] Created subdirs in: $new_dir"
        log "  +-- src/"
        log "  +-- bin/"
        return 0
    else
        log "[ERROR] Failed to create subdirs in: $new_dir"
        return 1
    fi
}

process_existing_dirs() {
    log "[INFO] Checking existing directories..."
    find "$WATCH_BASE" -mindepth 1 -maxdepth $MAX_DEPTH -type d 2>/dev/null | while read dir; do
        create_subdirs "$dir"
    done
}

start_daemon() {
    log "[INFO] Starting watcher daemon"
    log "  Base: $WATCH_BASE"
    log "  Depth: $MAX_DEPTH"
    
    if ! command -v inotifywait &> /dev/null; then
        log "[ERROR] inotifywait not found"
        log "  Install: sudo apt-get install inotify-tools"
        exit 1
    fi
    
    mkdir -p "$WATCH_BASE"
    process_existing_dirs
    
    log "[INFO] Watching for new directories..."
    
    inotifywait -m -r -e CREATE --format '%w%f' "$WATCH_BASE" 2>/dev/null | while read full_path
    do
        if [[ -d "$full_path" ]]; then
            sleep 0.1
            create_subdirs "$full_path"
        fi
    done
}

stop_daemon() {
    if [[ -f "$PID_FILE" ]]; then
        local pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            log "[INFO] Stopping daemon (PID: $pid)"
            kill "$pid"
            rm -f "$PID_FILE"
            log "[OK] Daemon stopped"
        else
            rm -f "$PID_FILE"
        fi
    fi
}

status_daemon() {
    if [[ -f "$PID_FILE" ]]; then
        local pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            log "[OK] Daemon is running (PID: $pid)"
            return 0
        else
            rm -f "$PID_FILE"
        fi
    fi
    log "[INFO] Daemon is not running"
    return 1
}

case "$1" in
    start)
        start_daemon &
        echo $! > "$PID_FILE"
        log "[OK] Daemon started (PID: $(cat $PID_FILE))"
        ;;
    stop)
        stop_daemon
        ;;
    restart)
        stop_daemon
        sleep 1
        start_daemon &
        echo $! > "$PID_FILE"
        log "[OK] Daemon restarted (PID: $(cat $PID_FILE))"
        ;;
    status)
        status_daemon
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac

exit 0
EOF

    chmod +x "$DAEMON_SCRIPT"
    echo "[OK] Daemon created: $DAEMON_SCRIPT"
}

create_config() {
    local config_file="$DAEMON_DIR/watcher.conf"
    
    cat > "$config_file" << EOF
# Watcher daemon configuration
# Created: $(date '+%Y-%m-%d %H:%M:%S')

# Base directory to watch
WATCH_BASE="$PROJECT_ROOT/src"

# Excluded directories (no src/bin will be created inside)
EXCLUDE_DIRS=("src" "bin" "lib" "logs" "results" "test")

# Maximum depth (1 = only first level)
MAX_DEPTH=1

# Check interval in seconds (for fallback mode without inotify)
CHECK_INTERVAL=2
EOF

    echo "[OK] Config created: $config_file"
}

create_systemd_service() {
    if command -v systemctl &> /dev/null; then
        echo "[INFO] Creating systemd service..."
        
        local service_name="analyze-watcher"
        local service_file="/etc/systemd/system/${service_name}.service"
        
        sudo bash -c "cat > $service_file" << EOF
[Unit]
Description=Analyze Tools Directory Watcher
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=$PROJECT_ROOT
ExecStart=$DAEMON_SCRIPT start
ExecStop=$DAEMON_SCRIPT stop
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

        sudo systemctl daemon-reload
        
        echo "[OK] Systemd service created: $service_name"
        echo "  Commands:"
        echo "  sudo systemctl start $service_name"
        echo "  sudo systemctl stop $service_name"
        echo "  sudo systemctl status $service_name"
        echo "  sudo systemctl enable $service_name  # autostart"
    fi
}

start_watcher() {
    echo "[INFO] Starting watcher..."
    "$DAEMON_SCRIPT" start
    
    sleep 1
    "$DAEMON_SCRIPT" status
}

# --- MAIN ---

main() {
    echo "=========================================="
    echo "  Watcher Daemon Installer"
    echo "=========================================="
    echo ""
    
    echo "[1/6] Checking dependencies..."
    install_inotify
    echo ""
    
    echo "[2/6] Creating directory structure..."
    create_directories
    echo ""
    
    echo "[3/6] Creating configuration..."
    create_config
    echo ""
    
    echo "[4/6] Creating daemon script..."
    create_daemon_script
    echo ""
    
    echo "[5/6] Setting up autostart..."
    create_systemd_service
    echo ""
    
    echo "[6/6] Starting daemon..."
    start_watcher
    echo ""
    
    echo "=========================================="
    echo "  Installation complete!"
    echo "=========================================="
    echo ""
    echo "Daemon management:"
    echo "  $DAEMON_SCRIPT start    - start"
    echo "  $DAEMON_SCRIPT stop     - stop"
    echo "  $DAEMON_SCRIPT restart  - restart"
    echo "  $DAEMON_SCRIPT status   - check status"
    echo ""
    echo "Logs:"
    echo "  tail -f $LOG_DIR/watcher.log"
    echo ""
    echo "Configuration:"
    echo "  Edit $DAEMON_DIR/watcher.conf"
    echo ""
}

main