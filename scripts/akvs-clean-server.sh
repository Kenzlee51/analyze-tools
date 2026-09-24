#!/usr/bin/bash
# =============================================================================
# akvs-clean-server.sh — удалённая очистка сервера АК-ВС 3
# =============================================================================
#
# ОПИСАНИЕ:
#   Подключается по SSH к серверу АК-ВС и выполняет очистку:
#     1) удаляет все проекты из БД MongoDB (db.projects.deleteMany({}));
#     2) удаляет папки проектов с диска (PROJECTS_DIR/*);
#     3) сбрасывает счётчик падений и перезапускает сервис.
#   Нужно, когда сервер «завис» в цикле падений из-за превышения лимита
#   проектов, либо чтобы освободить слот от накопившихся хвостов.
#
#   Вход по паролю (SSH и sudo). Пароли спрашиваются один раз, без эха,
#   и НЕ сохраняются на диск. SSH-пароль подставляется через sshpass,
#   sudo-пароль передаётся команде через `sudo -S`.
#
# ИСПОЛЬЗОВАНИЕ:
#   ./akvs-clean-server.sh            # показать состояние и спросить подтверждение
#   ./akvs-clean-server.sh --yes      # без подтверждения (для автоматизации)
#   ./akvs-clean-server.sh --status   # только показать состояние, ничего не менять
#   ./akvs-clean-server.sh -h         # справка
#
# ЗАВИСИМОСТИ: sshpass (на этой машине). На сервере: mongo, systemctl, sudo.
#
# ВНИМАНИЕ: скрипт делает `rm -rf` под sudo на сервере и перезапускает сервис.
#           Это УБЬЁТ идущий на сервере анализ, если он выполняется.
# =============================================================================

set -uo pipefail

# ---------------- НАСТРОЙКИ СЕРВЕРА (поменять при необходимости) -------------
SSH_USER="user"
SSH_HOST="192.168.25.173"
SSH_PORT="22"
MONGO_DB="akvs3"
PROJECTS_DIR="/opt/akvs_server/data/projects"
SERVICE="akvs_server.service"
# ---------------------------------------------------------------------------

ASSUME_YES=0
STATUS_ONLY=0

for arg in "$@"; do
    case "$arg" in
        --yes|-y)    ASSUME_YES=1 ;;
        --status|-s) STATUS_ONLY=1 ;;
        -h|--help)
            sed -n '2,40p' "$0"
            exit 0 ;;
        *)
            echo "Неизвестный аргумент: $arg (см. -h)"; exit 2 ;;
    esac
done

# Безопасность: PROJECTS_DIR не должен быть корнем/пустым
case "$PROJECTS_DIR" in
    ""|"/"|"/opt"|"/opt/"|"/opt/akvs_server"|"/opt/akvs_server/")
        echo "[ОШИБКА] PROJECTS_DIR выглядит небезопасно ('$PROJECTS_DIR') — прерываю."
        exit 1 ;;
esac

command -v sshpass >/dev/null 2>&1 || { echo "[ОШИБКА] sshpass не установлен."; exit 1; }

echo "=============================================================="
echo " Очистка сервера АК-ВС"
echo " Цель:      ${SSH_USER}@${SSH_HOST}:${SSH_PORT}"
echo " БД:        ${MONGO_DB}"
echo " Папки:     ${PROJECTS_DIR}"
echo " Сервис:    ${SERVICE}"
echo "=============================================================="

# --- Пароли (без эха, не сохраняются) ---
read -r -s -p "SSH-пароль для ${SSH_USER}@${SSH_HOST}: " SSH_PASS; echo
if [ "$STATUS_ONLY" -eq 0 ]; then
    read -r -s -p "sudo-пароль на сервере: " SUDO_PASS; echo
fi

# Обёртка запуска команды на сервере (SSH-пароль через sshpass)
run_ssh() {
    sshpass -p "$SSH_PASS" ssh -p "$SSH_PORT" \
        -o StrictHostKeyChecking=accept-new \
        -o ConnectTimeout=15 \
        "${SSH_USER}@${SSH_HOST}" "$1"
}

echo
echo "[1/1] Проверка подключения и состояния сервера..."
HOSTNAME_REMOTE="$(run_ssh 'hostname' 2>/dev/null)"
if [ -z "$HOSTNAME_REMOTE" ]; then
    echo "[ОШИБКА] Не удалось подключиться по SSH (проверьте пароль/доступность сервера)."
    exit 1
fi
echo "  Подключено к: $HOSTNAME_REMOTE"

# Состояние: проекты в БД и папки на диске
echo
echo "  Проекты в БД ($MONGO_DB):"
run_ssh "mongo ${MONGO_DB} --quiet --eval 'db.projects.find({},{name:1,_id:0}).forEach(function(d){print(\"    - \"+d.name)})'" \
    || echo "    (не удалось получить список из БД)"

echo "  Папки проектов на диске (${PROJECTS_DIR}):"
run_ssh "ls -1 '${PROJECTS_DIR}' 2>/dev/null | sed 's/^/    - /'" \
    || echo "    (папка недоступна или пуста)"

echo
echo "  Статус сервиса:"
run_ssh "systemctl is-active ${SERVICE}; systemctl is-failed ${SERVICE} 2>/dev/null" \
    | sed 's/^/    /'

if [ "$STATUS_ONLY" -eq 1 ]; then
    echo
    echo "Режим --status: ничего не меняю."
    exit 0
fi

# --- Подтверждение ---
if [ "$ASSUME_YES" -eq 0 ]; then
    echo
    echo "БУДЕТ ВЫПОЛНЕНО на ${HOSTNAME_REMOTE}:"
    echo "  1) mongo ${MONGO_DB}: db.projects.deleteMany({})"
    echo "  2) sudo rm -rf ${PROJECTS_DIR}/*"
    echo "  3) sudo systemctl reset-failed ${SERVICE} && sudo systemctl restart ${SERVICE}"
    echo "ВНИМАНИЕ: это удалит все проекты и перезапустит сервис (убьёт идущий анализ)."
    read -r -p "Продолжить? [y/N] " ans
    case "$ans" in
        y|Y|yes|YES|да|ДА) : ;;
        *) echo "Отменено."; exit 0 ;;
    esac
fi

echo
echo "== Очистка =="

# Выполняем все шаги в ОДНОЙ ssh-сессии. sudo-пароль отдаём через `sudo -S`.
# Разрушительные команды жёстко привязаны к MONGO_DB и PROJECTS_DIR.
CLEAN_SCRIPT=$(cat <<REMOTE
set -e
echo "[remote] Очистка БД ${MONGO_DB}.projects ..."
mongo ${MONGO_DB} --quiet --eval 'printjson(db.projects.deleteMany({}))'

echo "[remote] Удаление папок проектов из ${PROJECTS_DIR} ..."
if [ -d "${PROJECTS_DIR}" ]; then
    echo "${SUDO_PASS}" | sudo -S -p "" rm -rf "${PROJECTS_DIR}"/* || { echo "[remote] ОШИБКА rm (sudo?)"; exit 3; }
    echo "[remote] Готово. Осталось в папке:"
    ls -1 "${PROJECTS_DIR}" 2>/dev/null | sed 's/^/    - /' || echo "    (пусто)"
else
    echo "[remote] Папка ${PROJECTS_DIR} не найдена — пропускаю."
fi

echo "[remote] Перезапуск сервиса ${SERVICE} ..."
echo "${SUDO_PASS}" | sudo -S -p "" systemctl reset-failed ${SERVICE} || true
echo "${SUDO_PASS}" | sudo -S -p "" systemctl restart ${SERVICE}
REMOTE
)

run_ssh "$CLEAN_SCRIPT"
rc=$?

if [ $rc -ne 0 ]; then
    echo
    echo "[ОШИБКА] Очистка завершилась с кодом $rc. Проверьте вывод выше (sudo-пароль? права?)."
    exit $rc
fi

echo
echo "== Проверка после очистки (даём сервису ~20с на старт) =="
sleep 20
echo "  Проекты в БД:"
run_ssh "mongo ${MONGO_DB} --quiet --eval 'print(\"    всего: \"+db.projects.find().count())'" \
    || echo "    (не удалось проверить)"
echo "  Папки на диске:"
run_ssh "ls -1 '${PROJECTS_DIR}' 2>/dev/null | sed 's/^/    - /' || echo '    (пусто)'"
echo "  Статус сервиса:"
run_ssh "systemctl is-active ${SERVICE}" | sed 's/^/    /'

echo
echo "Готово."
