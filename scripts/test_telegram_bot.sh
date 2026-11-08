#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_STATE_DIR="${PROJECT_ROOT}/telegram-bot-test"
TEST_CONFIG_FILE="${TEST_STATE_DIR}/config.yaml"
TEST_ENV_FILE="${TEST_STATE_DIR}/.env"
TEST_DATA_DIR="${TEST_STATE_DIR}/data"
TEST_VENV_DIR="${TEST_STATE_DIR}/.venv"

mkdir -p \
  "${TEST_STATE_DIR}" \
  "${TEST_DATA_DIR}/incoming" \
  "${TEST_DATA_DIR}/rendered" \
  "${TEST_DATA_DIR}/db" \
  "${TEST_DATA_DIR}/inkypi"

cat > "${TEST_CONFIG_FILE}" <<EOF
telegram:
  bot_token_env: TELEGRAM_BOT_TOKEN

security:
  admin_user_ids: []
  whitelisted_user_ids: []

database:
  path: ${TEST_DATA_DIR}/db/photo_frame.db

storage:
  incoming_dir: ${TEST_DATA_DIR}/incoming
  rendered_dir: ${TEST_DATA_DIR}/rendered
  inkypi_payload_dir: ${TEST_DATA_DIR}/inkypi
  current_payload_path: ${TEST_DATA_DIR}/inkypi/current.json
  current_image_path: ${TEST_DATA_DIR}/inkypi/current.png
  keep_recent_rendered: 20

display:
  width: 800
  height: 480
  caption_height: 132
  margin: 18
  metadata_font_size: 22
  caption_font_size: 28
  font_path: /usr/share/fonts/truetype/dejavu/DejaVuSans.ttf
  text_color: "#111111"
  panel: file
  panel_output_path: ${TEST_DATA_DIR}/panel.png

EOF

cat > "${TEST_ENV_FILE}" <<EOF
TELEGRAM_BOT_TOKEN=
EOF

export CONFIG_FILE="${TEST_CONFIG_FILE}"
export ENV_FILE="${TEST_ENV_FILE}"
export VENV_DIR="${TEST_VENV_DIR}"
export PHOTO_FRAME_CONFIG="${TEST_CONFIG_FILE}"
export PHOTO_FRAME_ENV_FILE="${TEST_ENV_FILE}"

source "${PROJECT_ROOT}/scripts/common.sh"

ensure_runtime_files
ensure_venv

telegram_token="$(prompt_value "Telegram bot token for foreground test bot" "$(get_env_value TELEGRAM_BOT_TOKEN)" "" 0)"
admin_ids="$(prompt_value "Admin Telegram user IDs for test mode (comma-separated)" "$(get_yaml_value security.admin_user_ids list-int)" "" 0)"
admin_ids="$(normalize_id_list "${admin_ids}")"
whitelist_ids="$(prompt_value "Extra whitelisted Telegram user IDs for test mode (comma-separated, optional)" "$(get_yaml_value security.whitelisted_user_ids list-int)" "${admin_ids}" 0)"
whitelist_ids="$(normalize_id_list "${whitelist_ids}")"

set_env_value TELEGRAM_BOT_TOKEN "${telegram_token}"
set_yaml_value security.admin_user_ids list-int "${admin_ids}"
set_yaml_value security.whitelisted_user_ids list-int "${whitelist_ids}"

initialize_database

cat <<EOF

Starting Telegram bot in foreground test mode.

Useful commands to try in Telegram:
- /myid
- /help
- /status
- /whitelist <user_id>

You can also send a photo and walk through the full metadata flow.
The panel is simulated: each refresh writes ${TEST_DATA_DIR}/panel.png
(rotated and colour-tuned exactly as the real panel would receive it).

Stop the bot with Ctrl-C or by closing this terminal.
EOF

BOT_PID=""

cleanup() {
  local exit_code=$?
  if [[ -n "${BOT_PID}" ]] && kill -0 "${BOT_PID}" >/dev/null 2>&1; then
    echo
    echo "Stopping foreground test bot..."
    kill "${BOT_PID}" >/dev/null 2>&1 || true
    wait "${BOT_PID}" >/dev/null 2>&1 || true
  fi
  exit "${exit_code}"
}

trap cleanup EXIT INT TERM

"${RUN_PYTHON}" -m app.main --config "${TEST_CONFIG_FILE}" --log-level INFO &
BOT_PID=$!
wait "${BOT_PID}"
