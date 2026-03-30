#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_STATE_DIR="${PROJECT_ROOT}/telegram-bot-test"
TEST_CONFIG_FILE="${TEST_STATE_DIR}/config.yaml"
TEST_ENV_FILE="${TEST_STATE_DIR}/.env"
TEST_DATA_DIR="${TEST_STATE_DIR}/data"
TEST_INKYPI_DIR="${TEST_STATE_DIR}/InkyPi"
TEST_RUNTIME_DIR="${TEST_STATE_DIR}/usr/local/inkypi"
TEST_VENV_DIR="${TEST_STATE_DIR}/.venv"

mkdir -p \
  "${TEST_STATE_DIR}" \
  "${TEST_DATA_DIR}/incoming" \
  "${TEST_DATA_DIR}/rendered" \
  "${TEST_DATA_DIR}/cache" \
  "${TEST_DATA_DIR}/archive" \
  "${TEST_DATA_DIR}/db" \
  "${TEST_DATA_DIR}/inkypi" \
  "${TEST_INKYPI_DIR}/src/plugins" \
  "${TEST_INKYPI_DIR}/src/config" \
  "${TEST_RUNTIME_DIR}"

ln -snf "${TEST_INKYPI_DIR}/src" "${TEST_RUNTIME_DIR}/src"

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
  cache_dir: ${TEST_DATA_DIR}/cache
  archive_dir: ${TEST_DATA_DIR}/archive
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
  max_caption_lines: 2
  font_path: /usr/share/fonts/truetype/dejavu/DejaVuSans.ttf
  background_color: "#F7F3EA"
  text_color: "#111111"
  divider_color: "#3A3A3A"

inkypi:
  repo_path: ${TEST_INKYPI_DIR}
  install_path: ${TEST_RUNTIME_DIR}
  validated_commit: test-mode
  waveshare_model: epd7in3e
  plugin_id: telegram_frame
  payload_dir: ${TEST_DATA_DIR}/inkypi
  update_method: command
  update_now_url: http://127.0.0.1/update_now
  refresh_command: echo telegram-test-refresh
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
- /cancel

You can also send a photo and walk through the full metadata flow.
Display refresh is mocked with: echo telegram-test-refresh

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
