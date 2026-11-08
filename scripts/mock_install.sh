#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MOCK_STATE_DIR="${PROJECT_ROOT}/mock-installation"
MOCK_CONFIG_FILE="${MOCK_STATE_DIR}/config.yaml"
MOCK_ENV_FILE="${MOCK_STATE_DIR}/.env"
MOCK_DATA_DIR="${MOCK_STATE_DIR}/data"
MOCK_LOG_DIR="${MOCK_STATE_DIR}/logs"

mkdir -p \
  "${MOCK_STATE_DIR}" \
  "${MOCK_DATA_DIR}/incoming" \
  "${MOCK_DATA_DIR}/rendered" \
  "${MOCK_DATA_DIR}/db" \
  "${MOCK_DATA_DIR}/inkypi" \
  "${MOCK_LOG_DIR}"
chmod 700 "${MOCK_STATE_DIR}" || true

cat > "${MOCK_CONFIG_FILE}" <<EOF
telegram:
  bot_token_env: TELEGRAM_BOT_TOKEN

security:
  admin_user_ids: []
  whitelisted_user_ids: []

database:
  path: ${MOCK_DATA_DIR}/db/photo_frame.db

storage:
  incoming_dir: ${MOCK_DATA_DIR}/incoming
  rendered_dir: ${MOCK_DATA_DIR}/rendered
  inkypi_payload_dir: ${MOCK_DATA_DIR}/inkypi
  current_payload_path: ${MOCK_DATA_DIR}/inkypi/current.json
  current_image_path: ${MOCK_DATA_DIR}/inkypi/current.png
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
  panel_output_path: ${MOCK_DATA_DIR}/panel.png

EOF

cat > "${MOCK_ENV_FILE}" <<EOF
TELEGRAM_BOT_TOKEN=
DROPBOX_ACCESS_TOKEN=
EOF

cat <<EOF
Starting mock installation mode.

Nothing will be installed system-wide.
Mock state will be written under:
  ${MOCK_STATE_DIR}

This flow is useful for:
- checking prompt wording
- validating interactive inputs
- verifying config/env files are written as expected
- checking the panel setup steps without touching SPI or GPIO
EOF

CONFIG_FILE="${MOCK_CONFIG_FILE}" \
ENV_FILE="${MOCK_ENV_FILE}" \
VENV_DIR="${MOCK_STATE_DIR}/.venv" \
MOCK_INSTALL=1 \
MOCK_STATE_DIR="${MOCK_STATE_DIR}" \
PROMPT_MODE=interactive \
bash "${PROJECT_ROOT}/scripts/install.sh"

echo
echo "Mock installation completed."
echo "Mock config: ${MOCK_CONFIG_FILE}"
echo "Mock env:    ${MOCK_ENV_FILE}"
echo "Mock panel:  ${MOCK_DATA_DIR}/panel.png"
