#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MOCK_STATE_DIR="${PROJECT_ROOT}/mock-installation"
MOCK_CONFIG_FILE="${MOCK_STATE_DIR}/config.yaml"
MOCK_ENV_FILE="${MOCK_STATE_DIR}/.env"
MOCK_INKYPI_DIR="${MOCK_STATE_DIR}/InkyPi"
MOCK_RUNTIME_DIR="${MOCK_STATE_DIR}/usr/local/inkypi"
MOCK_DATA_DIR="${MOCK_STATE_DIR}/data"
MOCK_LOG_DIR="${MOCK_STATE_DIR}/logs"

mkdir -p \
  "${MOCK_STATE_DIR}" \
  "${MOCK_INKYPI_DIR}/src/plugins" \
  "${MOCK_INKYPI_DIR}/src/plugins/base_plugin" \
  "${MOCK_INKYPI_DIR}/src/config" \
  "${MOCK_INKYPI_DIR}/install" \
  "${MOCK_INKYPI_DIR}/.git" \
  "${MOCK_RUNTIME_DIR}" \
  "${MOCK_DATA_DIR}/incoming" \
  "${MOCK_DATA_DIR}/rendered" \
  "${MOCK_DATA_DIR}/cache" \
  "${MOCK_DATA_DIR}/archive" \
  "${MOCK_DATA_DIR}/db" \
  "${MOCK_DATA_DIR}/inkypi" \
  "${MOCK_LOG_DIR}"
chmod 700 "${MOCK_STATE_DIR}" || true

ln -snf "${MOCK_INKYPI_DIR}/src" "${MOCK_RUNTIME_DIR}/src"

if [[ ! -f "${MOCK_INKYPI_DIR}/install/install.sh" ]]; then
  printf '#!/usr/bin/env bash\necho "Mock InkyPi install called with: $*"\n' > "${MOCK_INKYPI_DIR}/install/install.sh"
  chmod +x "${MOCK_INKYPI_DIR}/install/install.sh"
fi

if [[ ! -f "${MOCK_INKYPI_DIR}/src/plugins/__init__.py" ]]; then
  printf '' > "${MOCK_INKYPI_DIR}/src/plugins/__init__.py"
fi

if [[ ! -f "${MOCK_INKYPI_DIR}/src/plugins/base_plugin/__init__.py" ]]; then
  printf '' > "${MOCK_INKYPI_DIR}/src/plugins/base_plugin/__init__.py"
fi

if [[ ! -f "${MOCK_INKYPI_DIR}/src/plugins/base_plugin/base_plugin.py" ]]; then
  cat > "${MOCK_INKYPI_DIR}/src/plugins/base_plugin/base_plugin.py" <<'EOF'
class BasePlugin:
    def __init__(self, config=None, **dependencies):
        self.config = config or {}
EOF
fi

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
  cache_dir: ${MOCK_DATA_DIR}/cache
  archive_dir: ${MOCK_DATA_DIR}/archive
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
  max_caption_lines: 2
  font_path: /usr/share/fonts/truetype/dejavu/DejaVuSans.ttf
  background_color: "#F7F3EA"
  text_color: "#111111"
  divider_color: "#3A3A3A"

inkypi:
  repo_path: ${MOCK_INKYPI_DIR}
  install_path: ${MOCK_RUNTIME_DIR}
  validated_commit: main
  waveshare_model: epd7in3e
  plugin_id: telegram_frame
  payload_dir: ${MOCK_DATA_DIR}/inkypi
  update_method: command
  update_now_url: http://127.0.0.1/update_now
  refresh_command: echo mock-inkypi-refresh
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
- exercising plugin injection into a fake InkyPi checkout
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
echo "Mock InkyPi: ${MOCK_INKYPI_DIR}"
