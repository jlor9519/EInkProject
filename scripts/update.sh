#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

quick_mode=0
clear_images=0
for arg in "$@"; do
  if [[ "${arg}" == "--quick" ]]; then
    quick_mode=1
  elif [[ "${arg}" == "--clear-images" ]]; then
    clear_images=1
  fi
done

ensure_not_running_as_root
ensure_runtime_files
ensure_venv

if [[ -z "$(get_env_value TELEGRAM_BOT_TOKEN)" ]]; then
  telegram_token="$(prompt_value "Telegram bot token" "" "" 1)"
  set_env_value TELEGRAM_BOT_TOKEN "${telegram_token}"
fi

if [[ -z "$(get_yaml_value security.admin_user_ids list-int)" ]]; then
  admin_ids="$(prompt_value "Initial admin Telegram user IDs (comma-separated)" "" "" 0)"
  admin_ids="$(normalize_id_list "${admin_ids}")"
  set_yaml_value security.admin_user_ids list-int "${admin_ids}"
fi

old_head=""
if git -C "${PROJECT_ROOT}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  old_head="$(git -C "${PROJECT_ROOT}" rev-parse HEAD 2>/dev/null || echo "")"
  run_cmd git -C "${PROJECT_ROOT}" pull --ff-only || true
  new_head="$(git -C "${PROJECT_ROOT}" rev-parse HEAD 2>/dev/null || echo "")"
  if [[ -n "${old_head}" && "${old_head}" != "${new_head}" ]]; then
    echo "Updated from ${old_head:0:7} to ${new_head:0:7}:"
    git -C "${PROJECT_ROOT}" log --oneline "${old_head}..${new_head}"
  else
    echo "Already up to date ($(git -C "${PROJECT_ROOT}" rev-parse --short HEAD 2>/dev/null || echo 'unknown'))."
  fi
fi

service_user="${SUDO_USER:-$(id -un)}"
ensure_photo_frame_maintenance_sudoers "${service_user}"

inkypi_update_method="$(get_yaml_value inkypi.update_method string)"
inkypi_update_now_url="$(get_yaml_value inkypi.update_now_url string)"
refresh_command="$(get_yaml_value inkypi.refresh_command string)"
inkypi_update_values=()
while IFS= read -r line; do
  inkypi_update_values+=("${line}")
done < <(resolve_inkypi_update_values "${inkypi_update_method}" "${inkypi_update_now_url}" "${refresh_command}")
set_yaml_value inkypi.update_method string "${inkypi_update_values[0]}"
set_yaml_value inkypi.update_now_url string "${inkypi_update_values[1]}"
set_yaml_value inkypi.refresh_command string "${inkypi_update_values[2]}"

if [[ "${quick_mode}" == "0" ]]; then
  PROMPT_MODE=missing-only bash "${PROJECT_ROOT}/scripts/setup_inkypi.sh"
else
  echo "Quick mode: skipping InkyPi plugin sync."
fi

initialize_database

if [[ "${clear_images}" == "1" ]]; then
  echo "Clearing stored image library while preserving the current display payload."
  "${RUN_PYTHON}" -m app.library_cleanup --config "${CONFIG_FILE}"
fi

if systemd_unit_exists 'photo-frame.service'; then
  echo "Restarting photo-frame.service."
  run_privileged systemctl restart photo-frame.service
  ensure_systemd_service_active photo-frame.service
fi

echo "Update completed."
