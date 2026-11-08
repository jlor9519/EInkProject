#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

main() {
  local quick_mode=0
  local clear_images=0
  local arg
  for arg in "$@"; do
    if [[ "${arg}" == "--quick" ]]; then
      quick_mode=1
    elif [[ "${arg}" == "--clear-images" ]]; then
      clear_images=1
    fi
  done

  ensure_not_running_as_root
  ensure_runtime_files

  if [[ -z "$(get_env_value TELEGRAM_BOT_TOKEN)" ]]; then
    local telegram_token
    telegram_token="$(prompt_value "Telegram bot token" "" "" 1)"
    set_env_value TELEGRAM_BOT_TOKEN "${telegram_token}"
  fi

  if git -C "${PROJECT_ROOT}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    local old_head new_head
    old_head="$(git -C "${PROJECT_ROOT}" rev-parse HEAD 2>/dev/null || echo "")"
    run_cmd git -C "${PROJECT_ROOT}" pull --ff-only || true
    new_head="$(git -C "${PROJECT_ROOT}" rev-parse HEAD 2>/dev/null || echo "")"
    if [[ -n "${old_head}" && "${old_head}" != "${new_head}" ]]; then
      echo "Updated from ${old_head:0:7} to ${new_head:0:7}:"
      git -C "${PROJECT_ROOT}" log --oneline "${old_head}..${new_head}"
      # The pull may have changed this script; run the new version for the remaining steps.
      exec bash "${PROJECT_ROOT}/scripts/update.sh" "$@"
    fi
    echo "Already up to date ($(git -C "${PROJECT_ROOT}" rev-parse --short HEAD 2>/dev/null || echo 'unknown'))."
  fi

  if [[ "${quick_mode}" == "0" ]]; then
    install_system_packages
  else
    echo "Quick mode: skipping apt packages."
  fi
  ensure_venv

  if [[ -z "$(get_yaml_value security.admin_user_ids list-int)" ]]; then
    local admin_ids
    admin_ids="$(prompt_value "Initial admin Telegram user IDs (comma-separated)" "" "" 0)"
    admin_ids="$(normalize_id_list "${admin_ids}")"
    set_yaml_value security.admin_user_ids list-int "${admin_ids}"
  fi

  local service_user="${SUDO_USER:-$(id -un)}"
  PANEL_REBOOT_NEEDED=0
  ensure_panel_ready "${service_user}"
  retire_inkypi_service

  initialize_database

  if [[ "${clear_images}" == "1" ]]; then
    echo "Clearing all stored image data without pushing a new display update."
    "${RUN_PYTHON}" -m app.library_cleanup --config "${CONFIG_FILE}"
  fi

  if [[ "${PANEL_REBOOT_NEEDED}" == "1" ]]; then
    echo
    echo "SPI settings changed. Reboot the Pi now (sudo reboot); the photo frame starts by itself afterwards."
    return 0
  fi

  if systemd_unit_exists 'photo-frame.service'; then
    echo "Restarting photo-frame.service."
    run_privileged systemctl restart photo-frame.service
    ensure_systemd_service_active photo-frame.service
  fi

  echo "Update completed."
}

# Everything runs from main, so bash has read the whole script before it starts. That matters
# because the git pull above can replace this file while it runs.
main "$@"
