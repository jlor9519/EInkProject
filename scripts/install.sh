#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

main() {
  ensure_not_running_as_root
  ensure_runtime_files

  if ask_yes_no "Install/update the apt packages for Python, the fonts and the e-paper panel?" "y"; then
    install_system_packages
  fi

  ensure_venv

  local telegram_token admin_ids whitelist_ids
  telegram_token="$(get_or_prompt_value "Telegram bot token" "$(get_env_value TELEGRAM_BOT_TOKEN)" "" 0)"
  admin_ids="$(get_or_prompt_value "Initial admin Telegram user IDs (comma-separated)" "$(get_yaml_value security.admin_user_ids list-int)" "" 0)"
  admin_ids="$(normalize_id_list "${admin_ids}")"
  whitelist_ids="$(get_yaml_value security.whitelisted_user_ids list-int)"
  if [[ -z "${whitelist_ids}" ]]; then
    whitelist_ids="${admin_ids}"
  fi
  whitelist_ids="$(normalize_id_list "${whitelist_ids}")"

  set_env_value TELEGRAM_BOT_TOKEN "${telegram_token}"
  set_yaml_value security.admin_user_ids list-int "${admin_ids}"
  set_yaml_value security.whitelisted_user_ids list-int "${whitelist_ids}"
  set_yaml_value display.waveshare_model string "$(waveshare_model)"

  local service_user="${SUDO_USER:-$(id -un)}"
  PANEL_REBOOT_NEEDED=0
  ensure_panel_ready "${service_user}"
  retire_inkypi_service

  initialize_database

  echo "Installing/updating photo-frame systemd service for user ${service_user}."
  ensure_service_unit "${service_user}" "${PROJECT_ROOT}"
  if [[ "${PANEL_REBOOT_NEEDED}" == "1" ]]; then
    echo
    echo "SPI was just turned on. Reboot the Pi now (sudo reboot); the photo frame starts by itself afterwards."
    return 0
  fi
  run_privileged systemctl restart photo-frame.service
  ensure_systemd_service_active photo-frame.service

  echo "Install flow completed."
}

# Everything runs from main, so bash has read the whole script before it starts.
main "$@"
