#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_FILE="${CONFIG_FILE:-${PROJECT_ROOT}/config/config.yaml}"
CONFIG_EXAMPLE="${PROJECT_ROOT}/config/config.example.yaml"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env}"
ENV_EXAMPLE="${PROJECT_ROOT}/.env.example"
VENV_DIR="${VENV_DIR:-${PROJECT_ROOT}/.venv}"
RUN_PYTHON="${VENV_DIR}/bin/python"
PROMPT_MODE="${PROMPT_MODE:-interactive}"
MOCK_INSTALL="${MOCK_INSTALL:-0}"
MOCK_STATE_DIR="${MOCK_STATE_DIR:-${PROJECT_ROOT}/mock-installation}"
MOCK_SKIP_PIP="${MOCK_SKIP_PIP:-0}"
LEGACY_INKYPI_REFRESH_COMMAND="sudo systemctl restart inkypi.service"
DEFAULT_INKYPI_UPDATE_METHOD="http_update_now"
DEFAULT_INKYPI_UPDATE_NOW_URL="http://127.0.0.1/update_now"
export CONFIG_FILE
export ENV_FILE
export PHOTO_FRAME_CONFIG="${PHOTO_FRAME_CONFIG:-${CONFIG_FILE}}"
export PHOTO_FRAME_ENV_FILE="${PHOTO_FRAME_ENV_FILE:-${ENV_FILE}}"
export PYTHONPATH="${PROJECT_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"


report_error() {
  local exit_code="$1"
  local line_no="$2"
  local command="$3"
  echo >&2
  echo >&2 "Installation error in ${0##*/} at line ${line_no}."
  echo >&2 "Failing command: ${command}"
  echo >&2 "Exit code: ${exit_code}"
  echo >&2 "If you rerun the script, existing values in ${CONFIG_FILE} and ${ENV_FILE} can be kept."
  exit "${exit_code}"
}


trap 'report_error $? $LINENO "$BASH_COMMAND"' ERR


run_cmd() {
  if [[ "${MOCK_INSTALL}" == "1" ]]; then
    echo "[mock] $*"
    return 0
  fi
  "$@"
}


run_privileged() {
  if [[ "${MOCK_INSTALL}" == "1" ]]; then
    echo "[mock sudo] $*"
    return 0
  fi
  sudo "$@"
}


systemd_unit_exists() {
  local unit_name="$1"
  if [[ "${MOCK_INSTALL}" == "1" ]]; then
    return 1
  fi
  if ! command -v systemctl >/dev/null 2>&1; then
    return 1
  fi
  systemctl list-unit-files --type=service --no-legend 2>/dev/null | awk '{print $1}' | grep -Fxq "${unit_name}"
}


detect_inkypi_repo_from_service() {
  local unit_name="${1:-inkypi.service}"
  if [[ "${MOCK_INSTALL}" == "1" ]]; then
    return 0
  fi
  if ! systemd_unit_exists "${unit_name}"; then
    return 0
  fi
  systemctl cat "${unit_name}" 2>/dev/null | python3 - <<'PY'
import os
import re
import sys

text = sys.stdin.read()

working_dir_match = re.search(r"^WorkingDirectory=(.+)$", text, re.MULTILINE)
if working_dir_match:
    working_dir = os.path.abspath(os.path.expanduser(working_dir_match.group(1).strip()))
    if working_dir.endswith("/src"):
        print(os.path.dirname(working_dir))
        raise SystemExit(0)
    if os.path.basename(working_dir) == "InkyPi" or os.path.exists(os.path.join(working_dir, "src", "inkypi.py")):
        print(working_dir)
        raise SystemExit(0)

exec_match = re.search(r"(/[^ \n\"']+/src/inkypi\.py)", text)
if exec_match:
    inkypi_py = os.path.abspath(os.path.expanduser(exec_match.group(1)))
    print(os.path.dirname(os.path.dirname(inkypi_py)))
PY
}


resolve_inkypi_layout_values() {
  local configured_repo_path="${1:-}"
  local configured_install_path="${2:-}"
  python3 - "${configured_repo_path}" "${configured_install_path}" <<'PY'
import sys

from app.inkypi_paths import resolve_inkypi_layout

repo_arg = sys.argv[1] if len(sys.argv) > 1 else ""
install_arg = sys.argv[2] if len(sys.argv) > 2 else ""
layout = resolve_inkypi_layout(repo_arg or None, install_arg or None)

print(layout.repo_path)
print(layout.install_path)
print(layout.source_root)
print(layout.device_config_path)
print(layout.git_sync_path or "")
print(layout.source_origin)
print("1" if layout.replaced_stale_repo_path else "0")
print("1" if layout.install_src_exists else "0")
PY
}


resolve_inkypi_update_values() {
  local configured_method="${1:-}"
  local configured_update_now_url="${2:-}"
  local configured_refresh_command="${3:-}"

  local update_method="${configured_method}"
  local update_now_url="${configured_update_now_url}"
  local refresh_command="${configured_refresh_command}"

  if [[ -z "${update_method}" ]]; then
    if [[ -z "${refresh_command}" || "${refresh_command}" == "${LEGACY_INKYPI_REFRESH_COMMAND}" ]]; then
      update_method="${DEFAULT_INKYPI_UPDATE_METHOD}"
    else
      update_method="command"
    fi
  fi

  if [[ -z "${update_now_url}" ]]; then
    update_now_url="${DEFAULT_INKYPI_UPDATE_NOW_URL}"
  fi

  if [[ -z "${refresh_command}" ]]; then
    refresh_command="${LEGACY_INKYPI_REFRESH_COMMAND}"
  fi

  printf '%s\n%s\n%s\n' "${update_method}" "${update_now_url}" "${refresh_command}"
}


ensure_systemd_service_active() {
  local unit_name="$1"
  if [[ "${MOCK_INSTALL}" == "1" ]]; then
    return 0
  fi
  if ! command -v systemctl >/dev/null 2>&1; then
    return 1
  fi
  if systemctl is-active --quiet "${unit_name}"; then
    return 0
  fi

  echo >&2 "systemd reports ${unit_name} is not active after restart."
  echo >&2 "Recent status output:"
  sudo systemctl status "${unit_name}" --no-pager || true
  echo >&2
  echo >&2 "Recent journal output:"
  sudo journalctl -u "${unit_name}" -n 50 --no-pager || true
  return 1
}


ensure_inkypi_service_reload_sudoers() {
  local service_user="$1"
  local systemctl_bin
  systemctl_bin="$(command -v systemctl || true)"
  if [[ -z "${systemctl_bin}" ]]; then
    echo >&2 "systemctl was not found; cannot install scoped sudoers access for inkypi.service."
    return 1
  fi

  if [[ "${MOCK_INSTALL}" == "1" ]]; then
    echo "[mock sudo] install scoped sudoers access for ${service_user} using ${systemctl_bin}"
    return 0
  fi

  local tmp_file
  tmp_file="$(mktemp)"
  cat >"${tmp_file}" <<EOF
# Managed by EInkProject installer
${service_user} ALL=(root) NOPASSWD: ${systemctl_bin} restart inkypi.service, ${systemctl_bin} is-active inkypi.service
EOF

  chmod 440 "${tmp_file}"
  run_privileged install -m 440 "${tmp_file}" /etc/sudoers.d/photo-frame-inkypi
  rm -f "${tmp_file}"

  local visudo_bin
  visudo_bin="$(command -v visudo || true)"
  if [[ -n "${visudo_bin}" ]]; then
    run_privileged "${visudo_bin}" -cf /etc/sudoers.d/photo-frame-inkypi >/dev/null
  fi
}


ensure_photo_frame_maintenance_sudoers() {
  local service_user="$1"
  local systemctl_bin journalctl_bin systemd_run_bin reboot_bin
  systemctl_bin="$(command -v systemctl || true)"
  journalctl_bin="$(command -v journalctl || true)"
  systemd_run_bin="$(command -v systemd-run || true)"
  reboot_bin="$(command -v reboot || true)"

  if [[ -z "${systemctl_bin}" || -z "${journalctl_bin}" || -z "${systemd_run_bin}" || -z "${reboot_bin}" ]]; then
    echo >&2 "Required maintenance commands were not found; cannot install photo-frame maintenance sudoers access."
    return 1
  fi

  if [[ "${MOCK_INSTALL}" == "1" ]]; then
    echo "[mock sudo] install maintenance sudoers access for ${service_user}"
    return 0
  fi

  local tmp_file
  tmp_file="$(mktemp)"
  cat >"${tmp_file}" <<EOF
# Managed by EInkProject installer
${service_user} ALL=(root) NOPASSWD: ${systemd_run_bin}, ${reboot_bin}, ${systemctl_bin} restart photo-frame.service, ${systemctl_bin} is-active photo-frame.service, ${systemctl_bin} status photo-frame.service --no-pager, ${journalctl_bin} -u photo-frame.service -n 50 --no-pager
EOF

  chmod 440 "${tmp_file}"
  run_privileged install -m 440 "${tmp_file}" /etc/sudoers.d/photo-frame-maintenance
  rm -f "${tmp_file}"

  local visudo_bin
  visudo_bin="$(command -v visudo || true)"
  if [[ -n "${visudo_bin}" ]]; then
    run_privileged "${visudo_bin}" -cf /etc/sudoers.d/photo-frame-maintenance >/dev/null
  fi
}


expand_path() {
  python3 - "$1" <<'PY'
import os
import sys

raw = sys.argv[1]
print(os.path.abspath(os.path.expandvars(os.path.expanduser(raw))))
PY
}


path_parent_is_writable() {
  local target
  target="$(expand_path "$1")"
  local parent
  parent="$(dirname "${target}")"
  [[ -w "${parent}" ]]
}


path_is_writable_or_creatable() {
  local target
  target="$(expand_path "$1")"
  if [[ -e "${target}" ]]; then
    [[ -w "${target}" ]]
    return
  fi
  path_parent_is_writable "${target}"
}


get_or_prompt_value() {
  local label="$1"
  local current="${2:-}"
  local default="${3:-}"
  local secret="${4:-0}"
  if [[ -n "${current}" ]]; then
    printf '%s' "${current}"
    return 0
  fi
  prompt_value "${label}" "" "${default}" "${secret}"
}


ensure_not_running_as_root() {
  if [[ "${MOCK_INSTALL}" == "1" ]]; then
    return 0
  fi
  if [[ "${EUID}" -eq 0 ]]; then
    echo >&2 "Do not run this script with sudo."
    echo >&2 "Run it as your normal user instead, for example:"
    echo >&2 "  bash scripts/install.sh"
    echo >&2 "The script already uses sudo internally for apt/systemd steps."
    exit 1
  fi
}


ensure_runtime_files() {
  if [[ ! -f "${CONFIG_FILE}" ]]; then
    cp "${CONFIG_EXAMPLE}" "${CONFIG_FILE}"
  fi
  if [[ ! -f "${ENV_FILE}" ]]; then
    if [[ -f "${ENV_EXAMPLE}" ]]; then
      cp "${ENV_EXAMPLE}" "${ENV_FILE}"
    else
      touch "${ENV_FILE}"
    fi
  fi
  chmod 600 "${ENV_FILE}" || true
}


ensure_venv() {
  if [[ ! -x "${RUN_PYTHON}" ]]; then
    python3 -m venv "${VENV_DIR}"
  fi
  if [[ ! -x "${RUN_PYTHON}" ]]; then
    echo >&2 "Virtualenv creation did not produce ${RUN_PYTHON}."
    echo >&2 "Please verify that python3-venv is installed and rerun the installer."
    return 1
  fi
  if [[ "${MOCK_INSTALL}" == "1" && -x "${RUN_PYTHON}" ]]; then
    if "${RUN_PYTHON}" - <<'PY' >/dev/null 2>&1
import dotenv
import PIL
import telegram
import yaml
PY
    then
      echo "[mock] Reusing existing virtualenv with required dependencies."
      return 0
    fi
  fi
  if [[ "${MOCK_INSTALL}" == "1" && "${MOCK_SKIP_PIP}" == "1" ]]; then
    echo "[mock] Skipping pip dependency installation in mock mode."
    RUN_PYTHON="python3"
    return 0
  fi
  if ! "${RUN_PYTHON}" -m pip --version >/dev/null 2>&1; then
    echo "pip is missing in ${VENV_DIR}; bootstrapping it with ensurepip."
    "${RUN_PYTHON}" -m ensurepip --upgrade
  fi
  if ! "${RUN_PYTHON}" -m pip --version >/dev/null 2>&1; then
    echo >&2 "Virtualenv is present but pip could not be initialized."
    echo >&2 "Install python3-venv/python3-full on the Pi and rerun scripts/install.sh."
    return 1
  fi
  "${RUN_PYTHON}" -m pip install --upgrade pip
  "${RUN_PYTHON}" -m pip install -r "${PROJECT_ROOT}/requirements.txt"
}


ask_yes_no() {
  local prompt="$1"
  local default="${2:-y}"
  local answer
  while true; do
    if [[ "${default}" == "y" ]]; then
      read -r -p "${prompt} [Y/n]: " answer
      answer="${answer:-Y}"
    else
      read -r -p "${prompt} [y/N]: " answer
      answer="${answer:-N}"
    fi

    case "${answer}" in
      Y|y) return 0 ;;
      N|n) return 1 ;;
      *) echo "Please answer y or n." ;;
    esac
  done
}


prompt_value() {
  local label="$1"
  local current="${2:-}"
  local default="${3:-}"
  local secret="${4:-0}"
  local value=""

  if [[ -n "${current}" ]]; then
    if [[ "${PROMPT_MODE}" == "missing-only" ]]; then
      printf '%s' "${current}"
      return 0
    fi
    if ask_yes_no "${label} is already set. Keep the existing value?" "y"; then
      printf '%s' "${current}"
      return 0
    fi
  fi

  while true; do
    if [[ -n "${default}" ]]; then
      if [[ "${secret}" == "1" ]]; then
        read -r -s -p "${label} [hidden, press Enter for default]: " value
        echo
      else
        read -r -p "${label} [${default}]: " value
      fi
      value="${value:-${default}}"
    else
      if [[ "${secret}" == "1" ]]; then
        read -r -s -p "${label}: " value
        echo
      else
        read -r -p "${label}: " value
      fi
    fi

    if [[ -n "${value}" ]]; then
      printf '%s' "${value}"
      return 0
    fi
    echo "A value is required."
  done
}


get_env_value() {
  local key="$1"
  python3 - "${ENV_FILE}" "${key}" <<'PY'
import sys
from pathlib import Path

env_path = Path(sys.argv[1])
key = sys.argv[2]
if not env_path.exists():
    raise SystemExit(0)
for line in env_path.read_text(encoding="utf-8").splitlines():
    if not line or line.lstrip().startswith("#") or "=" not in line:
        continue
    current_key, current_value = line.split("=", 1)
    if current_key == key:
        print(current_value)
        break
PY
}


set_env_value() {
  local key="$1"
  local value="$2"
  python3 - "${ENV_FILE}" "${key}" "${value}" <<'PY'
import sys
from pathlib import Path

env_path = Path(sys.argv[1])
key = sys.argv[2]
value = sys.argv[3]
lines = []
if env_path.exists():
    lines = env_path.read_text(encoding="utf-8").splitlines()

updated = False
for index, line in enumerate(lines):
    if line.startswith(f"{key}="):
        lines[index] = f"{key}={value}"
        updated = True
        break

if not updated:
    lines.append(f"{key}={value}")

env_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
PY
  chmod 600 "${ENV_FILE}" || true
}


get_yaml_value() {
  local path="$1"
  local value_type="${2:-string}"
  if [[ ! -x "${RUN_PYTHON}" ]]; then
    return 0
  fi
  "${RUN_PYTHON}" - "${CONFIG_FILE}" "${path}" "${value_type}" <<'PY'
import sys
from pathlib import Path

import yaml

config_path = Path(sys.argv[1])
path = sys.argv[2].split(".")
value_type = sys.argv[3]
if not config_path.exists():
    raise SystemExit(0)

data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
current = data
for part in path:
    if not isinstance(current, dict) or part not in current:
        raise SystemExit(0)
    current = current[part]

if value_type == "list-int" and isinstance(current, list):
    print(",".join(str(item) for item in current))
elif value_type == "bool":
    print("true" if current else "false")
else:
    print(current)
PY
}


set_yaml_value() {
  local path="$1"
  local value_type="$2"
  local raw_value="$3"
  "${RUN_PYTHON}" - "${CONFIG_FILE}" "${path}" "${value_type}" "${raw_value}" <<'PY'
import json
import sys
from pathlib import Path

import yaml

config_path = Path(sys.argv[1])
path = sys.argv[2].split(".")
value_type = sys.argv[3]
raw_value = sys.argv[4]

data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
current = data
for part in path[:-1]:
    current = current.setdefault(part, {})

if value_type == "int":
    value = int(raw_value)
elif value_type == "bool":
    value = raw_value.lower() in {"1", "true", "yes", "y"}
elif value_type == "list-int":
    value = [int(item.strip()) for item in raw_value.split(",") if item.strip()]
elif value_type == "json":
    value = json.loads(raw_value)
else:
    value = raw_value

current[path[-1]] = value
config_path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
PY
}


normalize_id_list() {
  python3 - "$1" <<'PY'
import sys

raw = sys.argv[1]
items = []
for part in raw.split(","):
    part = part.strip()
    if not part:
        continue
    try:
        items.append(str(int(part)))
    except ValueError as exc:
        raise SystemExit(f"Invalid Telegram user ID: {part}") from exc
print(",".join(items))
PY
}


initialize_database() {
  "${RUN_PYTHON}" - <<'PY'
from app.config import load_config
from app.database import Database
from app.storage import StorageService

config = load_config()
storage = StorageService(config.storage)
storage.ensure_directories()
database = Database(config.database.path)
database.initialize()
database.seed_admins(config.security.admin_user_ids)
database.seed_whitelist(config.security.whitelisted_user_ids)
print(f"Initialized database at {config.database.path}")
PY
}


ensure_service_unit() {
  local service_user="$1"
  local install_dir="$2"
  local target="/etc/systemd/system/photo-frame.service"
  local rendered
  rendered="$("${RUN_PYTHON}" - "${PROJECT_ROOT}" "${VENV_DIR}" "${CONFIG_FILE}" "${service_user}" "${install_dir}" <<'PY'
import shlex
import sys
from pathlib import Path

project_root = Path(sys.argv[1])
venv_dir = Path(sys.argv[2])
config_path = Path(sys.argv[3])
service_user = sys.argv[4]
install_dir = Path(sys.argv[5])
python_bin = venv_dir / "bin" / "python"
command = "cd {cwd} && exec {python_bin} -m app.main --config {config_path}".format(
    cwd=shlex.quote(str(project_root)),
    python_bin=shlex.quote(str(python_bin)),
    config_path=shlex.quote(str(config_path)),
)
print(f"""[Unit]
Description=Telegram to InkyPi Photo Frame
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={service_user}
WorkingDirectory={install_dir}
Environment=PYTHONUNBUFFERED=1
ExecStart=/bin/bash -lc {shlex.quote(command)}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
""")
PY
)"
  if [[ "${MOCK_INSTALL}" == "1" ]]; then
    mkdir -p "${MOCK_STATE_DIR}/systemd"
    target="${MOCK_STATE_DIR}/systemd/photo-frame.service"
    printf '%s\n' "${rendered}" > "${target}"
    echo "[mock] Wrote systemd unit to ${target}"
    return 0
  fi
  printf '%s\n' "${rendered}" | sudo tee "${target}" >/dev/null
  run_privileged systemctl daemon-reload
  run_privileged systemctl enable photo-frame.service
}
