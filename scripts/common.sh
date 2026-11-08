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
DEFAULT_WAVESHARE_MODEL="epd7in3e"
WAVESHARE_DRIVER_BASE_URL="https://raw.githubusercontent.com/waveshareteam/e-Paper/master/RaspberryPi_JetsonNano/python/lib/waveshare_epd"
# python3-pil/-yaml come from apt so pip does not have to compile them on a Pi Zero; spidev,
# gpiozero and lgpio are what the Waveshare driver needs. The venv sees them via
# --system-site-packages.
SYSTEM_PACKAGES=(
  python3 python3-venv python3-pip git curl fonts-dejavu-core
  python3-pil python3-yaml python3-spidev python3-gpiozero python3-lgpio
)
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
    if [[ "${MOCK_INSTALL}" == "1" ]]; then
      python3 -m venv "${VENV_DIR}"
    else
      python3 -m venv --system-site-packages "${VENV_DIR}"
    fi
  fi
  if [[ "${MOCK_INSTALL}" != "1" ]]; then
    ensure_venv_sees_system_packages
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


# A venv created before InkyPi was removed cannot see the apt packages (spidev, gpiozero, lgpio).
# Switching the flag in pyvenv.cfg is what `python3 -m venv --system-site-packages` does, without
# reinstalling everything.
ensure_venv_sees_system_packages() {
  local cfg="${VENV_DIR}/pyvenv.cfg"
  if [[ -f "${cfg}" ]] && grep -q '^include-system-site-packages = false' "${cfg}"; then
    echo "Letting ${VENV_DIR} use the system Python packages for the display."
    sed -i.bak 's/^include-system-site-packages = false/include-system-site-packages = true/' "${cfg}"
    rm -f "${cfg}.bak"
  fi
}


install_system_packages() {
  run_privileged apt-get update
  run_privileged apt-get install -y "${SYSTEM_PACKAGES[@]}"
}


waveshare_model() {
  local model
  model="$(get_yaml_value display.waveshare_model string)"
  if [[ -z "${model}" ]]; then
    model="$(get_yaml_value inkypi.waveshare_model string)"
  fi
  printf '%s' "${model:-${DEFAULT_WAVESHARE_MODEL}}"
}


waveshare_driver_dir() {
  local driver_dir
  driver_dir="$(get_yaml_value display.driver_dir string)"
  driver_dir="${driver_dir:-vendor/waveshare_epd}"
  if [[ "${driver_dir}" != /* ]]; then
    driver_dir="${PROJECT_ROOT}/${driver_dir}"
  fi
  printf '%s' "${driver_dir}"
}


# Download the Waveshare driver for the configured panel, unless it is already there.
download_waveshare_drivers() {
  local model driver_dir
  model="$(waveshare_model)"
  driver_dir="$(waveshare_driver_dir)"
  local files=("${model}.py" "epdconfig.py")

  if [[ "${MOCK_INSTALL}" == "1" ]]; then
    for filename in "${files[@]}"; do
      echo "[mock] curl ${WAVESHARE_DRIVER_BASE_URL}/${filename} -> ${driver_dir}/${filename}"
    done
    return 0
  fi

  mkdir -p "${driver_dir}"
  for filename in "${files[@]}"; do
    local dest_file="${driver_dir}/${filename}"
    if [[ -s "${dest_file}" ]]; then
      continue
    fi
    local tmp_file attempt=0 delay=5 success=0
    tmp_file="$(mktemp)"
    while (( attempt < 3 )); do
      attempt=$((attempt + 1))
      echo "Downloading Waveshare driver ${filename} (attempt ${attempt}/3)..."
      if curl --silent --fail --location -o "${tmp_file}" "${WAVESHARE_DRIVER_BASE_URL}/${filename}" && [[ -s "${tmp_file}" ]]; then
        success=1
        break
      fi
      sleep "${delay}"
      delay=$((delay * 2))
    done
    if [[ "${success}" != "1" ]]; then
      rm -f "${tmp_file}"
      echo >&2 "Could not download the Waveshare driver ${filename}."
      echo >&2 "Check the network connection and that the model name exists at:"
      echo >&2 "  https://github.com/waveshareteam/e-Paper/tree/master/RaspberryPi_JetsonNano/python/lib/waveshare_epd"
      return 1
    fi
    mv "${tmp_file}" "${dest_file}"
    chmod 644 "${dest_file}"
  done
  echo "Waveshare driver for ${model} is in ${driver_dir}."
}


boot_config_file() {
  if [[ -f /boot/firmware/config.txt ]]; then
    printf '%s' /boot/firmware/config.txt
  elif [[ -f /boot/config.txt ]]; then
    printf '%s' /boot/config.txt
  fi
}


# Turn on SPI for the panel. Returns 0 if it was already on, 2 if a reboot is needed.
ensure_spi_enabled() {
  if [[ "${MOCK_INSTALL}" == "1" ]]; then
    echo "[mock sudo] raspi-config nonint do_spi 0"
    return 0
  fi
  local reboot_needed=0
  if [[ ! -e /dev/spidev0.0 ]]; then
    if command -v raspi-config >/dev/null 2>&1; then
      echo "Enabling SPI for the e-paper panel."
      run_privileged raspi-config nonint do_spi 0
      reboot_needed=1
    else
      echo >&2 "SPI is not enabled and raspi-config is missing. Enable SPI by hand, then reboot."
      return 2
    fi
  fi
  # InkyPi set up the Waveshare HAT with both SPI chip-select lines; keep the same setup.
  local boot_config
  boot_config="$(boot_config_file)"
  if [[ -n "${boot_config}" ]] && ! grep -q '^dtoverlay=spi0-2cs' "${boot_config}"; then
    if grep -q '^dtparam=spi=on' "${boot_config}"; then
      run_privileged sed -i '/^dtparam=spi=on/a dtoverlay=spi0-2cs' "${boot_config}"
      reboot_needed=1
    fi
  fi
  if [[ "${reboot_needed}" == "1" ]]; then
    return 2
  fi
  return 0
}


# The service talks to the panel as its normal user; that needs the spi and gpio groups.
ensure_panel_groups() {
  local service_user="$1"
  local group missing=()
  for group in spi gpio; do
    if getent group "${group}" >/dev/null 2>&1 && ! id -nG "${service_user}" | tr ' ' '\n' | grep -qx "${group}"; then
      missing+=("${group}")
    fi
  done
  if (( ${#missing[@]} > 0 )); then
    local joined
    joined="$(IFS=,; echo "${missing[*]}")"
    echo "Adding ${service_user} to the ${joined} group(s) for panel access."
    run_privileged usermod -aG "${joined}" "${service_user}"
  fi
}


# Everything the panel needs. Prints a reboot hint when SPI was just turned on.
ensure_panel_ready() {
  local service_user="$1"
  download_waveshare_drivers
  if [[ "${MOCK_INSTALL}" == "1" ]]; then
    return 0
  fi
  ensure_panel_groups "${service_user}"
  local spi_status=0
  ensure_spi_enabled || spi_status=$?
  if [[ "${spi_status}" == "2" ]]; then
    PANEL_REBOOT_NEEDED=1
  fi
}


# Older versions drove the panel through InkyPi. Both must not use SPI at the same time, so the
# InkyPi service is turned off. Its files stay in place so a rollback is still possible.
retire_inkypi_service() {
  if [[ "${MOCK_INSTALL}" == "1" ]]; then
    return 0
  fi
  local dropin="/etc/systemd/system/photo-frame.service.d/after-inkypi.conf"
  if [[ -f "${dropin}" ]]; then
    run_privileged rm -f "${dropin}"
    run_privileged rmdir --ignore-fail-on-non-empty "$(dirname "${dropin}")"
    run_privileged systemctl daemon-reload
  fi
  if ! systemd_unit_exists 'inkypi.service'; then
    return 0
  fi
  if systemctl is-enabled --quiet inkypi.service 2>/dev/null || systemctl is-active --quiet inkypi.service 2>/dev/null; then
    echo "Turning off inkypi.service; the photo frame now drives the panel itself."
    run_privileged systemctl disable --now inkypi.service
  fi
  cat <<'EOF'
InkyPi is no longer used. Once the new version works, you can remove it with:
  sudo rm -rf ~/InkyPi /usr/local/inkypi
  sudo rm -f /etc/systemd/system/inkypi.service /etc/sudoers.d/photo-frame-inkypi
  sudo systemctl daemon-reload
EOF
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
Description=Telegram e-paper photo frame
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
