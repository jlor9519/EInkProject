#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"


predownload_waveshare_drivers() {
  local src_root="$1"
  local ws_model="$2"
  local driver_dest="${src_root}/display/waveshare_epd"
  local base_url="https://raw.githubusercontent.com/waveshareteam/e-Paper/master/RaspberryPi_JetsonNano/python/lib/waveshare_epd"
  local max_retries=3
  local files=("${ws_model}.py" "epdconfig.py")

  if [[ "${MOCK_INSTALL}" == "1" ]]; then
    echo "[mock] Pre-downloading Waveshare drivers to ${driver_dest}"
    for filename in "${files[@]}"; do
      echo "[mock] curl ${base_url}/${filename} -> ${driver_dest}/${filename}"
    done
    return 0
  fi

  if path_is_writable_or_creatable "${driver_dest}"; then
    mkdir -p "${driver_dest}"
  else
    run_privileged mkdir -p "${driver_dest}"
  fi

  for filename in "${files[@]}"; do
    local dest_file="${driver_dest}/${filename}"
    local url="${base_url}/${filename}"

    if [[ -f "${dest_file}" && -s "${dest_file}" ]]; then
      echo "  Waveshare driver ${filename} already exists, skipping download."
      continue
    fi

    local tmp_file
    tmp_file="$(mktemp)"
    local attempt=0
    local delay=5
    local success=0

    while (( attempt < max_retries )); do
      attempt=$((attempt + 1))
      echo "  Downloading ${filename} (attempt ${attempt}/${max_retries})..."
      local http_code
      http_code="$(curl --silent --fail --location -o "${tmp_file}" -w '%{http_code}' "${url}" 2>/dev/null)" && {
        if [[ -s "${tmp_file}" ]]; then
          success=1
          break
        fi
        echo "  Download returned empty file. Retrying in ${delay}s..."
      } || {
        echo "  Download failed (HTTP ${http_code}). Retrying in ${delay}s..."
      }
      sleep "${delay}"
      delay=$((delay * 2))
    done

    if [[ "${success}" != "1" ]]; then
      rm -f "${tmp_file}"
      echo >&2 "Failed to download Waveshare driver ${filename} after ${max_retries} attempts."
      echo >&2 "URL: ${url}"
      echo >&2 "Check network connectivity and verify the model name exists at:"
      echo >&2 "  https://github.com/waveshareteam/e-Paper/tree/master/RaspberryPi_JetsonNano/python/lib/waveshare_epd"
      return 1
    fi

    if path_is_writable_or_creatable "${dest_file}"; then
      mv "${tmp_file}" "${dest_file}"
    else
      run_privileged cp "${tmp_file}" "${dest_file}"
      rm -f "${tmp_file}"
    fi
    echo "  Waveshare driver ${filename} downloaded successfully."
  done
}


ensure_not_running_as_root
ensure_runtime_files
ensure_venv

configured_repo_path="$(get_yaml_value inkypi.repo_path string)"
configured_install_path="$(get_yaml_value inkypi.install_path string)"
inkypi_layout=()
while IFS= read -r line; do
  inkypi_layout+=("${line}")
done < <(resolve_inkypi_layout_values "${configured_repo_path}" "${configured_install_path}")
repo_path="${inkypi_layout[0]}"
install_path="${inkypi_layout[1]}"
source_root="${inkypi_layout[2]}"
device_config_path="${inkypi_layout[3]}"
git_sync_path="${inkypi_layout[4]}"
source_origin="${inkypi_layout[5]}"
replaced_stale_repo_path="${inkypi_layout[6]}"
install_src_exists="${inkypi_layout[7]}"

validated_commit="$(get_yaml_value inkypi.validated_commit string)"
validated_commit="${validated_commit:-main}"
waveshare_model="$(get_yaml_value inkypi.waveshare_model string)"
waveshare_model="${waveshare_model:-epd7in3e}"
plugin_id="$(get_yaml_value inkypi.plugin_id string)"
plugin_id="${plugin_id:-telegram_frame}"
payload_dir="$(get_yaml_value inkypi.payload_dir string)"
payload_dir="${payload_dir:-${PROJECT_ROOT}/data/inkypi}"
payload_dir="$(expand_path "${payload_dir}")"
update_method="$(get_yaml_value inkypi.update_method string)"
update_now_url="$(get_yaml_value inkypi.update_now_url string)"
refresh_command="$(get_yaml_value inkypi.refresh_command string)"
inkypi_update_values=()
while IFS= read -r line; do
  inkypi_update_values+=("${line}")
done < <(resolve_inkypi_update_values "${update_method}" "${update_now_url}" "${refresh_command}")
update_method="${inkypi_update_values[0]}"
update_now_url="${inkypi_update_values[1]}"
refresh_command="${inkypi_update_values[2]}"

set_yaml_value inkypi.repo_path string "${repo_path}"
set_yaml_value inkypi.install_path string "${install_path}"
set_yaml_value inkypi.validated_commit string "${validated_commit}"
set_yaml_value inkypi.waveshare_model string "${waveshare_model}"
set_yaml_value inkypi.plugin_id string "${plugin_id}"
set_yaml_value inkypi.payload_dir string "${payload_dir}"
set_yaml_value inkypi.update_method string "${update_method}"
set_yaml_value inkypi.update_now_url string "${update_now_url}"
set_yaml_value inkypi.refresh_command string "${refresh_command}"

service_user="${SUDO_USER:-$(id -un)}"
echo "Installing scoped sudoers access so ${service_user} can reload inkypi.service."
ensure_inkypi_service_reload_sudoers "${service_user}"

fresh_clone=0
if [[ "${replaced_stale_repo_path}" == "1" ]]; then
  echo "Replaced stale InkyPi repo_path with ${repo_path}."
fi

echo "Resolved InkyPi checkout path: ${repo_path}"
echo "Resolved InkyPi runtime install path: ${install_path}"
echo "Resolved InkyPi source tree: ${source_root}"
echo "Resolved InkyPi device config path: ${device_config_path}"

if [[ "${source_origin}" == "planned_clone" ]]; then
  fresh_clone=1
  echo "Git sync source: no existing InkyPi checkout found, cloning into ${repo_path}."
  if path_parent_is_writable "${repo_path}"; then
    run_cmd git clone https://github.com/fatihak/InkyPi.git "${repo_path}"
  else
    run_privileged git clone https://github.com/fatihak/InkyPi.git "${repo_path}"
  fi
  source_root="${repo_path}/src"
  device_config_path="${source_root}/config/device.json"
  git_sync_path="${repo_path}"
elif [[ -n "${git_sync_path}" && -d "${git_sync_path}/.git" ]]; then
  echo "Git sync source: ${git_sync_path}"
  if path_is_writable_or_creatable "${git_sync_path}/.git"; then
    run_cmd git -C "${git_sync_path}" fetch --all --tags
  else
    run_privileged git -C "${git_sync_path}" fetch --all --tags
  fi
else
  echo "Git sync source: skipped because the resolved source tree does not have a writable git checkout."
fi

if [[ -n "${git_sync_path}" && -d "${git_sync_path}/.git" ]]; then
  if path_is_writable_or_creatable "${git_sync_path}/.git"; then
    run_cmd git -C "${git_sync_path}" checkout "${validated_commit}"
  else
    run_privileged git -C "${git_sync_path}" checkout "${validated_commit}"
  fi
fi

source_plugin_dir="${PROJECT_ROOT}/integrations/inkypi_plugin/telegram_frame"
target_plugin_dir="${source_root}/plugins/${plugin_id}"
echo "Plugin source path: ${source_plugin_dir}"
echo "Final plugin target path: ${target_plugin_dir}"

if path_is_writable_or_creatable "${source_root}"; then
  mkdir -p "${source_root}/plugins" "${source_root}/config"
  if ! rsync -a --delete "${source_plugin_dir}/" "${target_plugin_dir}/"; then
    echo "Normal plugin sync failed, retrying with sudo to handle protected files in ${target_plugin_dir}."
    run_privileged mkdir -p "${source_root}/plugins" "${source_root}/config"
    run_privileged rsync -a --delete "${source_plugin_dir}/" "${target_plugin_dir}/"
  fi
else
  run_privileged mkdir -p "${source_root}/plugins" "${source_root}/config"
  run_privileged rsync -a --delete "${source_plugin_dir}/" "${target_plugin_dir}/"
fi

payload_json_path="${payload_dir%/}/current.json"
dashboard_seed_result=()
if path_is_writable_or_creatable "${device_config_path}"; then
  while IFS= read -r line; do
    dashboard_seed_result+=("${line}")
  done < <("${RUN_PYTHON}" - "${device_config_path}" "${plugin_id}" "${payload_json_path}" <<'PY'
from app.inkypi_setup import seed_dashboard_plugin_instance, verify_seeded_plugin_instance
import sys

device_path = sys.argv[1]
plugin_id = sys.argv[2]
payload_path = sys.argv[3]
result = seed_dashboard_plugin_instance(device_path, plugin_id, payload_path)
if result.applied:
    verify_seeded_plugin_instance(device_path, plugin_id, payload_path)
print("1" if result.applied else "0")
print(result.message)
PY
)
else
  while IFS= read -r line; do
    dashboard_seed_result+=("${line}")
  done < <(run_privileged env PYTHONPATH="${PROJECT_ROOT}${PYTHONPATH:+:${PYTHONPATH}}" python3 - "${device_config_path}" "${plugin_id}" "${payload_json_path}" <<'PY'
from app.inkypi_setup import seed_dashboard_plugin_instance, verify_seeded_plugin_instance
import sys

device_path = sys.argv[1]
plugin_id = sys.argv[2]
payload_path = sys.argv[3]
result = seed_dashboard_plugin_instance(device_path, plugin_id, payload_path)
if result.applied:
    verify_seeded_plugin_instance(device_path, plugin_id, payload_path)
print("1" if result.applied else "0")
print(result.message)
PY
)
fi
dashboard_seed_applied="${dashboard_seed_result[0]:-0}"
dashboard_seed_message="${dashboard_seed_result[1]:-Dashboard seed status unavailable.}"
echo "Dashboard seed status: ${dashboard_seed_message}"

if [[ -f "${repo_path}/install/install.sh" ]]; then
  should_run_inkypi_install=0
  if [[ "${fresh_clone}" == "1" ]]; then
    should_run_inkypi_install=1
  elif [[ "${install_src_exists}" != "1" ]]; then
    should_run_inkypi_install=1
  elif [[ "${INKYPI_FORCE_INSTALL:-0}" == "1" ]]; then
    should_run_inkypi_install=1
  fi

  if [[ "${should_run_inkypi_install}" == "1" ]]; then
    echo "Running InkyPi installer for Waveshare model ${waveshare_model}."
    predownload_waveshare_drivers "${source_root}" "${waveshare_model}"
    if [[ "${MOCK_INSTALL}" == "1" ]]; then
      echo "[mock sudo] bash ${repo_path}/install/install.sh -W ${waveshare_model}"
    else
      echo "n" | run_privileged bash "${repo_path}/install/install.sh" -W "${waveshare_model}"
    fi
  else
    echo "Skipping InkyPi installer rerun because an existing installation was detected."
    echo "Set INKYPI_FORCE_INSTALL=1 when running setup_inkypi.sh if you need to rerun it."
  fi
fi

if path_is_writable_or_creatable "${device_config_path}"; then
  "${RUN_PYTHON}" - "${device_config_path}" <<'PY'
from app.inkypi_setup import seed_device_defaults
import sys
seed_device_defaults(sys.argv[1])
PY
else
  run_privileged env PYTHONPATH="${PROJECT_ROOT}${PYTHONPATH:+:${PYTHONPATH}}" python3 - "${device_config_path}" <<'PY'
from app.inkypi_setup import seed_device_defaults
import sys
seed_device_defaults(sys.argv[1])
PY
fi
echo "Device defaults seeded into ${device_config_path}."

if [[ ! -f "${target_plugin_dir}/telegram_frame.py" ]]; then
  echo >&2 "Plugin verification failed: ${target_plugin_dir}/telegram_frame.py was not created."
  exit 1
fi
if [[ ! -f "${target_plugin_dir}/plugin-info.json" ]]; then
  echo >&2 "Plugin verification failed: ${target_plugin_dir}/plugin-info.json was not created."
  exit 1
fi

plugin_class_name="$("${RUN_PYTHON}" - "${target_plugin_dir}/plugin-info.json" <<'PY'
import json
import sys
from pathlib import Path

plugin_info = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(plugin_info.get("class", ""))
PY
)"
if [[ -z "${plugin_class_name}" ]]; then
  echo >&2 "Plugin verification failed: class name is missing from ${target_plugin_dir}/plugin-info.json."
  exit 1
fi

plugin_verification_python="${install_path}/venv_inkypi/bin/python"
if [[ ! -x "${plugin_verification_python}" ]]; then
  plugin_verification_python="${RUN_PYTHON}"
fi

if ! PYTHONPATH="${PROJECT_ROOT}${PYTHONPATH:+:${PYTHONPATH}}" "${plugin_verification_python}" - "${source_root}" "${plugin_id}" "${plugin_class_name}" <<'PY'
from app.inkypi_setup import verify_plugin_module_import
import sys

verify_plugin_module_import(sys.argv[1], sys.argv[2], sys.argv[3])
PY
then
  echo "Warning: Plugin module import verification failed. This may resolve after InkyPi installs its dependencies."
fi

if [[ "${MOCK_INSTALL}" == "1" ]]; then
  echo "[mock] Skipping inkypi.service restart and HTTP registration verification."
elif systemd_unit_exists 'inkypi.service'; then
  echo "Restarting inkypi.service so the plugin is loaded."
  run_privileged systemctl restart inkypi.service
  ensure_systemd_service_active inkypi.service

  if ! "${RUN_PYTHON}" - <<'PY'
import time
from urllib import error, request

url = "http://127.0.0.1/"
last_error = "unknown error"

time.sleep(5)
for _ in range(30):
    try:
        with request.urlopen(url, timeout=5) as response:
            response.read()
            raise SystemExit(0)
    except error.HTTPError:
        raise SystemExit(0)  # any HTTP response means InkyPi is serving
    except Exception as exc:
        last_error = str(exc)
    time.sleep(1)

raise SystemExit(f"InkyPi did not become reachable at {url}: {last_error}")
PY
  then
    echo >&2 "Recent inkypi.service journal output:"
    run_privileged journalctl -u inkypi.service -n 80 --no-pager || true
    exit 1
  fi
else
  echo >&2 "inkypi.service was not found after setup."
  exit 1
fi

echo "InkyPi setup completed. Plugin copied to ${target_plugin_dir}."
