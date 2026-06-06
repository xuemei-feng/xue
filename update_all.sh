#!/usr/bin/env bash
set -euo pipefail

# Repo root = directory containing this script (works no matter where you invoke from).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

SOURCE_DIR="$SCRIPT_DIR"
HOSTS_FILE="${SCRIPT_DIR}/hosts"
REMOTE_DIR="/users/xue/xue"

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "Error: hosts file not found: $HOSTS_FILE" >&2
  exit 1
fi

while read -r ip; do
  [[ -z "${ip// }" || "$ip" =~ ^# ]] && continue
  echo "Copying to host: $ip..."
  sudo rsync -avz --delete \
    --exclude='project/cmake/build/CMakeFiles' \
    --exclude='project/cmake/build/run_client' \
    --exclude='project/cmake/build/main_test' \
    --exclude='project/cmake/build/main_client' \
    --exclude='storage/*' \
    -e ssh "${SOURCE_DIR}/" "${ip}:${REMOTE_DIR}/"
  echo "Successfully copied to $ip!"
done < "$HOSTS_FILE"

bash "${SCRIPT_DIR}/generate_run_proxy.sh"

echo "All done!"
