#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set +a
fi

: "${WORKSTATION_HOST:?请在 .env 中配置 WORKSTATION_HOST}"
: "${WORKSTATION_PORT:?请在 .env 中配置 WORKSTATION_PORT}"
: "${WORKSTATION_USER:?请在 .env 中配置 WORKSTATION_USER}"
: "${WORKSTATION_PASSWORD:?请在 .env 中配置 WORKSTATION_PASSWORD}"

if [[ $# -eq 0 ]]; then
  cat <<'EOF'
用法:
  scripts/ops/workstation_ssh.sh "cd /quantmind && ls -lh"
EOF
  exit 1
fi

exec sshpass -p "${WORKSTATION_PASSWORD}" ssh \
  -p "${WORKSTATION_PORT}" \
  -o StrictHostKeyChecking=no \
  "${WORKSTATION_USER}@${WORKSTATION_HOST}" \
  "$@"
