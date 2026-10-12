#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

force=false
if [[ "${1:-}" == "--force" ]]; then
  force=true
elif [[ $# -gt 0 ]]; then
  echo "Usage: $0 [--force]" >&2
  exit 64
fi

if [[ ! -f "$ENV_FILE" ]]; then
  cp "$EXAMPLE_ENV_FILE" "$ENV_FILE"
  chmod 0600 "$ENV_FILE"
  echo "Created $ENV_FILE from .env.example"
fi

load_local_env
umask 077

if [[ "$force" == true ]] && command -v docker >/dev/null 2>&1; then
  sql_volume="${COMPOSE_PROJECT_NAME:-arrowcol-infra}_sqlserver-data"
  if docker volume inspect "$sql_volume" >/dev/null 2>&1; then
    echo "Refusing to rotate secrets while SQL volume $sql_volume exists." >&2
    echo "Run arrowcol-infra:reset first, then repeat bootstrap with --force." >&2
    exit 1
  fi
fi

generate_password() {
  printf 'Aa1!%s\n' "$(openssl rand -hex 24)"
}

while IFS= read -r secret; do
  install -d -m 0700 "$(dirname "$secret")"

  if [[ -e "$secret" && "$force" != true ]]; then
    chmod 0600 "$secret"
    echo "Preserved existing secret: $secret"
    continue
  fi

  generate_password >"$secret"
  chmod 0600 "$secret"
  echo "Generated secret: $secret"
done < <(required_secret_paths)

echo "Local Docker secrets are ready. Values were not printed."
