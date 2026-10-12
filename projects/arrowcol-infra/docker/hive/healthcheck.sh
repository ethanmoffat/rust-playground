#!/usr/bin/env bash
set -Eeuo pipefail

port="${METASTORE_PORT:-9083}"
exec timeout 3 bash -c "exec 3<>/dev/tcp/127.0.0.1/${port}"
