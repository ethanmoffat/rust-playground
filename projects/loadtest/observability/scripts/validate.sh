#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
observability_dir="$(cd "$script_dir/.." && pwd)"
workspace="$(cd "$observability_dir/../../.." && pwd)"
validation_dir="$observability_dir/.validation"

prometheus_image="prom/prometheus:v3.15.0@sha256:efd719c99d83b060d9daefdcf00360461adf279f45ef5391f8d111892118753e"
loki_image="grafana/loki:3.7.8@sha256:1107dd5274e0ada47e42472b7a7e71f3b2a2fe878878108f3e2f9e51528f0193"
tempo_image="grafana/tempo:3.1.0@sha256:3076b8dcdfb32fd6bc5ccef85e7b7313e6199b9cb84366257fc17ecb696db5fd"
alloy_image="grafana/alloy:v1.20.1@sha256:2aa2099af76c0098d4af7a4d6e48f86cb66dc1a000222ad927a1c67c6542d13f"
sql_exporter_image="burningalchemist/sql_exporter:0.24.9@sha256:6c554722faccd4b54342ccd765f87b26e8e65fba2a8cddc60bec6de336ecf2e0"
json_exporter_image="quay.io/prometheuscommunity/json-exporter:v0.8.0@sha256:3aad20d41a0b31dfaaf7ab1a091440e92a82a61ef68b76750eac0ad50a983652"

cleanup() {
  rm -rf "$validation_dir"
}
trap cleanup EXIT
cleanup
mkdir -p "$validation_dir"

cd "$workspace"

node "$observability_dir/scripts/generate-dashboards.mjs"
node "$observability_dir/scripts/validate.mjs" "$validation_dir/dashboard-promql.rules.yml"

docker run --rm \
  -v "$observability_dir/prometheus:/etc/prometheus:ro" \
  --entrypoint /bin/promtool \
  "$prometheus_image" \
  check config /etc/prometheus/prometheus.yml

docker run --rm \
  -v "$observability_dir/prometheus/rules:/rules:ro" \
  --entrypoint /bin/promtool \
  "$prometheus_image" \
  check rules /rules/arrowcol.rules.yml

docker run --rm \
  -v "$validation_dir/dashboard-promql.rules.yml:/rules/dashboard.rules.yml:ro" \
  --entrypoint /bin/promtool \
  "$prometheus_image" \
  check rules /rules/dashboard.rules.yml

docker run --rm \
  -e LOADTEST_RUN_ID=validation \
  -e LOADTEST_OBSERVABILITY_SCENARIO=comparison \
  -v "$observability_dir/alloy/config.alloy:/etc/alloy/config.alloy:ro" \
  "$alloy_image" \
  validate /etc/alloy/config.alloy

docker run --rm \
  -v "$observability_dir/alloy/config.alloy:/etc/alloy/config.alloy:ro" \
  "$alloy_image" \
  fmt --test /etc/alloy/config.alloy

docker run --rm \
  -v "$observability_dir/loki/loki.yaml:/etc/loki/loki.yaml:ro" \
  "$loki_image" \
  -config.file=/etc/loki/loki.yaml -verify-config

docker run --rm \
  -v "$observability_dir/tempo/tempo.yaml:/etc/tempo/tempo.yaml:ro" \
  "$tempo_image" \
  -config.file=/etc/tempo/tempo.yaml -config.verify=true -config.verify-errors-only=true

docker run --rm \
  -e SQLEXPORTER_TARGET_DSN='sqlserver://arrowcol_app:validation@sqlserver:1433/arrowcol?encrypt=true&TrustServerCertificate=true' \
  -v "$observability_dir/sql-exporter:/etc/sql-exporter:ro" \
  "$sql_exporter_image" \
  --config.file=/etc/sql-exporter/sql_exporter.yml -config.check

docker run --rm \
  -v "$observability_dir/json-exporter/config.yml:/etc/json-exporter/config.yml:ro" \
  "$json_exporter_image" \
  --config.file=/etc/json-exporter/config.yml --config.check

LOADTEST_UID="$(id -u)" \
LOADTEST_GID="$(id -g)" \
LOADTEST_RUN_ID="validation" \
LOADTEST_SCRATCH_HOST_PATH="$workspace/projects/loadtest/.temp" \
LOADTEST_BIN_HOST_PATH="$workspace/target/debug" \
ONELAKE_HOST_PATH="$workspace" \
ARROWCOL_APP_PASSWORD_FILE="${ARROWCOL_APP_PASSWORD_FILE:-$workspace/projects/arrowcol-infra/secrets/arrowcol-app-password.txt}" \
docker compose \
  --project-name lgtm-config-check \
  --file "$workspace/projects/loadtest/compose.yaml" \
  --file "$workspace/projects/loadtest/compose.observability.yaml" \
  config --quiet

echo "PASS: observability syntax, metric contracts, dashboards, PromQL, exporters, and merged Compose config"
