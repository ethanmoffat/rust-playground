#!/usr/bin/env bash
set -Eeuo pipefail

read_secret() {
  local path="$1"
  local value

  if [[ ! -r "$path" ]]; then
    echo "Hive database password secret is not readable: $path" >&2
    return 64
  fi

  value="$(<"$path")"
  if [[ -z "$value" || "$value" == *$'\n'* || "$value" == *$'\r'* ]]; then
    echo "Hive database password secret must be non-empty and single-line" >&2
    return 64
  fi

  printf '%s' "$value"
}

xml_escape() {
  sed \
    -e 's/&/\&amp;/g' \
    -e 's/</\&lt;/g' \
    -e 's/>/\&gt;/g'
}

write_runtime_configuration() {
  local password escaped_password runtime_conf

  password="$(read_secret "${HIVE_DB_PASSWORD_FILE:-/run/secrets/hive_metastore_password}")"
  escaped_password="$(printf '%s' "$password" | xml_escape)"
  unset password

  runtime_conf="/opt/hive/runtime-conf/$$"
  install -d -m 0700 "$runtime_conf"

  cat >"$runtime_conf/hive-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:sqlserver://${HIVE_DB_HOST:-sqlserver}:${HIVE_DB_PORT:-1433};databaseName=${HIVE_DB_NAME:-hive_metastore};encrypt=true;trustServerCertificate=true;loginTimeout=30;applicationName=arrowcol-hive-metastore</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>com.microsoft.sqlserver.jdbc.SQLServerDriver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>${HIVE_DB_USER:-hive_metastore}</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>${escaped_password}</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateAll</name>
    <value>false</value>
  </property>
  <property>
    <name>datanucleus.autoCreateSchema</name>
    <value>false</value>
  </property>
  <property>
    <name>datanucleus.autoCreateTables</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.metastore.thrift.bind.host</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>file:///opt/hive/data/warehouse</value>
  </property>
  <property>
    <name>hive.metastore.disallow.incompatible.col.type.changes</name>
    <value>true</value>
  </property>
</configuration>
EOF

  chmod 0600 "$runtime_conf/hive-site.xml"
  if [[ "$(id -u)" -eq 0 ]]; then
    chown -R hive:hive "$runtime_conf"
  fi
  unset escaped_password
  printf '%s' "$runtime_conf"
}

run_as_hive() {
  if [[ "$(id -u)" -eq 0 ]]; then
    setpriv --reuid=1000 --regid=1000 --clear-groups "$@"
  else
    "$@"
  fi
}

run_schema_tool() {
  local action="$1"
  local runtime_conf

  runtime_conf="$(write_runtime_configuration)"
  export HIVE_CONF_DIR="$runtime_conf"
  export HADOOP_CONF_DIR="$runtime_conf"
  export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx768m"

  case "$action" in
    schema-init)
      run_as_hive "$HIVE_HOME/bin/schematool" -dbType mssql -initOrUpgradeSchema
      run_as_hive "$HIVE_HOME/bin/schematool" -dbType mssql -validate
      run_as_hive "$HIVE_HOME/bin/schematool" -dbType mssql -info
      ;;
    schema-verify)
      run_as_hive "$HIVE_HOME/bin/schematool" -dbType mssql -validate
      run_as_hive "$HIVE_HOME/bin/schematool" -dbType mssql -info
      ;;
    *)
      echo "Unsupported schema action: $action" >&2
      return 64
      ;;
  esac

  rm -rf "$runtime_conf"
}

case "${1:-service}" in
  schema-init | schema-verify)
    run_schema_tool "$1"
    ;;
  service)
    runtime_conf="$(write_runtime_configuration)"
    export HIVE_CUSTOM_CONF_DIR="$runtime_conf"
    export DB_DRIVER=mssql
    export SERVICE_NAME=metastore
    export IS_RESUME=true
    if [[ "$(id -u)" -eq 0 ]]; then
      exec setpriv --reuid=1000 --regid=1000 --clear-groups /entrypoint.sh
    fi
    exec /entrypoint.sh
    ;;
  *)
    echo "Usage: hive-secret-entrypoint {service|schema-init|schema-verify}" >&2
    exit 64
    ;;
esac
