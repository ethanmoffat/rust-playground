# syntax=docker/dockerfile:1.7

# Apache Hive 4.0.1 multi-architecture index digest.
FROM apache/hive:4.0.1@sha256:5194161ef50b80875f937dff04936df047cbe894e9727ab8f0606349345b1bd3

USER root

# Hive 4.0.1's upstream image runs Java 8, so use the matching Microsoft JDBC
# artifact and verify the Maven Central payload at build time.
ADD --checksum=sha256:79da517f86a982f0c371f804ba06305708c43923786386c2582c81cb3e4ae648 \
  https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/13.6.0.jre8/mssql-jdbc-13.6.0.jre8.jar \
  /opt/hive/lib/mssql-jdbc.jar

COPY --chmod=0555 docker/hive/entrypoint.sh /usr/local/bin/hive-secret-entrypoint
COPY --chmod=0555 docker/hive/healthcheck.sh /usr/local/bin/hive-healthcheck

RUN chown hive:hive /opt/hive/lib/mssql-jdbc.jar \
  && mkdir -p /opt/hive/runtime-conf \
  && chown hive:hive /opt/hive/runtime-conf

ENTRYPOINT ["/usr/local/bin/hive-secret-entrypoint"]
CMD ["service"]
