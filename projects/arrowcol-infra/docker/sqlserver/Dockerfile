# syntax=docker/dockerfile:1.7

# SQL Server 2022 CU27, Ubuntu 22.04. MCR currently publishes this as an
# amd64-only manifest, so the exact manifest digest is intentionally pinned.
FROM mcr.microsoft.com/mssql/server:2022-CU27-ubuntu-22.04@sha256:4402d880dd4c34bfa7d8705e56a86cd6c88da80a1f6bbbe741f999e76264a090

USER root

COPY --chmod=0555 docker/sqlserver/entrypoint.sh /usr/local/bin/sqlserver-secret-entrypoint
COPY --chmod=0555 docker/sqlserver/healthcheck.sh /usr/local/bin/sqlserver-healthcheck
COPY --chmod=0555 docker/sqlserver/sqlcmd-secret.sh /usr/local/bin/sqlcmd-secret
COPY --chmod=0555 scripts/container/bootstrap-databases.sh /usr/local/bin/database-bootstrap
COPY --chmod=0555 scripts/container/run-migrations.sh /usr/local/bin/arrowcol-migrate
COPY --chmod=0444 sql/bootstrap-databases.sql /opt/arrowcol/sql/bootstrap-databases.sql
COPY --chmod=0444 migrations/ /opt/arrowcol/migrations/
COPY --chmod=0444 verification/verify.sql /opt/arrowcol/verification/verify.sql

RUN chown -R mssql:mssql /opt/arrowcol

ENTRYPOINT ["/usr/local/bin/sqlserver-secret-entrypoint"]
CMD ["/opt/mssql/bin/sqlservr"]
