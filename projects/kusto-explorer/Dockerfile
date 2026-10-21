# syntax=docker/dockerfile:1.7

# The wasm-bindgen CLI must match the library version locked in Cargo.lock. Extracting the version
# into its own file keeps the CLI build cached until that version actually changes.
FROM rust:1.96.1-bookworm@sha256:a339861ae23e9abb272cea45dfafde21760d2ce6577a70f8a926153677902663 AS wasm-bindgen-version
COPY Cargo.lock /tmp/Cargo.lock
RUN sed -n '/^name = "wasm-bindgen"$/{n;s/^version = "\(.*\)"$/\1/p;q;}' /tmp/Cargo.lock > /tmp/wasm-bindgen-version \
    && test -s /tmp/wasm-bindgen-version

FROM rust:1.96.1-bookworm@sha256:a339861ae23e9abb272cea45dfafde21760d2ce6577a70f8a926153677902663 AS wasm-bindgen
COPY --from=wasm-bindgen-version /tmp/wasm-bindgen-version /tmp/wasm-bindgen-version
RUN --mount=type=cache,id=rust-playground-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=rust-playground-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    cargo install --locked --root /opt/wasm-bindgen wasm-bindgen-cli --version "$(cat /tmp/wasm-bindgen-version)"

FROM rust:1.96.1-bookworm@sha256:a339861ae23e9abb272cea45dfafde21760d2ce6577a70f8a926153677902663 AS builder
COPY --from=wasm-bindgen /opt/wasm-bindgen/bin/wasm-bindgen /usr/local/bin/wasm-bindgen
WORKDIR /workspace
COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml /workspace/
RUN rustup target add wasm32-unknown-unknown
COPY crates /workspace/crates
COPY bin /workspace/bin
COPY projects /workspace/projects
COPY submodules/otel-arrow /workspace/submodules/otel-arrow
RUN --mount=type=cache,id=rust-playground-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=rust-playground-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=rust-playground-target,target=/workspace/target,sharing=locked \
    CARGO_TARGET_DIR=/workspace/target cargo build --locked --release \
      --package kusto-explorer-ui --target wasm32-unknown-unknown \
    && CARGO_TARGET_DIR=/workspace/target cargo build --locked --release --package kusto-explorer \
    && wasm-bindgen --target web --no-typescript --out-dir /out/pkg \
      /workspace/target/wasm32-unknown-unknown/release/kusto_explorer_ui.wasm \
    && install --mode=0555 /workspace/target/release/kusto-explorer /out/kusto-explorer \
    && strip /out/kusto-explorer

FROM node:24.22.0-bookworm-slim@sha256:a5f4366e834695d5f363b920c104953a535d3a9864d1bdf74566a0ebe01daee5 AS web
WORKDIR /workspace/projects/kusto-explorer
COPY projects/kusto-explorer/package.json projects/kusto-explorer/package-lock.json ./
RUN --mount=type=cache,id=rust-playground-npm,target=/root/.npm,sharing=locked \
    npm ci --no-audit --no-fund
COPY projects/kusto-explorer/web ./web
COPY --from=builder /out/pkg /tmp/pkg
RUN node web/build.mjs --pkg /tmp/pkg --out /out/www \
    && find /out/www -type d -exec chmod 0555 {} + \
    && find /out/www -type f -exec chmod 0444 {} +

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS runtime
ARG VCS_REF=local
LABEL org.opencontainers.image.title="kusto-explorer" \
      org.opencontainers.image.description="Browser-based KQL workbench and query proxy for Kusto clusters" \
      org.opencontainers.image.source="https://github.com/mdrakiburrahman/rust-playground" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.revision="${VCS_REF}"
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home --no-log-init --home-dir /nonexistent --shell /usr/sbin/nologin app
COPY --from=builder --chmod=0555 /out/kusto-explorer /usr/local/bin/kusto-explorer
COPY --from=web /out/www /usr/share/kusto-explorer/www
USER 10001:10001
STOPSIGNAL SIGTERM
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 \
  CMD ["curl", "--fail", "--silent", "--show-error", "--max-time", "2", "http://127.0.0.1:8080/healthz"]
ENTRYPOINT ["/usr/local/bin/kusto-explorer"]
