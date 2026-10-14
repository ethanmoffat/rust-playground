ARG SOURCE_DEVCONTAINER=rust-playground-ci-base:source
FROM ${SOURCE_DEVCONTAINER}

USER vscode
RUN cargo install cargo-make --version 0.37.24 --locked

ENV CARGO_INCREMENTAL=0 \
    CARGO_PROFILE_DEV_DEBUG=0 \
    CARGO_PROFILE_TEST_DEBUG=0 \
    RUSTFLAGS="-C link-arg=-fuse-ld=mold"
