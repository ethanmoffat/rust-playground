ARG SOURCE_DEVCONTAINER=rust-playground-ci-base:source
FROM ${SOURCE_DEVCONTAINER}

USER vscode
RUN cargo install cargo-make --version 0.37.24 --locked
