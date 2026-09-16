# syntax=docker/dockerfile:1
FROM rust:1.96-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
RUN rustup component add clippy rustfmt
WORKDIR /source
ENV CARGO_BUILD_JOBS=4
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
COPY deploy ./deploy
RUN --mount=type=cache,id=polym-rust-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=polym-rust-target,target=/source/target,sharing=locked \
    cargo test --locked && cargo clippy --locked --all-targets -- -D warnings && cargo fmt --check && cargo build --release --locked && cp target/release/polym-rust-demo /binary
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --no-create-home demo && mkdir -p /app/data && chown demo:demo /app/data
WORKDIR /app
COPY --from=build /binary /usr/local/bin/polym-rust-demo
COPY deploy/config.json /app/config.json
COPY deploy/shadow.json /app/shadow.json
COPY deploy/live.json /app/live.json
COPY deploy/uma-trader.json /app/uma-trader.json
USER 10001:10001
ENTRYPOINT ["polym-rust-demo"]
CMD ["serve", "/app/config.json"]
