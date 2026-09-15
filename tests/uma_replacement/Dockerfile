# syntax=docker/dockerfile:1
FROM rust:1.96-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake pkg-config libssl-dev python3 && rm -rf /var/lib/apt/lists/*
RUN rustup component add clippy rustfmt
WORKDIR /source
ENV CARGO_BUILD_JOBS=4
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
RUN --mount=type=cache,id=polym-rust-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=polym-rust-target,target=/source/target,sharing=locked \
    cargo test --locked --no-run --message-format=json > /tmp/artifacts.jsonl && \
    mkdir /binaries && python3 -c 'import json,shutil; [shutil.copy2(v["executable"], "/binaries/") for line in open("/tmp/artifacts.jsonl") if (v:=json.loads(line)).get("executable") and v.get("profile",{}).get("test")]' && \
    cargo clippy --locked --all-targets -- -D warnings && cargo fmt --check
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates python3 nats-server redis-server && rm -rf /var/lib/apt/lists/*
COPY --from=build /binaries /binaries
COPY tests/uma_replacement/run.py /runner.py
USER 65534:65534
ENTRYPOINT ["python3", "/runner.py"]
