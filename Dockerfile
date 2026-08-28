# syntax=docker/dockerfile:1

# ---- chef base: shared Rust toolchain + cargo-chef ----
FROM rust:1.96-alpine@sha256:a41f7740f8b45d45795624eec13a8b42263cc700f19f7e4e86e04d3dda08a479 AS chef
RUN apk add --no-cache musl-dev
RUN cargo install cargo-chef --locked
WORKDIR /app

# ---- planner: produce a recipe from the manifests only ----
# cargo-chef inspects the workspace, so COPY . . (kept small by .dockerignore)
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- development: cached debug dependencies + hot reload ----
FROM chef AS development
ARG PARSEON_FEATURES=postgres-storage
ENV PARSEON_FEATURES=${PARSEON_FEATURES}
RUN apk add --no-cache watchexec
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --locked --recipe-path recipe.json --no-default-features --features "${PARSEON_FEATURES}"
COPY . .
EXPOSE 8080
CMD ["sh", "-c", "exec watchexec --restart --stop-signal SIGINT --exts rs,toml,lock,sql -- cargo run --no-default-features --features \"${PARSEON_FEATURES}\""]

# ---- builder: pre-build deps, then the real binary ----
FROM chef AS builder
ARG PARSEON_FEATURES=postgres-storage
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo chef cook --locked --release --recipe-path recipe.json --no-default-features --features "${PARSEON_FEATURES}"
COPY . .
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked --no-default-features --features "${PARSEON_FEATURES}" && \
    cp /app/target/release/parseon /usr/local/bin/parseon

# ---- runtime: minimal alpine, non-root, healthcheck ----
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc AS runtime
RUN apk add --no-cache ca-certificates wget && \
    adduser -D -u 1000 parseon
COPY --from=builder /usr/local/bin/parseon /usr/local/bin/parseon
USER parseon
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=5 \
  CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["parseon"]
