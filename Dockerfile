# syntax=docker/dockerfile:1

# ---- chef base: shared Rust toolchain + cargo-chef ----
FROM rust:1.96-alpine AS chef
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
RUN apk add --no-cache watchexec
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --recipe-path recipe.json
COPY . .
EXPOSE 8080
CMD ["watchexec", "--restart", "--stop-signal", "SIGINT", "--exts", "rs,toml,lock,sql", "--", "cargo", "run"]

# ---- builder: pre-build deps, then the real binary ----
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release && \
    cp /app/target/release/parseon /usr/local/bin/parseon

# ---- runtime: minimal alpine, non-root, healthcheck ----
FROM alpine:3.20 AS runtime
RUN apk add --no-cache ca-certificates wget && \
    adduser -D -u 1000 parseon
COPY --from=builder /usr/local/bin/parseon /usr/local/bin/parseon
USER parseon
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=5 \
  CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["parseon"]
