# syntax=docker/dockerfile:1

# ---- builder ----
FROM rust:1.96-slim AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/parseon

# Cache deps by copying manifests first
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && cargo build --release || true

# Copy actual sources and build
COPY src ./src
RUN touch src/main.rs && cargo build --release

# ---- runtime ----
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/parseon/target/release/parseon /usr/local/bin/parseon

EXPOSE 8080

ENTRYPOINT ["parseon"]