FROM rust:1.80-slim-bookworm AS builder
WORKDIR /usr/src/app

# Install build dependencies
RUN apt-get update && apt-get install -y pkg-config libssl-dev

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy src to build dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    echo "" > src/lib.rs && \
    cargo build --release && \
    rm -rf src

# Copy source code
COPY src ./src
COPY tests ./tests
COPY benches ./benches

# Build actual application
# We need to touch main.rs so cargo knows it's updated
RUN touch src/main.rs && cargo build --release

# Final runtime image
FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/target/release/fluxcache /usr/local/bin/fluxcache

# Create data directory
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 6380 9090

# Run fluxcache with default paths configured for docker
ENTRYPOINT ["fluxcache"]
CMD ["--listen", "0.0.0.0:6380", "--admin-listen", "0.0.0.0:9090", "--data-dir", "/data"]
