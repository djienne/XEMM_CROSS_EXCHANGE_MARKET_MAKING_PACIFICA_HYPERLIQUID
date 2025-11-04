# Multi-stage build for optimized image size
# Stage 1: Build
FROM rust:1.83-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src
COPY examples ./examples

# Build release binary
RUN cargo build --release --bin xemm_rust

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/xemm_rust /app/xemm_rust

# Copy configuration (will be overridden by volume mount)
COPY config.json /app/config.json

# Create directory for logs
RUN mkdir -p /app/logs

# Set environment variables
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

# Run as non-root user for security
RUN useradd -m -u 1000 xemm && chown -R xemm:xemm /app
USER xemm

# Run the bot
CMD ["/app/xemm_rust"]
