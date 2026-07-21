# ── Builder stage ─────────────────────────────────────────────────────
FROM rust:1-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/

RUN cargo build --release --bin server

# ── Runtime stage ─────────────────────────────────────────────────────
FROM debian:bookworm-slim

# Install minimal runtime deps and create non-root user.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -s /bin/false kvr

# Create socket directory with restrictive permissions.
RUN mkdir -p /run/kvr && chown kvr:kvr /run/kvr && chmod 700 /run/kvr

# Copy the server binary.
COPY --from=builder /build/target/release/server /usr/local/bin/kvr-server

# Switch to non-root user.
USER kvr

# Set default env vars.
ENV KVR_SOCKET_PATH=/run/kvr/kvr.sock
ENV KVR_MAX_ENTRIES=100000
ENV KVR_MAX_CONNECTIONS=256

# Expose the socket directory as a volume.
VOLUME ["/run/kvr"]

# Healthcheck: use the built-in --ping client mode to connect via UDS
# and send a PING. The server responds with 0x10 (OK) for a successful PING.
# This replaces the broken nc/printf/grep approach (nc not installed, dash
# printf doesn't support \xHH, grep \x10 is locale-dependent).
HEALTHCHECK --interval=30s --timeout=5s --start-period=2s --retries=3 \
    CMD /usr/local/bin/kvr-server --ping "$KVR_SOCKET_PATH"

# Run the server.
ENTRYPOINT ["kvr-server"]
