# ATL Server Benchmark - All-in-One
# Runs benchmark and outputs results in one command:
#   docker run --rm ghcr.io/evidentum-io/atl-benchmark

FROM rustlang/rust:nightly-bullseye AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Clone repositories
RUN git clone https://github.com/evidentum-io/atl-core.git && \
    git clone https://github.com/evidentum-io/atl-server.git

# Build atl-server
WORKDIR /build/atl-server
RUN cargo build --release

# Build wrk
WORKDIR /build
RUN git clone https://github.com/wg/wrk.git && \
    cd wrk && \
    make -j$(nproc)

# Runtime image
FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y \
    libssl1.1 \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binaries
COPY --from=builder /build/atl-server/target/release/atl-server /usr/local/bin/
COPY --from=builder /build/wrk/wrk /usr/local/bin/

# Create wrk script inline (avoids dependency on repo having it)
RUN cat > /app/post.lua << 'EOF'
-- wrk POST script for atl-server stress test
wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"

counter = 0

request = function()
    counter = counter + 1
    local body = string.format('{"payload":"wrk-test-%d","metadata":{"seq":%d}}', counter, counter)
    return wrk.format(nil, nil, nil, body)
end
EOF

# Generate signing key at build time
RUN head -c 32 /dev/urandom > /app/signing.key

# Copy entrypoint
COPY benchmark/entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

ENV RUST_LOG=error
ENV ATL_SERVER_PORT=3388
ENV ATL_DB_PATH=/tmp/atl.db
ENV ATL_SIGNING_KEY_PATH=/app/signing.key
ENV ATL_SERVER_HOST=127.0.0.1

ENTRYPOINT ["/app/entrypoint.sh"]
