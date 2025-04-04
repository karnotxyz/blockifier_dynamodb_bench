# Builder stage
FROM rust:1.85 as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a new empty shell project
WORKDIR /app
COPY . .

# Build the project
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/target/release/blockifier_dynamodb_bench /app/blockifier_dynamodb_bench

# Copy resources folder from cargo dependencies
# Hacky but does the job for now :)
COPY --from=builder /usr/local/cargo/git/checkouts/sequencer-77397f56b8742484/e8dd07f/crates/blockifier_test_utils/resources /usr/local/cargo/git/checkouts/sequencer-77397f56b8742484/e8dd07f/crates/blockifier_test_utils/resources

# Set the startup command
CMD ["./blockifier_dynamodb_bench"]
