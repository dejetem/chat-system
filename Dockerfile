# ---------- Builder ----------
FROM rust:1-bookworm AS builder

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY apps ./apps

# Build the chat-node binary using cargo build cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release -p chat-node && \
    cp target/release/chat-node /usr/local/bin/chat-node


# ---------- Runtime ----------
FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /usr/local/bin/chat-node .

EXPOSE 8080

CMD ["./chat-node"]