# ---------- Builder ----------
FROM rust:1.76 as builder

WORKDIR /app

# Copy workspace files
COPY Cargo.toml .
COPY crates ./crates
COPY apps ./apps

# Build the chat-node binary
RUN cargo build --release -p chat-node


# ---------- Runtime ----------
FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /app/target/release/chat-node .

EXPOSE 8080

CMD ["./chat-node"]