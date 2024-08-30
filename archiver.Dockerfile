# Use the official Rust image as the base
FROM rust:latest as builder

# Set the working directory
WORKDIR /usr/src/blob-archiver-rs

# Copy the entire project
COPY . .

# Build the project
RUN cargo build --release

# Create a new stage with a minimal image
FROM debian:buster-slim

# Install OpenSSL
RUN apt-get update && apt-get install -y openssl ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the built binary from the builder stage
COPY --from=builder /usr/src/blob-archiver-rs/target/release/archiver /usr/local/bin/archiver

# Set the entrypoint
ENTRYPOINT ["archiver"]
