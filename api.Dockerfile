# Use the official Rust image as the base
FROM rust:latest as builder

RUN apt-get update && apt-get install -y cmake

# Set the working directory
WORKDIR /usr/src/blob-archiver-rs

# Copy the entire project
COPY . .

# Build the project
RUN cargo build --release -p api

# Create a new stage with a minimal image
FROM ubuntu:latest

# Install OpenSSL
RUN apt-get update && apt-get install -y openssl ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the built binary from the builder stage
COPY --from=builder /usr/src/blob-archiver-rs/target/release/api /usr/local/bin/api

# Set the entrypoint
ENTRYPOINT ["api"]
