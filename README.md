# blob-archiver-rs
This is a Rust implementation of
the [Beacon Chain blob archiver](https://github.com/base-org/blob-archiver)

### Development
```sh
# Run the tests
cargo test --workspace --all-features --all-targets --locked

# Lint the project
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Build the project
cargo build --workspace --all-targets --all-features

```

#### Run Locally
To run the project locally, you should first copy `.env.template` to `.env` and then modify the environment variables
to your beacon client and storage backend of choice. Then you can run the project with:

```sh
docker compose up
```
