# blob-archiver-rs
This is a Rust implementation of
the [Beacon Chain blob archiver](https://github.com/base/blob-archiver)

The Blob Archiver is a service to archive and allow querying of all historical blobs from the beacon chain. It consists
of two components:

* **Archiver** - Tracks the beacon chain and writes blobs to a storage backend
* **API** - Implements the blob sidecars [API](https://ethereum.github.io/beacon-APIs/#/Beacon/getBlobSidecars), which
  allows clients to retrieve blobs from the storage backend

### Storage
There are currently two supported storage options:

* On-disk storage - Blobs are written to disk in a directory
* S3 storage - Blobs are written to an S3 bucket (or compatible service)

You can control which storage backend is used by setting the `STORAGE_TYPE` to
either `file` or `s3`.

The `s3` backend will also work with (for example) Google Cloud Storage buckets (instructions [here](https://medium.com/google-cloud/using-google-cloud-storage-with-minio-object-storage-c994fe4aab6b)).

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

#### Get blobs
After successfully starting the service, you can use the following command to obtain the blob:

- get blob by block id from api service:
```shell
# also there is support other type of block id, like: finalized,justified.
curl -X 'GET' 'http://localhost:8000/eth/v1/beacon/blob_sidecars/head' -H 'accept: application/json'
```

- get blob by slot number from api service:
```shell
curl -X 'GET' 'http://localhost:8000/eth/v1/beacon/blob_sidecars/7111008' -H 'accept: application/json'
```

##  Options

### `verbose`

<Tabs>
<TabItem value="Syntax" label="Syntax" default>

    ```shell
    --verbose=<VERBOSE>
    ```

</TabItem>

<TabItem value="Example" label="Example">

    ```shell
    --verbose=2
    ```

</TabItem>
</Tabs>

### `log_dir`

<Tabs>
<TabItem value="Syntax" label="Syntax" default>

    ```shell
    --log_dir=<LOG_DIR>
    ```

</TabItem>

<TabItem value="Example" label="Example">

    ```shell
    --log_dir=/var/log/blob-archiver
    ```

</TabItem>
</Tabs>

### `log_rotation`

<Tabs>
<TabItem value="Syntax" label="Syntax" default>

    ```shell
    --log_rotation=<LOG_ROTATION>
    ```

</TabItem>

<TabItem value="Example" label="Example">

    ```shell
    --log_rotation=DAILY
    ```

</TabItem>
</Tabs>

### `beacon_endpoint`

<Tabs>
<TabItem value="Syntax" label="Syntax" default>

    ```shell
    --beacon_endpoint=<BEACON_ENDPOINT>
    ```

</TabItem>

<TabItem value="Example" label="Example">

    ```shell
    --beacon_endpoint=http://localhost:5052
    ```

</TabItem>
</Tabs>

### `beacon_client_timeout`

<Tabs>
<TabItem value="Syntax" label="Syntax" default>

    ```shell
    --beacon_client_timeout=<BEACON_CLIENT_TIMEOUT>
    ```

</TabItem>

<TabItem value="Example" label="Example">

    ```shell
    --beacon_client_timeout=10
    ```

</TabItem>
</Tabs>

### `poll_interval`

<Tabs>
<TabItem value="Syntax" label="Syntax" default>

    ```shell
    --poll_interval=<POLL_INTERVAL>
    ```

</TabItem>

<TabItem value="Example" label="Example">

    ```shell
    --poll_interval=6
    ```

</TabItem>
</Tabs>

### `listen_addr`

<Tabs>
<TabItem value="Syntax" label="Syntax" default>

    ```shell
    --listen_addr=<LISTEN_ADDR>
    ```

</TabItem>

<TabItem value="Example" label="Example">

    ```shell
    --listen_addr=0.0.0.0:8000
    ```

</TabItem>
</Tabs>

### `origin_block`

<Tabs>
<TabItem value="Syntax" label="Syntax" default>

    ```shell
    --origin_block=<ORIGIN_BLOCK>
    ```

</TabItem>

<TabItem value="Example" label="Example">

    ```shell
    --origin_block="0x0"
    ```

</TabItem>
</Tabs>

### `storage_type`

<Tabs>
<TabItem value="Syntax" label="Syntax" default>

    ```shell
    --storage_type=<STORAGE_TYPE>
    ```

</TabItem>

<TabItem value="Example" label="Example">

    ```shell
    --storage_type="s3"
    ```

</TabItem>
</Tabs>

### `s3_endpoint`

<Tabs>
<TabItem value="Syntax" label="Syntax" default>

    ```shell
    --s3_endpoint=<S3_ENDPOINT>
    ```

</TabItem>

<TabItem value="Example" label="Example">

    ```shell
    --s3_endpoint="http://localhost:9000"
    ```

</TabItem>
</Tabs>

### `s3_bucket`

<Tabs>
<TabItem value="Syntax" label="Syntax" default>

    ```shell
    --s3_bucket=<S3_BUCKET>
    ```

</TabItem>

<TabItem value="Example" label="Example">

    ```shell
    --s3_bucket="blobs"
    ```

</TabItem>
</Tabs>

### `s3_path`

<Tabs>
<TabItem value="Syntax" label="Syntax" default>

    ```shell
    --s3_path=<S3_PATH>
    ```

</TabItem>

<TabItem value="Example" label="Example">

    ```shell
    --s3_path=/blobs
    ```

</TabItem>
</Tabs>

### `s3_compress`

<Tabs>
<TabItem value="Syntax" label="Syntax" default>

    ```shell
    --s3_compress=<S3_COMPRESS>
    ```

</TabItem>

<TabItem value="Example" label="Example">

    ```shell
    --s3_compress=false
    ```

</TabItem>
</Tabs>

### `fs_dir`

<Tabs>
<TabItem value="Syntax" label="Syntax" default>

    ```shell
    --fs_dir=<FS_DIR>
    ```

</TabItem>

<TabItem value="Example" label="Example">

    ```shell
    --fs_dir=/blobs
    ```

</TabItem>
</Tabs>