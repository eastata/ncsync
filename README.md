# ncsync

One-way (server → local) Nextcloud mirror, built for many small files, low
bandwidth and Raspberry Pi class hardware. Standard library only, no CGO.

## How it works

Nextcloud changes the etag of every parent directory when any file changes.

1. Each pass starts with **one** `PROPFIND Depth:0` on the root. Same etag as last
   time → nothing to do, pass ends.
2. Otherwise it walks down, listing (`Depth:1`) **only directories whose etag
   changed**, in parallel, and downloads only changed files (bounded worker pool,
   HTTP/2 keep-alive, gzip for XML).
3. A directory's etag is stored **only after everything below it succeeded**, so
   failures and crashes are retried automatically on the next pass.
4. Downloads go to `.<name>.~<hex>` temp files (a pattern the desktop client
   ignores), are verified (size and SHA256/SHA1/MD5 when the server has one),
   get the server mtime, then are renamed into place. No per-file fsync.

## Run locally (to test)

```bash
export NC_URL=https://cloud.example.com
export NC_USER=your-login
export NC_APP_PASSWORD=xxxxx-xxxxx-xxxxx-xxxxx-xxxxx   # Settings > Security
export NC_DEST=$PWD/data NC_STATE=$PWD/state/ncsync.state
export NC_ONCE=true NC_LOG_LEVEL=debug NC_REMOTE_PATH=SomeSmallFolder

go run ./cmd/ncsync          # first pass
go run ./cmd/ncsync          # second pass: should log "no changes"
```

Tip: start with `NC_REMOTE_PATH` pointing at a small folder.

## Run in Docker (Raspberry Pi)

```bash
cp .env.example .env && nano .env
mkdir -p data state && sudo chown 1000:1000 data state
docker compose up -d --build
docker compose logs -f
```

Cross-build on a PC for a Pi: `docker buildx build --platform linux/arm64 -t ncsync .`
(`linux/arm/v7` for 32-bit OS).

## Tests

```bash
go vet ./... && go test -race ./...
```

The tests run the real client and syncer against an in-process mock Nextcloud
WebDAV server (initial sync, no-change pass = exactly 1 request, minimal
re-listing, deletions, failure/retry, E2EE skipping, state-file crash safety).

## Behaviour notes

- **Auth:** use an app password; it works with TOTP enabled.
- **End-to-end encrypted folders are skipped** (`NC_SKIP_E2EE=true`). Detection
  uses the `nc:is-encrypted` property. Verify it on your server:
  ```bash
  curl -s -u USER:APP_PW -X PROPFIND -H 'Depth: 1' \
    --data '<?xml version="1.0"?><d:propfind xmlns:d="DAV:" xmlns:nc="http://nextcloud.org/ns"><d:prop><nc:is-encrypted/></d:prop></d:propfind>' \
    https://HOST/remote.php/dav/files/USER/ | grep -i encrypted
  ```
- **Local drift:** a file deleted/modified locally is repaired only when its
  folder is re-listed (i.e. when something in that folder changes on the server).
- **Local-only files** are never uploaded and never touched.
- **Not yet desktop-client compatible:** the state is ncsync's own file, not the
  desktop client's `.sync_*.db` journal. See "Next steps".
- State is held in RAM (about 150-250 bytes per entry): fine for a few hundred
  thousand files on a Pi. For millions, swap `internal/state` for SQLite/bbolt.

## Layout

```
cmd/ncsync/main.go            entrypoint, poll loop, signals
internal/config/config.go     env var parsing + validation
internal/dav/client.go        PROPFIND/GET client, retries, idle timeouts
internal/state/store.go       append-only state log + in-memory index
internal/state/lock_*.go      single-instance flock
internal/syncer/syncer.go     etag-driven parallel one-way sync
```

## Next steps

1. `notify_push` websocket listener to replace polling (near-instant, zero idle traffic).
2. Desktop-client compatible `.sync_*.db` journal (needs a port of the client's path hash).
3. `Range` resume for very large files; periodic local verify pass.
