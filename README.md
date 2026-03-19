# solana-grpc-test

Test suite for validating Solana Yellowstone gRPC (Geyser) endpoints. Tests all RPCs, streaming subscriptions, and edge cases, then reports pass/fail.

## Install

```bash
cargo install --path .
```

This builds the binary and puts it in `~/.cargo/bin/solana-grpc-test`. Make sure `~/.cargo/bin` is in your `PATH` (it is by default with rustup).

If you just want to run from source without installing, use `cargo run --` instead of `solana-grpc-test` in any command below.

## Prerequisites

- Rust (1.85+)
- A Yellowstone gRPC endpoint and API key

## Usage

```bash
# Run all tests
solana-grpc-test -k <api-key>

# Custom endpoint
solana-grpc-test -e https://your-endpoint.com -k <api-key>

# Custom endpoint with different auth header (e.g. x-token instead of x-api-key)
solana-grpc-test -e https://your-endpoint.com -H x-token -k <api-key>

# Run only unary RPCs
solana-grpc-test -k <api-key> unary

# Run only streaming tests
solana-grpc-test -k <api-key> stream

# Run specific tests
solana-grpc-test -k <api-key> run version ping slots

# 2-hour soak test
solana-grpc-test -k <api-key> soak 7200

# Stress test: 20 connections × 10 streams each = 200 total
solana-grpc-test -k <api-key> stress 20 10

# Compare latency between two endpoints (60s)
solana-grpc-test latency \
  "tatum,https://solana-mainnet-grpc.gateway.tatum.dev,x-api-key,YOUR_KEY" \
  "shyft,https://grpc.us.shyft.to:443,x-token,YOUR_KEY"

# Latency comparison for 120 seconds
solana-grpc-test latency -d 120 \
  "tatum,https://solana-mainnet-grpc.gateway.tatum.dev,x-api-key,KEY1" \
  "shyft,https://grpc.us.shyft.to:443,x-token,KEY2"

# List all available tests
solana-grpc-test list

# Billing analysis: count all requests/responses and compute billing events
solana-grpc-test -k <api-key> billing

# Billing with custom stream message count (default: 5)
solana-grpc-test -k <api-key> billing --messages 10
```

With `cargo run`:
```bash
cargo run -- -k <api-key>
cargo run -- -k <api-key> unary
cargo run -- -k <api-key> run version ping
cargo run -- -k <api-key> soak 300
```

All options can also be set via environment variables:
```bash
API_KEY=<key> GRPC_ENDPOINT=https://your-endpoint.com cargo run
```

## Options

| Flag | Env var | Default | Description |
|---|---|---|---|
| `-k, --api-key` | `API_KEY` | (required) | Auth token |
| `-e, --endpoint` | `GRPC_ENDPOINT` | `https://solana-mainnet-grpc.gateway.tatum.io` | gRPC endpoint URL |
| `-H, --auth-header` | `AUTH_HEADER` | `x-api-key` | Header name for the token (e.g. `x-token` for Shyft) |
| `--stream-timeout` | `STREAM_TIMEOUT_SECS` | `30` | Timeout for streaming tests (seconds) |
| `--block-timeout` | `BLOCK_TIMEOUT_SECS` | `120` | Timeout for full block reception (seconds) |
| `--keepalive-timeout` | `KEEPALIVE_TIMEOUT_SECS` | `40` | Keepalive test duration (seconds) |
| `--stress-connections` | `STRESS_CONNECTIONS` | `10` | Number of TCP connections for stress test |
| `--stress-streams` | `STRESS_STREAMS` | `10` | gRPC streams per connection for stress test |

## Subcommands

| Command | Description |
|---|---|
| _(none)_ / `all` | Run all tests |
| `unary` | Run unary RPC tests only |
| `stream` | Run streaming subscription tests only |
| `advanced` | Run advanced stream tests only |
| `run <test>...` | Run specific test(s) by name |
| `soak [seconds]` | Long-running soak test (default: 7200s = 2 hours) |
| `stress [connections] [streams-per-conn]` | Open many concurrent streams to find the server limit (default: 10×10) |
| `goaway [seconds]` | Ping flood on single connection to detect GOAWAY (default: 7200s = 2 hours) |
| `latency [-d secs] <target>...` | Compare slot/block delivery latency between endpoints. Target format: `name,url,header,key` |
| `billing [-m N]` | Count all requests and responses across every RPC type and compute billing events (default: 5 stream messages per subscription) |
| `list` | List all available test names |

## Tests

Each test maps 1:1 to a function in `src/main.rs`.

| # | Name | What it does |
|---|---|---|
| 1 | `version` | Calls `GetVersion` RPC, logs the server version string |
| 2 | `ping` | Calls `Ping` RPC with count=1, verifies the pong response matches |
| 3 | `blockhash` | Calls `GetLatestBlockhash` at all 3 commitment levels (processed, confirmed, finalized), logs slot + blockhash |
| 4 | `block-height` | Calls `GetBlockHeight` at all 3 commitment levels |
| 5 | `slot` | Calls `GetSlot` at all 3 commitment levels |
| 6 | `blockhash-valid` | Fetches a finalized blockhash, then validates it — checks the server says it's valid |
| 7 | `replay-info` | Calls `SubscribeReplayInfo` RPC, logs the first available slot for replay |
| 8 | `slots` | Opens a `Subscribe` stream with a slots filter (confirmed), waits for 3 slot updates |
| 9 | `accounts` | Subscribes to the USDC mint account (`EPjFWdd5...`), waits for 1 account update, logs data length |
| 10 | `transactions` | Subscribes to non-vote, non-failed transactions involving the System Program, waits for 3 |
| 11 | `blocks-meta` | Subscribes to block metadata (confirmed), waits for 2, logs slot + blockhash + tx count |
| 12 | `blocks` | Subscribes to full finalized blocks with transactions included. Receives 1 block, logs its size in MB. Uses 128MB max decode size since blocks can be large |
| 13 | `deshred` | Opens a `SubscribeDeshred` stream (the second streaming RPC). Deshred = reassembled-from-shreds transactions, available before block confirmation. Lowest-latency tx feed. Gracefully skips if the server doesn't support it |
| 14 | `keepalive` | Subscribes to slots for 40s. The server sends pings every ~15s — this test verifies we reply correctly and the connection stays alive through multiple ping/pong cycles |
| 15 | `resubscribe` | Starts with a slots subscription, then sends a new `SubscribeRequest` on the same stream switching to blocks_meta. Verifies the old filter stops and the new one works |
| 16 | `unsubscribe` | Subscribes to slots, then sends empty filters (unsubscribe). Stays connected for 20s verifying the connection remains alive via ping/pong but no data arrives |
| 17 | `backpressure` | Subscribes to transactions but reads slowly (3s sleep between reads). Verifies the proxy doesn't kill the connection when the client can't keep up |
| 18 | `streams` | Opens C connections × S streams each (all subscribing to slots) to find both the connection limit and per-connection stream limit. Verifies all streams receive data. Use `stress <connections> <streams-per-conn>` subcommand or `--stress-connections` / `--stress-streams` flags |
| 19 | `soak` | Long-running stability test. Subscribes to slots + transactions + blocks_meta simultaneously for the configured duration. Prints per-minute stats: message counts, slot gaps, ping/pong health, errors |
| 20 | `goaway` | Sends 1 gRPC ping/sec on a single connection for 2+ hours. Monitors for HTTP/2 GOAWAY frames, stream resets, or connection drops. Reports when/if the connection dies and whether GOAWAY was the cause |
