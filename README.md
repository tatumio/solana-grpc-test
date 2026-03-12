# grpc-test-tonic

Test client for validating Solana Yellowstone gRPC (Geyser) endpoints. Runs a suite of unary RPCs, streaming subscriptions, and edge-case tests, then reports pass/fail.

## Prerequisites

- Rust (1.85+)
- A Yellowstone gRPC endpoint and API key

## Usage

```bash
GRPC_ENDPOINT='https://solana-mainnet-grpc.gateway.tatum.io' \
  API_KEY='your-api-key' \
  cargo run
```

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `API_KEY` | yes | — | Auth token for the gRPC endpoint |
| `GRPC_ENDPOINT` | no | `https://solana-mainnet-grpc.gateway.tatum.io` | gRPC endpoint URL |
| `AUTH_HEADER` | no | `x-api-key` | HTTP header name for the token (e.g. `x-token` for Shyft) |
| `STREAM_TIMEOUT_SECS` | no | `30` | Timeout for streaming subscription tests |
| `BLOCK_TIMEOUT_SECS` | no | `120` | Timeout for full block reception |
| `KEEPALIVE_TIMEOUT_SECS` | no | `40` | How long the keepalive test runs |
| `SOAK_DURATION_SECS` | no | `0` (off) | Long-running stability test duration. Set to `7200` for 2 hours |

## Tests

Each test maps 1:1 to a function in `src/main.rs`.

| # | Test | What it does |
|---|---|---|
| 1 | **GetVersion** | Calls `GetVersion` RPC, logs the server version string |
| 2 | **Ping** | Calls `Ping` RPC with count=1, verifies the pong response matches |
| 3 | **GetLatestBlockhash** | Calls `GetLatestBlockhash` at all 3 commitment levels (processed, confirmed, finalized), logs slot + blockhash |
| 4 | **GetBlockHeight** | Calls `GetBlockHeight` at all 3 commitment levels |
| 5 | **GetSlot** | Calls `GetSlot` at all 3 commitment levels |
| 6 | **IsBlockhashValid** | Fetches a finalized blockhash, then validates it — checks the server says it's valid |
| 7 | **SubscribeReplayInfo** | Calls `SubscribeReplayInfo` RPC, logs the first available slot for replay |
| 8 | **Subscribe Slots** | Opens a `Subscribe` stream with a slots filter (confirmed), waits for 3 slot updates |
| 9 | **Subscribe Accounts** | Subscribes to the USDC mint account (`EPjFWdd5...`), waits for 1 account update, logs data length |
| 10 | **Subscribe Transactions** | Subscribes to non-vote, non-failed transactions involving the System Program, waits for 3 |
| 11 | **Subscribe Blocks Meta** | Subscribes to block metadata (confirmed), waits for 2, logs slot + blockhash + tx count |
| 12 | **Subscribe Full Block** | Subscribes to full finalized blocks with transactions included. Receives 1 block, logs its size in MB. Uses 128MB max decode size since blocks can be large |
| 13 | **Subscribe Deshred** | Opens a `SubscribeDeshred` stream (the second streaming RPC). Deshred = reassembled-from-shreds transactions, available before block confirmation. Lowest-latency tx feed. Gracefully skips if the server doesn't support it |
| 14 | **Keepalive** | Subscribes to slots for 40s. The server sends pings every ~15s — this test verifies we reply correctly and the connection stays alive through multiple ping/pong cycles |
| 15 | **Re-subscribe** | Starts with a slots subscription, then sends a new `SubscribeRequest` on the same stream switching to blocks_meta. Verifies the old filter stops and the new one works |
| 16 | **Unsubscribe** | Subscribes to slots, then sends empty filters (unsubscribe). Stays connected for 20s verifying the connection remains alive via ping/pong but no data arrives |
| 17 | **Back Pressure** | Subscribes to transactions but reads slowly (3s sleep between reads). Verifies the proxy doesn't kill the connection when the client can't keep up |
| 18 | **Soak** | Long-running stability test (off by default). Subscribes to slots + transactions + blocks_meta simultaneously for the configured duration. Prints per-minute stats: message counts, slot gaps, ping/pong health, errors |
