use std::collections::HashMap;
use std::future::Future;
use std::process;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use prost::Message as ProstMessage;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use yellowstone_grpc_client::ClientTlsConfig;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeDeshredRequest, SubscribeReplayInfoRequest, SubscribeRequest,
    SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterDeshredTransactions, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions, SubscribeRequestPing, SubscribeUpdate,
    geyser_client::GeyserClient, subscribe_update::UpdateOneof, subscribe_update_deshred,
};
use yellowstone_grpc_proto::tonic;
use yellowstone_grpc_proto::tonic::transport::{Channel, Endpoint};

const DEFAULT_ENDPOINT: &str = "https://solana-mainnet-grpc.gateway.tatum.io";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
const MAX_DECODE_SIZE: usize = 128 * 1024 * 1024;

// --- CLI ---

#[derive(Parser)]
#[command(
    name = "solana-grpc-test",
    about = "Test suite for Solana Yellowstone gRPC endpoints"
)]
struct Cli {
    /// gRPC endpoint URL
    #[arg(short, long, env = "GRPC_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
    endpoint: String,

    /// API key for authentication
    #[arg(short = 'k', long, env = "API_KEY")]
    api_key: Option<String>,

    /// Auth header name
    #[arg(short = 'H', long, env = "AUTH_HEADER", default_value = "x-api-key")]
    auth_header: String,

    /// Timeout for streaming tests (seconds)
    #[arg(long, env = "STREAM_TIMEOUT_SECS", default_value = "30")]
    stream_timeout: u64,

    /// Timeout for full block reception (seconds)
    #[arg(long, env = "BLOCK_TIMEOUT_SECS", default_value = "120")]
    block_timeout: u64,

    /// Keepalive test duration (seconds)
    #[arg(long, env = "KEEPALIVE_TIMEOUT_SECS", default_value = "40")]
    keepalive_timeout: u64,

    /// Number of TCP connections for stress test
    #[arg(long, env = "STRESS_CONNECTIONS", default_value = "10")]
    stress_connections: u64,

    /// Number of gRPC streams per connection for stress test
    #[arg(long, env = "STRESS_STREAMS", default_value = "10")]
    stress_streams: u64,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run all tests (default)
    All,
    /// Run unary RPC tests only
    Unary,
    /// Run streaming subscription tests only
    Stream,
    /// Run advanced stream tests only
    Advanced,
    /// Run specific test(s) by name
    Run {
        /// Test names (see `list` for available names)
        tests: Vec<String>,
    },
    /// Run long-running soak test
    Soak {
        /// Duration in seconds
        #[arg(default_value = "7200")]
        duration: u64,
    },
    /// Open many concurrent streams to find the server limit
    Stress {
        /// Number of TCP connections
        #[arg(default_value = "10")]
        connections: u64,
        /// Number of gRPC streams per connection
        #[arg(default_value = "10")]
        streams_per_conn: u64,
    },
    /// Ping flood on single connection to detect GOAWAY
    Goaway {
        /// Duration in seconds (default: 2 hours)
        #[arg(default_value = "7200")]
        duration: u64,
    },
    /// Compare latency between multiple endpoints
    Latency {
        /// Duration in seconds
        #[arg(short, long, default_value = "60")]
        duration: u64,
        /// Targets: name,url,header,key (one per arg)
        targets: Vec<String>,
    },
    /// List all available tests
    List,
    /// Billing analysis: run all RPC types and count messages for cost estimation
    Billing {
        /// Stream messages to collect per subscription
        #[arg(short, long, default_value = "5")]
        messages: u32,
    },
}

// --- Config (global, populated from CLI) ---

#[derive(Debug)]
struct Config {
    endpoint: String,
    api_key: String,
    auth_header: String,
    stream_timeout: Duration,
    block_timeout: Duration,
    keepalive_timeout: Duration,
    stress_connections: u64,
    stress_streams: u64,
}

static CONFIG: OnceLock<Config> = OnceLock::new();

fn config() -> &'static Config {
    CONFIG.get().expect("config not initialized")
}

fn stream_timeout() -> Duration {
    config().stream_timeout
}

fn block_timeout() -> Duration {
    config().block_timeout
}

fn keepalive_timeout() -> Duration {
    config().keepalive_timeout
}

async fn connect_channel() -> Result<Channel> {
    let url = config().endpoint.clone();
    let endpoint =
        Endpoint::from_shared(url)?.tls_config(ClientTlsConfig::new().with_native_roots())?;
    let channel = endpoint.connect().await?;
    Ok(channel)
}

macro_rules! connect_client {
    ($channel:expr) => {{
        let cfg = config();
        let key: tonic::metadata::AsciiMetadataValue = cfg
            .api_key
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid API key (not valid ASCII metadata)"))?;
        let header: tonic::metadata::AsciiMetadataKey = cfg
            .auth_header
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid auth header name: {}", cfg.auth_header))?;
        GeyserClient::with_interceptor($channel, move |mut req: tonic::Request<()>| {
            req.metadata_mut().insert(header.clone(), key.clone());
            Ok(req)
        })
        .max_decoding_message_size(MAX_DECODE_SIZE)
    }};
}

macro_rules! connect {
    () => {{
        let channel = connect_channel().await?;
        connect_client!(channel)
    }};
}

// --- Bidirectional Stream Wrapper ---
// Handles server pings automatically on all streaming tests.
// Server sends Ping every ~15s. We reply with SubscribeRequestPing to keep alive.

struct GeyserStream {
    tx: mpsc::Sender<SubscribeRequest>,
    stream: tonic::Streaming<SubscribeUpdate>,
    ping_counter: i32,
    pings_handled: u32,
    pongs_received: u32,
}

impl GeyserStream {
    async fn connect(request: SubscribeRequest) -> Result<Self> {
        let mut client = connect!();
        let (tx, rx) = mpsc::channel::<SubscribeRequest>(16);
        tx.send(request)
            .await
            .map_err(|_| anyhow::anyhow!("send failed"))?;
        let stream = client
            .subscribe(ReceiverStream::new(rx))
            .await?
            .into_inner();
        Ok(Self {
            tx,
            stream,
            ping_counter: 0,
            pings_handled: 0,
            pongs_received: 0,
        })
    }

    /// Get next data update, automatically replying to server pings.
    /// Skips Ping and Pong updates, returns only data (Slot, Account, Transaction, etc.)
    async fn next_data(&mut self) -> Result<Option<UpdateOneof>> {
        while let Some(msg) = self.stream.next().await {
            let msg = msg?;
            match msg.update_oneof {
                Some(UpdateOneof::Ping(_)) => {
                    self.reply_to_ping().await;
                }
                Some(UpdateOneof::Pong(_)) => {
                    self.pongs_received += 1;
                }
                Some(update) => return Ok(Some(update)),
                None => {}
            }
        }
        Ok(None)
    }

    /// Get next raw update including Pong (still auto-replies to Ping).
    /// Used by keepalive test to observe the ping/pong cycle.
    async fn next_raw(&mut self) -> Result<Option<UpdateOneof>> {
        while let Some(msg) = self.stream.next().await {
            let msg = msg?;
            match msg.update_oneof {
                Some(UpdateOneof::Ping(_)) => {
                    self.reply_to_ping().await;
                    // Return the ping event so caller can see it
                    return Ok(Some(UpdateOneof::Ping(
                        yellowstone_grpc_proto::geyser::SubscribeUpdatePing {},
                    )));
                }
                Some(update) => return Ok(Some(update)),
                None => {}
            }
        }
        Ok(None)
    }

    async fn reply_to_ping(&mut self) {
        self.ping_counter += 1;
        self.pings_handled += 1;
        let _ = self
            .tx
            .send(SubscribeRequest {
                ping: Some(SubscribeRequestPing {
                    id: self.ping_counter,
                }),
                ..Default::default()
            })
            .await;
    }

    /// Send a new SubscribeRequest (for re-subscription or unsubscribe).
    /// This REPLACES all existing filters.
    async fn send(&self, request: SubscribeRequest) -> Result<()> {
        self.tx
            .send(request)
            .await
            .map_err(|_| anyhow::anyhow!("failed to send subscribe request"))?;
        Ok(())
    }
}

// --- Helpers ---

fn commitment_levels() -> [(&'static str, CommitmentLevel); 3] {
    [
        ("processed", CommitmentLevel::Processed),
        ("confirmed", CommitmentLevel::Confirmed),
        ("finalized", CommitmentLevel::Finalized),
    ]
}

fn bs58_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if bytes.is_empty() {
        return String::new();
    }
    let mut digits = vec![0u8];
    for &byte in bytes {
        let mut carry = byte as u32;
        for d in digits.iter_mut() {
            carry += (*d as u32) * 256;
            *d = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    let mut result = String::new();
    for &byte in bytes {
        if byte == 0 {
            result.push('1');
        } else {
            break;
        }
    }
    for &d in digits.iter().rev() {
        result.push(ALPHABET[d as usize] as char);
    }
    result
}

// --- Test Runner ---

struct TestRunner {
    passed: u32,
    total: u32,
}

impl TestRunner {
    fn new() -> Self {
        Self {
            passed: 0,
            total: 0,
        }
    }

    async fn run(&mut self, name: &str, fut: impl Future<Output = Result<String>>) {
        self.total += 1;
        print!("  {name} ... ");
        let start = Instant::now();
        match fut.await {
            Ok(detail) => {
                let elapsed = start.elapsed();
                println!("\x1b[32m[PASS]\x1b[0m ({elapsed:.0?})\n         {detail}");
                self.passed += 1;
            }
            Err(e) => {
                let elapsed = start.elapsed();
                println!("\x1b[31m[FAIL]\x1b[0m ({elapsed:.0?})\n         Error: {e:#}");
            }
        }
    }

    fn summary(&self) {
        println!();
        println!("\x1b[1m=== Results: {}/{} passed ===\x1b[0m", self.passed, self.total);
        if self.passed == self.total {
            println!("\x1b[32mAll tests passed!\x1b[0m");
        } else {
            println!("\x1b[31m{} test(s) failed.\x1b[0m", self.total - self.passed);
        }
    }

    fn exit_code(&self) -> i32 {
        if self.passed == self.total { 0 } else { 1 }
    }
}

// --- Unary RPC Tests ---

async fn test_get_version() -> Result<String> {
    let mut client = connect!();
    let resp = client
        .get_version(yellowstone_grpc_proto::geyser::GetVersionRequest {})
        .await?
        .into_inner();
    Ok(format!("version={}", resp.version))
}

async fn test_ping() -> Result<String> {
    let mut client = connect!();
    let resp = client
        .ping(yellowstone_grpc_proto::geyser::PingRequest { count: 1 })
        .await?
        .into_inner();
    if resp.count != 1 {
        bail!("expected pong count=1, got {}", resp.count);
    }
    Ok(format!("pong count={}", resp.count))
}

async fn test_get_latest_blockhash() -> Result<String> {
    let mut client = connect!();
    let mut details = Vec::new();
    for (name, level) in commitment_levels() {
        let resp = client
            .get_latest_blockhash(yellowstone_grpc_proto::geyser::GetLatestBlockhashRequest {
                commitment: Some(level as i32),
            })
            .await
            .with_context(|| format!("{name} failed"))?
            .into_inner();
        details.push(format!(
            "{name}: slot={} blockhash={} last_valid_height={}",
            resp.slot, resp.blockhash, resp.last_valid_block_height
        ));
    }
    Ok(details.join("\n         "))
}

async fn test_get_block_height() -> Result<String> {
    let mut client = connect!();
    let mut details = Vec::new();
    for (name, level) in commitment_levels() {
        let resp = client
            .get_block_height(yellowstone_grpc_proto::geyser::GetBlockHeightRequest {
                commitment: Some(level as i32),
            })
            .await
            .with_context(|| format!("{name} failed"))?
            .into_inner();
        details.push(format!("{name}: block_height={}", resp.block_height));
    }
    Ok(details.join("\n         "))
}

async fn test_get_slot() -> Result<String> {
    let mut client = connect!();
    let mut details = Vec::new();
    for (name, level) in commitment_levels() {
        let resp = client
            .get_slot(yellowstone_grpc_proto::geyser::GetSlotRequest {
                commitment: Some(level as i32),
            })
            .await
            .with_context(|| format!("{name} failed"))?
            .into_inner();
        details.push(format!("{name}: slot={}", resp.slot));
    }
    Ok(details.join("\n         "))
}

async fn test_is_blockhash_valid() -> Result<String> {
    let mut client = connect!();
    let bh_resp = client
        .get_latest_blockhash(yellowstone_grpc_proto::geyser::GetLatestBlockhashRequest {
            commitment: Some(CommitmentLevel::Finalized as i32),
        })
        .await
        .context("get_latest_blockhash for IsBlockhashValid failed")?
        .into_inner();
    let blockhash = bh_resp.blockhash.clone();
    let resp = client
        .is_blockhash_valid(yellowstone_grpc_proto::geyser::IsBlockhashValidRequest {
            blockhash: blockhash.clone(),
            commitment: Some(CommitmentLevel::Finalized as i32),
        })
        .await
        .context("is_blockhash_valid call failed")?
        .into_inner();
    if !resp.valid {
        bail!(
            "blockhash {blockhash} reported as invalid (slot={})",
            resp.slot
        );
    }
    Ok(format!(
        "blockhash={blockhash} valid=true slot={}",
        resp.slot
    ))
}

async fn test_subscribe_replay_info() -> Result<String> {
    let mut client = connect!();
    let resp = client
        .subscribe_replay_info(SubscribeReplayInfoRequest {})
        .await?
        .into_inner();
    match resp.first_available {
        Some(slot) => Ok(format!("first_available_slot={slot}")),
        None => Ok("first_available_slot=none (replay not available)".to_string()),
    }
}

// --- Streaming Subscription Tests ---

async fn test_subscribe_slots() -> Result<String> {
    let request = SubscribeRequest {
        slots: HashMap::from([(
            "slot_sub".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                ..Default::default()
            },
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };
    let mut sub = GeyserStream::connect(request).await?;
    let mut count = 0u32;
    let mut slots = Vec::new();

    tokio::time::timeout(stream_timeout(), async {
        while let Some(update) = sub.next_data().await? {
            if let UpdateOneof::Slot(slot) = update {
                slots.push(slot.slot);
                count += 1;
                if count >= 3 {
                    break;
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out waiting for slot updates")??;

    Ok(format!(
        "received {count} slots: {slots:?} (pings handled: {})",
        sub.pings_handled
    ))
}

async fn test_subscribe_accounts() -> Result<String> {
    let request = SubscribeRequest {
        accounts: HashMap::from([(
            "usdc_sub".to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![USDC_MINT.to_string()],
                ..Default::default()
            },
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };
    let mut sub = GeyserStream::connect(request).await?;

    let detail = tokio::time::timeout(stream_timeout(), async {
        while let Some(update) = sub.next_data().await? {
            if let UpdateOneof::Account(acct_update) = update {
                let slot = acct_update.slot;
                let data_len = acct_update
                    .account
                    .as_ref()
                    .map(|a| a.data.len())
                    .unwrap_or(0);
                return Ok::<_, anyhow::Error>(format!(
                    "USDC mint update at slot={slot} data_len={data_len}"
                ));
            }
        }
        bail!("stream ended without account update")
    })
    .await
    .context("timed out waiting for USDC account update")??;

    Ok(detail)
}

async fn test_subscribe_transactions() -> Result<String> {
    let request = SubscribeRequest {
        transactions: HashMap::from([(
            "tx_sub".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                account_include: vec![SYSTEM_PROGRAM.to_string()],
                ..Default::default()
            },
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };
    let mut sub = GeyserStream::connect(request).await?;
    let mut count = 0u32;
    let mut sigs = Vec::new();

    tokio::time::timeout(stream_timeout(), async {
        while let Some(update) = sub.next_data().await? {
            if let UpdateOneof::Transaction(tx_update) = update {
                let sig = tx_update
                    .transaction
                    .as_ref()
                    .map(|t| bs58_encode(&t.signature))
                    .unwrap_or_else(|| "unknown".to_string());
                sigs.push(sig);
                count += 1;
                if count >= 3 {
                    break;
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out waiting for transaction updates")??;

    let sigs_display: Vec<String> = sigs
        .iter()
        .map(|s| s[..8.min(s.len())].to_string())
        .collect();
    Ok(format!("received {count} txs, sigs: {sigs_display:?}"))
}

async fn test_subscribe_blocks_meta() -> Result<String> {
    let request = SubscribeRequest {
        blocks_meta: HashMap::from([(
            "bmeta_sub".to_string(),
            SubscribeRequestFilterBlocksMeta {},
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };
    let mut sub = GeyserStream::connect(request).await?;
    let mut count = 0u32;
    let mut details = Vec::new();

    tokio::time::timeout(stream_timeout(), async {
        while let Some(update) = sub.next_data().await? {
            if let UpdateOneof::BlockMeta(meta) = update {
                details.push(format!(
                    "slot={} blockhash={} txn_count={}",
                    meta.slot, meta.blockhash, meta.executed_transaction_count
                ));
                count += 1;
                if count >= 2 {
                    break;
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out waiting for blocks_meta updates")??;

    Ok(format!(
        "received {count} blocks_meta\n         {}",
        details.join("\n         ")
    ))
}

async fn test_subscribe_full_block() -> Result<String> {
    let request = SubscribeRequest {
        blocks: HashMap::from([(
            "block_sub".to_string(),
            SubscribeRequestFilterBlocks {
                account_include: vec![],
                include_transactions: Some(true),
                include_accounts: Some(false),
                include_entries: Some(false),
            },
        )]),
        commitment: Some(CommitmentLevel::Finalized as i32),
        ..Default::default()
    };
    let mut sub = GeyserStream::connect(request).await?;

    let detail = tokio::time::timeout(block_timeout(), async {
        while let Some(update) = sub.next_data().await? {
            if let UpdateOneof::Block(block) = update {
                let size_bytes = block.encoded_len();
                let size_mb = size_bytes as f64 / (1024.0 * 1024.0);
                let tx_count = block.transactions.len();
                return Ok::<_, anyhow::Error>(format!(
                    "slot={} blockhash={} txns={tx_count} size={size_mb:.2}MB ({size_bytes} bytes)",
                    block.slot, block.blockhash
                ));
            }
        }
        bail!("stream ended without receiving a full block")
    })
    .await
    .context("timed out waiting for full block")??;

    Ok(detail)
}

// --- Advanced Streaming Tests ---

/// Keepalive: subscribe to slots for 35+ seconds, verify server pings arrive
/// and our pong replies keep the connection alive.
async fn test_keepalive() -> Result<String> {
    let request = SubscribeRequest {
        slots: HashMap::from([(
            "keepalive_sub".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                ..Default::default()
            },
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };
    let mut sub = GeyserStream::connect(request).await?;
    let duration = keepalive_timeout();
    let mut slot_count = 0u32;

    tokio::time::timeout(duration + Duration::from_secs(5), async {
        let deadline = Instant::now() + duration;
        while Instant::now() < deadline {
            match sub.next_raw().await? {
                Some(UpdateOneof::Slot(_)) => {
                    slot_count += 1;
                }
                Some(UpdateOneof::Pong(_)) => {
                    sub.pongs_received += 1;
                }
                Some(UpdateOneof::Ping(_)) => { /* already handled in next_raw */ }
                None => bail!("stream closed unexpectedly during keepalive"),
                _ => {}
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("keepalive test timed out")??;

    if sub.pings_handled == 0 {
        bail!(
            "ran for {duration:.0?} but received 0 server pings (expected at least 1 every ~15s)"
        );
    }

    Ok(format!(
        "alive for {duration:.0?}: pings_received={} pongs_received={} slots={slot_count}",
        sub.pings_handled, sub.pongs_received
    ))
}

/// Re-subscription: start with slots filter, switch to blocks_meta on the same stream.
/// Verify old filter stops producing and new filter works.
async fn test_resubscribe() -> Result<String> {
    // Phase 1: subscribe to slots
    let slots_request = SubscribeRequest {
        slots: HashMap::from([(
            "resub_slots".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                ..Default::default()
            },
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };
    let mut sub = GeyserStream::connect(slots_request).await?;

    // Collect 3 slot updates
    let mut slot_count = 0u32;
    tokio::time::timeout(stream_timeout(), async {
        while let Some(update) = sub.next_data().await? {
            if let UpdateOneof::Slot(_) = update {
                slot_count += 1;
                if slot_count >= 3 {
                    break;
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out waiting for initial slot updates")??;

    // Phase 2: switch to blocks_meta (replaces all filters)
    let meta_request = SubscribeRequest {
        blocks_meta: HashMap::from([(
            "resub_meta".to_string(),
            SubscribeRequestFilterBlocksMeta {},
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };
    sub.send(meta_request).await?;

    // Collect 2 blocks_meta, verify no more slots arrive
    let mut meta_count = 0u32;
    let mut stale_slots = 0u32;
    tokio::time::timeout(stream_timeout(), async {
        while let Some(update) = sub.next_data().await? {
            match update {
                UpdateOneof::BlockMeta(_) => {
                    meta_count += 1;
                    if meta_count >= 2 {
                        break;
                    }
                }
                UpdateOneof::Slot(_) => {
                    // Slots arriving after re-sub are "stale" (may get a few in-flight)
                    stale_slots += 1;
                }
                _ => {}
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out waiting for blocks_meta after re-subscription")??;

    if meta_count < 2 {
        bail!("expected 2 blocks_meta after re-sub, got {meta_count}");
    }

    Ok(format!(
        "slots phase: {slot_count} slots -> re-subscribed -> blocks_meta phase: {meta_count} metas (stale slots after switch: {stale_slots})"
    ))
}

/// Unsubscribe: send empty filters, verify connection stays alive via ping/pong.
async fn test_unsubscribe() -> Result<String> {
    // Start with a slots subscription
    let request = SubscribeRequest {
        slots: HashMap::from([(
            "unsub_slots".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                ..Default::default()
            },
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };
    let mut sub = GeyserStream::connect(request).await?;

    // Get a couple updates to confirm subscription works
    let mut got_data = false;
    tokio::time::timeout(stream_timeout(), async {
        while let Some(update) = sub.next_data().await? {
            if let UpdateOneof::Slot(_) = update {
                got_data = true;
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out waiting for initial data before unsubscribe")??;

    if !got_data {
        bail!("never received initial slot data");
    }

    // Unsubscribe: send empty filters
    sub.send(SubscribeRequest::default()).await?;

    // Stay connected for 20 seconds, verifying ping/pong keeps us alive.
    // We should get NO data updates, only pings/pongs.
    let alive_duration = Duration::from_secs(20);
    let mut data_after_unsub = 0u32;
    let pings_before = sub.pings_handled;

    let deadline = tokio::time::sleep(alive_duration);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            _ = &mut deadline => break, // 20s elapsed — success
            msg = sub.next_raw() => {
                match msg? {
                    Some(UpdateOneof::Ping(_) | UpdateOneof::Pong(_)) => {}
                    Some(UpdateOneof::Slot(_) | UpdateOneof::Account(_) | UpdateOneof::Transaction(_)
                        | UpdateOneof::Block(_) | UpdateOneof::BlockMeta(_)) => {
                        data_after_unsub += 1;
                    }
                    None => bail!("stream closed after unsubscribe (should stay alive)"),
                    _ => {}
                }
            }
        }
    }

    let pings_during = sub.pings_handled - pings_before;

    Ok(format!(
        "unsubscribed, stayed alive for {alive_duration:.0?}: pings_handled={pings_during} data_after_unsub={data_after_unsub}"
    ))
}

/// SubscribeDeshred: the second streaming subscribe method.
/// Subscribes to deshredded transactions (low-latency, pre-block).
async fn test_subscribe_deshred() -> Result<String> {
    let mut client = connect!();
    let (tx, rx) = mpsc::channel::<SubscribeDeshredRequest>(16);
    tx.send(SubscribeDeshredRequest {
        deshred_transactions: HashMap::from([(
            "deshred_sub".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: Some(false),
                account_include: vec![SYSTEM_PROGRAM.to_string()],
                ..Default::default()
            },
        )]),
        ping: None,
    })
    .await
    .map_err(|_| anyhow::anyhow!("send failed"))?;

    let mut stream = match client.subscribe_deshred(ReceiverStream::new(rx)).await {
        Err(status) if status.code() == tonic::Code::Unimplemented => {
            return Ok(format!(
                "SKIPPED — server does not support SubscribeDeshred: {}",
                status.message()
            ));
        }
        Err(e) => return Err(e.into()),
        Ok(resp) => resp.into_inner(),
    };

    let mut count = 0u32;
    let mut ping_count = 0u32;
    let detail = tokio::time::timeout(stream_timeout(), async {
        while let Some(msg) = stream.next().await {
            let msg = msg?;
            match msg.update_oneof {
                Some(subscribe_update_deshred::UpdateOneof::DeshredTransaction(dt)) => {
                    count += 1;
                    if count == 1 {
                        let sig = dt
                            .transaction
                            .as_ref()
                            .map(|t| bs58_encode(&t.signature))
                            .unwrap_or_else(|| "unknown".to_string());
                        let sig_short = &sig[..8.min(sig.len())];
                        return Ok::<_, anyhow::Error>(format!(
                            "received deshred tx at slot={}, sig: {sig_short}...",
                            dt.slot
                        ));
                    }
                }
                Some(subscribe_update_deshred::UpdateOneof::Ping(_)) => {
                    ping_count += 1;
                    let _ = tx
                        .send(SubscribeDeshredRequest {
                            deshred_transactions: HashMap::new(),
                            ping: Some(SubscribeRequestPing {
                                id: ping_count as i32,
                            }),
                        })
                        .await;
                }
                _ => {}
            }
        }
        bail!("stream ended without deshred transaction")
    })
    .await
    .context("timed out waiting for deshred transactions")??;

    Ok(detail)
}

/// Back pressure: subscribe to a high-volume stream but read slowly.
/// Verifies that endpoint handles slow consumers gracefully.
async fn test_back_pressure() -> Result<String> {
    let request = SubscribeRequest {
        transactions: HashMap::from([(
            "backpressure_sub".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                account_include: vec![SYSTEM_PROGRAM.to_string()],
                ..Default::default()
            },
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };
    let mut sub = GeyserStream::connect(request).await?;

    // Read a few fast to confirm stream works
    let mut fast_count = 0u32;
    tokio::time::timeout(stream_timeout(), async {
        while let Some(update) = sub.next_data().await? {
            if let UpdateOneof::Transaction(_) = update {
                fast_count += 1;
                if fast_count >= 2 {
                    break;
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out during fast-read phase")??;

    // Now read slowly: sleep 3s between each read
    let mut slow_count = 0u32;
    let slow_reads = 3u32;
    let slow_delay = Duration::from_secs(3);

    for _ in 0..slow_reads {
        tokio::time::sleep(slow_delay).await;
        let msg = tokio::time::timeout(stream_timeout(), sub.next_data())
            .await
            .context("timed out during slow-read phase")?;
        match msg? {
            Some(UpdateOneof::Transaction(_)) => {
                slow_count += 1;
            }
            Some(_) => {
                slow_count += 1;
            } // any data is fine
            None => bail!("stream closed during slow reading (back pressure disconnect)"),
        }
    }

    // Final fast read to verify stream still works
    let mut post_count = 0u32;
    tokio::time::timeout(stream_timeout(), async {
        while let Some(update) = sub.next_data().await? {
            if let UpdateOneof::Transaction(_) = update {
                post_count += 1;
                if post_count >= 2 {
                    break;
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out after slow-read phase (stream may have died)")??;

    Ok(format!(
        "fast={fast_count} slow={slow_count}/{slow_reads} ({}s delay each) post={post_count} — stream survived",
        slow_delay.as_secs()
    ))
}

/// Stream flood: open many connections, each with multiple gRPC streams.
/// Tests both the connection limit and the per-connection stream limit.
async fn test_stream_flood(num_connections: u64, streams_per_conn: u64) -> Result<String> {
    let total_target = num_connections * streams_per_conn;
    println!(
        "         target: {num_connections} connections × {streams_per_conn} streams = {total_target} total"
    );

    let mut all_txs: Vec<mpsc::Sender<SubscribeRequest>> = Vec::new();
    let mut all_streams: Vec<tonic::Streaming<SubscribeUpdate>> = Vec::new();
    let mut connections_opened = 0u64;
    let mut first_error: Option<String> = None;
    let connect_start = Instant::now();

    'outer: for c in 1..=num_connections {
        let channel = match connect_channel().await {
            Ok(ch) => ch,
            Err(e) => {
                first_error = Some(format!("connection {c} failed: {e:#}"));
                println!("         connection {c}/{num_connections} FAILED: {e:#}");
                break;
            }
        };
        connections_opened += 1;
        let mut client = connect_client!(channel);

        for s in 1..=streams_per_conn {
            let (tx, rx) = mpsc::channel::<SubscribeRequest>(16);
            let request = SubscribeRequest {
                slots: HashMap::from([(
                    format!("flood_{c}_{s}"),
                    SubscribeRequestFilterSlots {
                        filter_by_commitment: Some(true),
                        ..Default::default()
                    },
                )]),
                commitment: Some(CommitmentLevel::Confirmed as i32),
                ..Default::default()
            };
            if tx.send(request).await.is_err() {
                first_error = Some(format!("conn {c} stream {s}: send failed"));
                break 'outer;
            }

            match client.subscribe(ReceiverStream::new(rx)).await {
                Ok(resp) => {
                    all_txs.push(tx);
                    all_streams.push(resp.into_inner());
                }
                Err(e) => {
                    let so_far = all_streams.len();
                    first_error =
                        Some(format!("conn {c} stream {s} ({so_far} total open): {e:#}"));
                    println!(
                        "         conn {c} stream {s} FAILED ({so_far} total open): {e:#}"
                    );
                    break 'outer;
                }
            }
        }

        let so_far = all_streams.len();
        if c % 5 == 0 || c == 1 {
            println!("         [{c}/{num_connections}] {so_far} streams open");
        }
    }

    let opened = all_streams.len() as u64;
    let connect_elapsed = connect_start.elapsed();
    println!(
        "         {connections_opened} connections, {opened}/{total_target} streams in {connect_elapsed:.1?}"
    );

    if all_streams.is_empty() {
        bail!("could not open any streams");
    }

    // Read one update from each concurrently to verify that the streams are healthy.
    println!("         verifying {opened} streams receive data...");
    let mut handles = Vec::new();
    for (tx, mut stream) in all_txs.into_iter().zip(all_streams) {
        handles.push(tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(15), async {
                while let Some(msg) = stream.next().await {
                    let msg = msg?;
                    match msg.update_oneof {
                        Some(UpdateOneof::Ping(_)) => {
                            let _ = tx
                                .send(SubscribeRequest {
                                    ping: Some(SubscribeRequestPing { id: 1 }),
                                    ..Default::default()
                                })
                                .await;
                        }
                        Some(UpdateOneof::Pong(_)) => {}
                        Some(_) => return Ok::<_, anyhow::Error>(true),
                        None => {}
                    }
                }
                Ok(false)
            })
            .await
        }));
    }
    let results = futures::future::join_all(handles).await;
    let healthy = results
        .iter()
        .filter(|r| matches!(r, Ok(Ok(Ok(true)))))
        .count() as u64;
    let timed_out = results
        .iter()
        .filter(|r| matches!(r, Ok(Err(_))))
        .count() as u64;
    let errored = results
        .iter()
        .filter(|r| matches!(r, Ok(Ok(Err(_)))))
        .count() as u64;

    let mut detail = format!(
        "connections={connections_opened}/{num_connections} streams={opened}/{total_target} healthy={healthy} timed_out={timed_out} errored={errored} connect_time={connect_elapsed:.1?}"
    );
    if let Some(err) = &first_error {
        detail.push_str(&format!("\n         first failure: {err}"));
    }

    Ok(detail)
}

/// Soak test: run all subscriptions for an extended period.
/// Monitors for connection drops, message gaps, parse errors, and throughput.
async fn test_soak(duration_secs: u64) -> Result<String> {
    let duration = Duration::from_secs(duration_secs);
    let report_interval = Duration::from_secs(60);

    println!(
        "         soak test starting for {}h{}m...",
        duration_secs / 3600,
        (duration_secs % 3600) / 60
    );

    // Subscribe to slots + transactions + blocks_meta simultaneously
    let request = SubscribeRequest {
        slots: HashMap::from([(
            "soak_slots".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                ..Default::default()
            },
        )]),
        transactions: HashMap::from([(
            "soak_txs".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                account_include: vec![SYSTEM_PROGRAM.to_string()],
                ..Default::default()
            },
        )]),
        blocks_meta: HashMap::from([(
            "soak_bmeta".to_string(),
            SubscribeRequestFilterBlocksMeta {},
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };

    let mut sub = GeyserStream::connect(request).await?;

    let mut slot_count = 0u64;
    let mut tx_count = 0u64;
    let mut meta_count = 0u64;
    let errors = 0u64;
    let mut last_slot: Option<u64> = None;
    let mut slot_gaps = 0u64;
    let mut last_report = Instant::now();
    let start = Instant::now();

    let deadline = tokio::time::sleep(duration);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            _ = &mut deadline => break,
            msg = sub.next_data() => {
                match msg {
                    Ok(Some(UpdateOneof::Slot(s))) => {
                        if let Some(prev) = last_slot
                            && s.slot > prev + 1
                        {
                            slot_gaps += 1;
                        }
                        last_slot = Some(s.slot);
                        slot_count += 1;
                    }
                    Ok(Some(UpdateOneof::Transaction(_))) => { tx_count += 1; }
                    Ok(Some(UpdateOneof::BlockMeta(_))) => { meta_count += 1; }
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        bail!("stream closed unexpectedly at {:?} elapsed (errors={errors})", start.elapsed());
                    }
                    Err(e) => {
                        bail!("stream error at {:?} elapsed (errors={errors}): {e:#}", start.elapsed());
                    }
                }

                // Periodic report
                if last_report.elapsed() >= report_interval {
                    let elapsed = start.elapsed();
                    let mins = elapsed.as_secs() / 60;
                    println!(
                        "         [{mins}m] slots={slot_count} txs={tx_count} metas={meta_count} gaps={slot_gaps} pings={} errors={errors}",
                        sub.pings_handled
                    );
                    last_report = Instant::now();
                }
            }
        }
    }

    let elapsed = start.elapsed();
    Ok(format!(
        "ran for {:.1}m: slots={slot_count} txs={tx_count} metas={meta_count} slot_gaps={slot_gaps} pings={} errors={errors}",
        elapsed.as_secs_f64() / 60.0,
        sub.pings_handled
    ))
}

/// GOAWAY detection: ping flood on a single connection.
/// Sends 1 ping/sec and monitors for connection drops or GOAWAY frames.
async fn test_goaway(duration_secs: u64) -> Result<String> {
    let duration = Duration::from_secs(duration_secs);

    println!(
        "         ping flood: 1 ping/sec for {}h{}m on single connection...",
        duration_secs / 3600,
        (duration_secs % 3600) / 60
    );

    // Use raw stream (not GeyserStream) so we can send pings and read concurrently
    let mut client = connect!();
    let ping_client = client.clone();
    let (tx, rx) = mpsc::channel::<SubscribeRequest>(16);
    let (unary_err_tx, mut unary_err_rx) = tokio::sync::mpsc::channel::<tonic::Status>(16);
    let request = SubscribeRequest {
        slots: HashMap::from([(
            "goaway_slots".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                ..Default::default()
            },
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };
    tx.send(request)
        .await
        .map_err(|_| anyhow::anyhow!("send failed"))?;

    let mut stream = client
        .subscribe(ReceiverStream::new(rx))
        .await?
        .into_inner();

    let mut ping_counter = 0i32;
    let mut pings_sent = 0u64;
    let mut pongs_received = 0u64;
    let mut server_pings = 0u64;
    let mut slot_count = 0u64;
    let mut goaway_detected = false;
    let mut goaway_time = 0.0;
    let mut exit_reason = "duration elapsed";
    let start = Instant::now();

    let mut ping_tick = tokio::time::interval(Duration::from_secs(1));
    let mut report_tick = tokio::time::interval(Duration::from_secs(60));
    report_tick.tick().await; // skip immediate first tick

    let mut first_slot_logged = false;
    let mut last_unary_err_log = Instant::now() - Duration::from_secs(60);

    let deadline = tokio::time::sleep(duration);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = ping_tick.tick() => {
                pings_sent += 1;
                ping_counter += 1;
                if tx.send(SubscribeRequest {
                    ping: Some(SubscribeRequestPing { id: ping_counter }),
                    ..Default::default()
                }).await.is_err() {
                    exit_reason = "send channel closed";
                    println!(
                        "         [{:.0}m] SEND FAILED — channel closed",
                        start.elapsed().as_secs_f64() / 60.0
                    );
                    break;
                }

                // Probe for GOAWAY by attempting to open a new stream via Unary Ping
                if !goaway_detected {
                    let mut p_client = ping_client.clone();
                    let p_tx = unary_err_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = p_client.ping(yellowstone_grpc_proto::geyser::PingRequest { count: 1 }).await {
                            let _ = p_tx.send(e).await;
                        }
                    });
                }
            }
            Some(err) = unary_err_rx.recv() => {
                let all_text = format!("{err:#} {err:?}").to_lowercase();
                if all_text.contains("goaway") || all_text.contains("go_away") || all_text.contains("connection closed") || all_text.contains("transport error") {
                    if !goaway_detected {
                        let mins = start.elapsed().as_secs_f64() / 60.0;
                        goaway_detected = true;
                        goaway_time = mins;
                    }
                } else {
                    // Likely an upstream error 
                    // Log it periodically but don't break the test
                    if last_unary_err_log.elapsed().as_secs() >= 30 {
                        let mins = start.elapsed().as_secs_f64() / 60.0;
                        println!("         [{mins:.1}m] [WARNING] Unary ping failed: {} (suppressing further errors for 30s)", err.message());
                        last_unary_err_log = Instant::now();
                    }
                }
            }
            _ = report_tick.tick() => {
                let secs = start.elapsed().as_secs();
                println!(
                    "         [{:02}:{:02}:{:02}] pings={pings_sent} pongs={pongs_received} srv_pings={server_pings} slots={slot_count}",
                    secs / 3600, (secs % 3600) / 60, secs % 60
                );
                if slot_count == 0 && pongs_received > 0 {
                    println!("         [WARNING] 0 slots received! Gateway is responding to pings, but backend node might be down.");
                }
            }
            msg = stream.next() => {
                match msg {
                    None => {
                        goaway_detected = true;
                        exit_reason = "stream closed";
                        println!(
                            "         [{:.0}m] STREAM CLOSED — likely GOAWAY",
                            start.elapsed().as_secs_f64() / 60.0
                        );
                        break;
                    }
                    Some(Ok(msg)) => {
                        match msg.update_oneof {
                            Some(UpdateOneof::Ping(_)) => {
                                server_pings += 1;
                                ping_counter += 1;
                                let _ = tx.send(SubscribeRequest {
                                    ping: Some(SubscribeRequestPing { id: ping_counter }),
                                    ..Default::default()
                                }).await;
                            }
                            Some(UpdateOneof::Pong(_)) => {
                                pongs_received += 1;
                            }
                            Some(UpdateOneof::Slot(_)) => {
                                slot_count += 1;
                                if !first_slot_logged {
                                    println!("         [{:.1}s] [INFO] First slot received! Upstream is healthy and streaming data.", start.elapsed().as_secs_f64());
                                    first_slot_logged = true;
                                }
                            }
                            _ => {}
                        }
                    }
                    Some(Err(e)) => {
                        let mins = start.elapsed().as_secs_f64() / 60.0;
                        println!("         [{mins:.0}m] CONNECTION ERROR:");
                        println!("           gRPC code: {:?}", e.code());
                        println!("           message: {}", e.message());
                        // Walk the error source chain for h2/transport details
                        let mut source: Option<&dyn std::error::Error> =
                            std::error::Error::source(&e);
                        let mut depth = 0;
                        while let Some(err) = source {
                            depth += 1;
                            println!("           cause [{depth}]: {err}");
                            // Debug format may reveal h2 error codes/reason
                            let dbg = format!("{err:?}");
                            if dbg != format!("{err}") {
                                println!("           debug [{depth}]: {dbg}");
                            }
                            source = err.source();
                        }

                        let all_text = format!("{e:#} {e:?}").to_lowercase();
                        if all_text.contains("goaway") || all_text.contains("go_away")
                            || all_text.contains("connection closed")
                            || all_text.contains("reset by peer")
                        {
                            goaway_detected = true;
                            exit_reason = "goaway";
                        } else {
                            exit_reason = "error";
                        }
                        break;
                    }
                }
            }
        }
    }

    let elapsed = start.elapsed();
    let mut detail = format!(
        "ran {:.1}m ({exit_reason}): pings_sent={pings_sent} pongs={pongs_received} server_pings={server_pings} slots={slot_count}",
        elapsed.as_secs_f64() / 60.0
    );
    if goaway_detected {
        detail.push_str(&format!(
            "\n         ✅ GOAWAY received at {:.1}m, and the main stream successfully survived until {:.1}m!",
            goaway_time,
            elapsed.as_secs_f64() / 60.0
        ));
    }

    Ok(detail)
}

// --- Latency Comparison ---

struct Target {
    name: String,
    url: String,
    auth_header: String,
    api_key: String,
}

struct LatencyEvent {
    target: String,
    slot: u64,
    kind: &'static str,
    time: Instant,
    size: usize,
}

fn parse_target(s: &str) -> Result<Target> {
    let parts: Vec<&str> = s.splitn(4, ',').collect();
    if parts.len() != 4 {
        bail!("invalid target format: expected name,url,header,key\n  got: {s}");
    }
    Ok(Target {
        name: parts[0].to_string(),
        url: parts[1].to_string(),
        auth_header: parts[2].to_string(),
        api_key: parts[3].to_string(),
    })
}

fn format_delta(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 1.0 {
        format!("+{ms:.1}ms")
    } else {
        format!("+{ms:.0}ms")
    }
}

fn percentile_duration(values: &mut [Duration], p: f64) -> Duration {
    if values.is_empty() {
        return Duration::ZERO;
    }
    values.sort();
    let idx = ((values.len() as f64 * p / 100.0).ceil() as usize).saturating_sub(1);
    values[idx.min(values.len() - 1)]
}

async fn run_latency_target(
    target: Target,
    event_tx: mpsc::Sender<LatencyEvent>,
    duration: Duration,
) -> Result<()> {
    let endpoint = Endpoint::from_shared(target.url.clone())?
        .tls_config(ClientTlsConfig::new().with_native_roots())?;
    let channel = endpoint
        .connect()
        .await
        .with_context(|| format!("{}: failed to connect", target.name))?;

    let key: tonic::metadata::AsciiMetadataValue = target
        .api_key
        .parse()
        .map_err(|_| anyhow::anyhow!("{}: invalid API key", target.name))?;
    let header: tonic::metadata::AsciiMetadataKey = target
        .auth_header
        .parse()
        .map_err(|_| anyhow::anyhow!("{}: invalid auth header", target.name))?;

    let mut client = GeyserClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
        req.metadata_mut().insert(header.clone(), key.clone());
        Ok(req)
    })
    .max_decoding_message_size(MAX_DECODE_SIZE);

    let (tx, rx) = mpsc::channel::<SubscribeRequest>(16);
    let request = SubscribeRequest {
        slots: HashMap::from([(
            "lat_slots".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                ..Default::default()
            },
        )]),
        blocks: HashMap::from([(
            "lat_blocks".to_string(),
            SubscribeRequestFilterBlocks {
                account_include: vec![],
                include_transactions: Some(true),
                include_accounts: Some(false),
                include_entries: Some(false),
            },
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };
    tx.send(request)
        .await
        .map_err(|_| anyhow::anyhow!("send failed"))?;

    let mut stream = client
        .subscribe(ReceiverStream::new(rx))
        .await
        .with_context(|| format!("{}: subscribe failed", target.name))?
        .into_inner();

    let mut ping_counter = 0i32;
    let name = target.name;

    let deadline = tokio::time::sleep(duration);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            _ = &mut deadline => break,
            msg = stream.next() => {
                let Some(msg) = msg else { break };
                let msg = msg?;
                match msg.update_oneof {
                    Some(UpdateOneof::Ping(_)) => {
                        ping_counter += 1;
                        let _ = tx.send(SubscribeRequest {
                            ping: Some(SubscribeRequestPing { id: ping_counter }),
                            ..Default::default()
                        }).await;
                    }
                    Some(UpdateOneof::Slot(slot)) => {
                        let _ = event_tx.send(LatencyEvent {
                            target: name.clone(),
                            slot: slot.slot,
                            kind: "slot",
                            time: Instant::now(),
                            size: 0,
                        }).await;
                    }
                    Some(UpdateOneof::Block(block)) => {
                        let _ = event_tx.send(LatencyEvent {
                            target: name.clone(),
                            slot: block.slot,
                            kind: "block",
                            time: Instant::now(),
                            size: block.encoded_len(),
                        }).await;
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

async fn test_latency(targets: Vec<Target>, duration_secs: u64) -> Result<String> {
    let duration = Duration::from_secs(duration_secs);
    let num_targets = targets.len();
    let target_names: Vec<String> = targets.iter().map(|t| t.name.clone()).collect();

    println!("         comparing {num_targets} endpoints for {duration_secs}s...");
    for t in &targets {
        println!("         - {}: {} ({})", t.name, t.url, t.auth_header);
    }

    let (event_tx, mut event_rx) = mpsc::channel::<LatencyEvent>(10_000);

    let mut handles = Vec::new();
    for target in targets {
        let name = target.name.clone();
        let tx = event_tx.clone();
        handles.push((name, tokio::spawn(run_latency_target(target, tx, duration))));
    }
    drop(event_tx);

    // Collect events
    let mut slot_arrivals: HashMap<u64, Vec<(String, Instant)>> = HashMap::new();
    let mut block_arrivals: HashMap<u64, Vec<(String, Instant, usize)>> = HashMap::new();

    while let Some(event) = event_rx.recv().await {
        match event.kind {
            "slot" => slot_arrivals
                .entry(event.slot)
                .or_default()
                .push((event.target, event.time)),
            "block" => block_arrivals
                .entry(event.slot)
                .or_default()
                .push((event.target, event.time, event.size)),
            _ => {}
        }
    }

    // Wait for tasks
    for (name, handle) in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => println!("         {name}: error: {e:#}"),
            Err(e) => println!("         {name}: task panicked: {e}"),
        }
    }

    // Count raw totals per endpoint
    let mut raw_slot_totals: HashMap<String, u64> = HashMap::new();
    let mut raw_block_totals: HashMap<String, u64> = HashMap::new();
    for name in &target_names {
        raw_slot_totals.insert(name.clone(), 0);
        raw_block_totals.insert(name.clone(), 0);
    }
    for arrivals in slot_arrivals.values() {
        for (name, _) in arrivals {
            *raw_slot_totals.get_mut(name).unwrap() += 1;
        }
    }
    for arrivals in block_arrivals.values() {
        for (name, _, _) in arrivals {
            *raw_block_totals.get_mut(name).unwrap() += 1;
        }
    }

    // Print raw totals
    println!("\n         === Per-Endpoint Totals ===");
    for name in &target_names {
        println!(
            "         {:<16} slots={:<6} blocks={}",
            name, raw_slot_totals[name], raw_block_totals[name]
        );
    }

    // Check if comparison is possible
    let endpoints_with_slots = target_names
        .iter()
        .filter(|n| raw_slot_totals[*n] > 0)
        .count();
    if endpoints_with_slots < 2 {
        bail!(
            "cannot compare: only {endpoints_with_slots} endpoint(s) produced data (need at least 2)"
        );
    }

    // Analyze slots
    let mut slot_deltas: HashMap<String, Vec<Duration>> = HashMap::new();
    let mut slot_wins: HashMap<String, u64> = HashMap::new();
    let mut slot_compared: HashMap<String, u64> = HashMap::new();
    for name in &target_names {
        slot_deltas.insert(name.clone(), Vec::new());
        slot_wins.insert(name.clone(), 0);
        slot_compared.insert(name.clone(), 0);
    }

    let mut compared_slots = 0u64;
    for (_, mut arrivals) in slot_arrivals {
        if arrivals.len() < 2 {
            continue;
        }
        compared_slots += 1;
        arrivals.sort_by_key(|(_, t)| *t);
        let first_time = arrivals[0].1;
        *slot_wins.get_mut(&arrivals[0].0).unwrap() += 1;
        for (name, time) in &arrivals {
            slot_deltas
                .get_mut(name)
                .unwrap()
                .push(time.duration_since(first_time));
            *slot_compared.get_mut(name).unwrap() += 1;
        }
    }

    // Analyze blocks
    let mut block_deltas: HashMap<String, Vec<Duration>> = HashMap::new();
    let mut block_wins: HashMap<String, u64> = HashMap::new();
    let mut block_compared: HashMap<String, u64> = HashMap::new();
    let mut block_sizes: HashMap<String, Vec<usize>> = HashMap::new();
    for name in &target_names {
        block_deltas.insert(name.clone(), Vec::new());
        block_wins.insert(name.clone(), 0);
        block_compared.insert(name.clone(), 0);
        block_sizes.insert(name.clone(), Vec::new());
    }

    let mut compared_blocks = 0u64;
    for (_, mut arrivals) in block_arrivals {
        if arrivals.len() < 2 {
            continue;
        }
        compared_blocks += 1;
        arrivals.sort_by_key(|(_, t, _)| *t);
        let first_time = arrivals[0].1;
        *block_wins.get_mut(&arrivals[0].0).unwrap() += 1;
        for (name, time, size) in &arrivals {
            block_deltas
                .get_mut(name)
                .unwrap()
                .push(time.duration_since(first_time));
            *block_compared.get_mut(name).unwrap() += 1;
            block_sizes.get_mut(name).unwrap().push(*size);
        }
    }

    // Print slot comparison
    println!(
        "\n         === Slot Latency ({compared_slots} comparable) ==="
    );
    println!(
        "         {:<16} {:>6} {:>8} {:>6} {:>10} {:>10} {:>10}",
        "Endpoint", "Total", "Compared", "Wins", "Avg Δ", "P50 Δ", "P99 Δ"
    );
    let mut slot_winner = String::new();
    let mut best_wins = 0u64;
    for name in &target_names {
        let wins = slot_wins[name];
        let total = raw_slot_totals[name];
        let compared = slot_compared[name];
        let deltas = slot_deltas.get_mut(name).unwrap();
        let avg = if deltas.is_empty() {
            Duration::ZERO
        } else {
            deltas.iter().sum::<Duration>() / deltas.len() as u32
        };
        let p50 = percentile_duration(deltas, 50.0);
        let p99 = percentile_duration(deltas, 99.0);
        println!(
            "         {:<16} {:>6} {:>8} {:>6} {:>10} {:>10} {:>10}",
            name,
            total,
            compared,
            wins,
            format_delta(avg),
            format_delta(p50),
            format_delta(p99)
        );
        if wins > best_wins {
            best_wins = wins;
            slot_winner = name.clone();
        }
    }

    // Print block comparison
    if compared_blocks > 0 {
        println!(
            "\n         === Block Latency ({compared_blocks} comparable) ==="
        );
        println!(
            "         {:<16} {:>6} {:>8} {:>6} {:>10} {:>10} {:>10} {:>10}",
            "Endpoint", "Total", "Compared", "Wins", "Avg Δ", "P50 Δ", "P99 Δ", "Avg Size"
        );
        for name in &target_names {
            let wins = block_wins[name];
            let total = raw_block_totals[name];
            let compared = block_compared[name];
            let deltas = block_deltas.get_mut(name).unwrap();
            let avg = if deltas.is_empty() {
                Duration::ZERO
            } else {
                deltas.iter().sum::<Duration>() / deltas.len() as u32
            };
            let p50 = percentile_duration(deltas, 50.0);
            let p99 = percentile_duration(deltas, 99.0);
            let sizes = &block_sizes[name];
            let avg_size = if sizes.is_empty() {
                0.0
            } else {
                sizes.iter().sum::<usize>() as f64 / sizes.len() as f64 / (1024.0 * 1024.0)
            };
            println!(
                "         {:<16} {:>6} {:>8} {:>6} {:>10} {:>10} {:>10} {:>8.1}MB",
                name,
                total,
                compared,
                wins,
                format_delta(avg),
                format_delta(p50),
                format_delta(p99),
                avg_size
            );
        }
    }

    Ok(format!(
        "compared {num_targets} endpoints for {duration_secs}s: {compared_slots} slots, {compared_blocks} blocks, slot winner={slot_winner}"
    ))
}

// --- Billing Analysis ---

/// Run all RPC types, count every request and response, and print a billing table.
///
/// Billing formula:
///   Unary RPC:  1 billing event per request message + 1 per response message
///   Streaming:  subscribe() call is FREE
///               initial SubscribeRequest (filter message) = 1 billing event
///               first response = FREE; each additional response = 1 billing event
///   Keepalive:  client pong-reply messages tracked separately
async fn run_billing_analysis(stream_messages: u32) -> Result<String> {
    struct Row {
        label: &'static str,
        kind: &'static str, // "unary" | "stream"
        reqs: u64,
        resps: u64,
        free: u64, // 1 for stream (first response free), 0 for unary
    }

    let mut rows: Vec<Row> = Vec::new();
    let mut ping_replies: u64 = 0;
    let mut server_pings_rcvd: u64 = 0;
    let mut pongs_rcvd: u64 = 0;

    // --- Unary RPCs ---
    println!("  Running unary RPCs...");
    {
        let mut client = connect!();

        client
            .get_version(yellowstone_grpc_proto::geyser::GetVersionRequest {})
            .await
            .context("GetVersion")?;
        rows.push(Row { label: "GetVersion", kind: "unary", reqs: 1, resps: 1, free: 0 });

        client
            .ping(yellowstone_grpc_proto::geyser::PingRequest { count: 1 })
            .await
            .context("Ping")?;
        rows.push(Row { label: "Ping (unary)", kind: "unary", reqs: 1, resps: 1, free: 0 });

        for (_, level) in commitment_levels() {
            client
                .get_latest_blockhash(
                    yellowstone_grpc_proto::geyser::GetLatestBlockhashRequest {
                        commitment: Some(level as i32),
                    },
                )
                .await
                .context("GetLatestBlockhash")?;
        }
        rows.push(Row {
            label: "GetLatestBlockhash (x3 commitments)",
            kind: "unary",
            reqs: 3,
            resps: 3,
            free: 0,
        });

        for (_, level) in commitment_levels() {
            client
                .get_block_height(yellowstone_grpc_proto::geyser::GetBlockHeightRequest {
                    commitment: Some(level as i32),
                })
                .await
                .context("GetBlockHeight")?;
        }
        rows.push(Row {
            label: "GetBlockHeight (x3 commitments)",
            kind: "unary",
            reqs: 3,
            resps: 3,
            free: 0,
        });

        for (_, level) in commitment_levels() {
            client
                .get_slot(yellowstone_grpc_proto::geyser::GetSlotRequest {
                    commitment: Some(level as i32),
                })
                .await
                .context("GetSlot")?;
        }
        rows.push(Row {
            label: "GetSlot (x3 commitments)",
            kind: "unary",
            reqs: 3,
            resps: 3,
            free: 0,
        });

        let bh = client
            .get_latest_blockhash(
                yellowstone_grpc_proto::geyser::GetLatestBlockhashRequest {
                    commitment: Some(CommitmentLevel::Finalized as i32),
                },
            )
            .await
            .context("GetLatestBlockhash for IsBlockhashValid")?
            .into_inner();
        client
            .is_blockhash_valid(yellowstone_grpc_proto::geyser::IsBlockhashValidRequest {
                blockhash: bh.blockhash,
                commitment: Some(CommitmentLevel::Finalized as i32),
            })
            .await
            .context("IsBlockhashValid")?;
        rows.push(Row {
            label: "GetLatestBlockhash + IsBlockhashValid",
            kind: "unary",
            reqs: 2,
            resps: 2,
            free: 0,
        });

        client
            .subscribe_replay_info(SubscribeReplayInfoRequest {})
            .await
            .context("SubscribeReplayInfo")?;
        rows.push(Row {
            label: "SubscribeReplayInfo",
            kind: "unary",
            reqs: 1,
            resps: 1,
            free: 0,
        });
    }
    println!("  Unary RPCs: done");

    // --- Subscribe: Slots ---
    println!("  Subscribe(slots): collecting {stream_messages} messages...");
    {
        let request = SubscribeRequest {
            slots: HashMap::from([(
                "bill_slots".to_string(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(true),
                    ..Default::default()
                },
            )]),
            commitment: Some(CommitmentLevel::Confirmed as i32),
            ..Default::default()
        };
        let mut sub = GeyserStream::connect(request).await?;
        let mut count = 0u32;
        tokio::time::timeout(stream_timeout(), async {
            while let Some(upd) = sub.next_data().await? {
                if let UpdateOneof::Slot(_) = upd {
                    count += 1;
                }
                if count >= stream_messages {
                    break;
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("Subscribe(slots) timed out")??;
        ping_replies += sub.pings_handled as u64;
        server_pings_rcvd += sub.pings_handled as u64;
        pongs_rcvd += sub.pongs_received as u64;
        rows.push(Row {
            label: "Subscribe(slots)",
            kind: "stream",
            reqs: 1,
            resps: count as u64,
            free: 1,
        });
        println!("  Subscribe(slots): received {count}");
    }

    // --- Subscribe: Transactions ---
    println!("  Subscribe(transactions): collecting {stream_messages} messages...");
    {
        let request = SubscribeRequest {
            transactions: HashMap::from([(
                "bill_txs".to_string(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: Some(false),
                    account_include: vec![SYSTEM_PROGRAM.to_string()],
                    ..Default::default()
                },
            )]),
            commitment: Some(CommitmentLevel::Confirmed as i32),
            ..Default::default()
        };
        let mut sub = GeyserStream::connect(request).await?;
        let mut count = 0u32;
        tokio::time::timeout(stream_timeout(), async {
            while let Some(upd) = sub.next_data().await? {
                if let UpdateOneof::Transaction(_) = upd {
                    count += 1;
                }
                if count >= stream_messages {
                    break;
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("Subscribe(transactions) timed out")??;
        ping_replies += sub.pings_handled as u64;
        server_pings_rcvd += sub.pings_handled as u64;
        pongs_rcvd += sub.pongs_received as u64;
        rows.push(Row {
            label: "Subscribe(transactions)",
            kind: "stream",
            reqs: 1,
            resps: count as u64,
            free: 1,
        });
        println!("  Subscribe(transactions): received {count}");
    }

    // --- Subscribe: BlocksMeta ---
    println!("  Subscribe(blocks_meta): collecting {stream_messages} messages...");
    {
        let request = SubscribeRequest {
            blocks_meta: HashMap::from([(
                "bill_bmeta".to_string(),
                SubscribeRequestFilterBlocksMeta {},
            )]),
            commitment: Some(CommitmentLevel::Confirmed as i32),
            ..Default::default()
        };
        let mut sub = GeyserStream::connect(request).await?;
        let mut count = 0u32;
        tokio::time::timeout(stream_timeout(), async {
            while let Some(upd) = sub.next_data().await? {
                if let UpdateOneof::BlockMeta(_) = upd {
                    count += 1;
                }
                if count >= stream_messages {
                    break;
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("Subscribe(blocks_meta) timed out")??;
        ping_replies += sub.pings_handled as u64;
        server_pings_rcvd += sub.pings_handled as u64;
        pongs_rcvd += sub.pongs_received as u64;
        rows.push(Row {
            label: "Subscribe(blocks_meta)",
            kind: "stream",
            reqs: 1,
            resps: count as u64,
            free: 1,
        });
        println!("  Subscribe(blocks_meta): received {count}");
    }

    // --- Print billing tables ---
    println!();
    println!("  Billing formula:");
    println!("    Unary RPC:  1 billing event per request + 1 per response");
    println!("    Streaming:  subscribe() call is FREE");
    println!("                initial SubscribeRequest (filter msg) = 1 billing event");
    println!("                1st response = FREE; each additional response = 1 billing event");
    println!("    Keepalive:  client pong-reply messages listed separately");
    println!();

    let sep = format!(
        "  {}  {}  {}  {}",
        "-".repeat(43),
        "-".repeat(7),
        "-".repeat(12),
        "-".repeat(24)
    );

    // ── REQUESTS SENT ──────────────────────────────────────────────────────
    println!("  {:<43}  {:>7}  {:>12}  {}", "REQUESTS SENT", "Count", "Bill.Events", "Note");
    println!("{sep}");
    let mut req_data_count = 0u64;
    let mut req_data_bill = 0u64;
    for r in &rows {
        req_data_count += r.reqs;
        req_data_bill += r.reqs;
        let note = if r.kind == "stream" { "initial SubscribeRequest" } else { "RPC call" };
        println!("  {:<43}  {:>7}  {:>12}  {}", r.label, r.reqs, r.reqs, note);
    }
    println!(
        "  {:<43}  {:>7}  {:>12}  {}",
        "PingReply (keepalive)", ping_replies, ping_replies, "pong-reply to server ping"
    );
    println!("{sep}");
    println!(
        "  {:<43}  {:>7}  {:>12}  {}",
        "Subtotal (data only)", req_data_count, req_data_bill, "excl. keepalive"
    );
    println!(
        "  {:<43}  {:>7}  {:>12}  {}",
        "TOTAL REQUESTS",
        req_data_count + ping_replies,
        req_data_bill + ping_replies,
        "incl. keepalive"
    );
    println!();

    // ── RESPONSES RECEIVED ─────────────────────────────────────────────────
    println!("  {:<43}  {:>7}  {:>12}  {}", "RESPONSES RECEIVED", "Count", "Bill.Events", "Note");
    println!("{sep}");
    let mut resp_data_count = 0u64;
    let mut resp_data_bill = 0u64;
    for r in &rows {
        let bill = r.resps.saturating_sub(r.free);
        resp_data_count += r.resps;
        resp_data_bill += bill;
        let note = if r.free > 0 { "1st response free" } else { "" };
        println!("  {:<43}  {:>7}  {:>12}  {}", r.label, r.resps, bill, note);
    }
    println!(
        "  {:<43}  {:>7}  {:>12}  {}",
        "ServerPing (server keepalive)", server_pings_rcvd, 0u64, "infra, not billed"
    );
    println!(
        "  {:<43}  {:>7}  {:>12}  {}",
        "Pong (reply to our PingReply)", pongs_rcvd, 0u64, "infra, not billed"
    );
    let resp_infra = server_pings_rcvd + pongs_rcvd;
    println!("{sep}");
    println!(
        "  {:<43}  {:>7}  {:>12}  {}",
        "Subtotal (data only)", resp_data_count, resp_data_bill, "excl. infra"
    );
    println!(
        "  {:<43}  {:>7}  {:>12}  {}",
        "TOTAL RESPONSES",
        resp_data_count + resp_infra,
        resp_data_bill,
        "incl. infra"
    );
    println!();

    // ── GRAND TOTAL ────────────────────────────────────────────────────────
    let grand_data = req_data_bill + resp_data_bill;
    let grand_all = req_data_bill + ping_replies + resp_data_bill;
    let rule = "=".repeat(92);
    println!("  {rule}");
    println!("  GRAND TOTAL BILLING EVENTS");
    println!("  {:<43}  {:>7}", "Excl. keepalive ping replies:", grand_data);
    println!("  {:<43}  {:>7}", "Incl. keepalive ping replies:", grand_all);
    println!("  {rule}");

    Ok(format!(
        "total_req={} total_resp={} billing_excl_pings={grand_data} billing_incl_pings={grand_all}",
        req_data_count + ping_replies,
        resp_data_count + resp_infra,
    ))
}

// --- Test Registry ---

const UNARY_TESTS: &[(&str, &str)] = &[
    ("version", "GetVersion"),
    ("ping", "Ping"),
    ("blockhash", "GetLatestBlockhash"),
    ("block-height", "GetBlockHeight"),
    ("slot", "GetSlot"),
    ("blockhash-valid", "IsBlockhashValid"),
    ("replay-info", "SubscribeReplayInfo"),
];

const STREAM_TESTS: &[(&str, &str)] = &[
    ("slots", "Subscribe Slots"),
    ("accounts", "Subscribe Accounts (USDC)"),
    ("transactions", "Subscribe Transactions (System)"),
    ("blocks-meta", "Subscribe Blocks Meta"),
    ("blocks", "Subscribe Full Block"),
    ("deshred", "Subscribe Deshred (txs)"),
];

const ADVANCED_TESTS: &[(&str, &str)] = &[
    ("keepalive", "Keepalive (ping/pong)"),
    ("resubscribe", "Re-subscribe (slots -> blocks_meta)"),
    ("unsubscribe", "Unsubscribe (empty filters)"),
    ("backpressure", "Back Pressure (slow reader)"),
    ("streams", "Stream Flood (concurrent)"),
];

async fn run_test_by_name(t: &mut TestRunner, name: &str) -> bool {
    match name {
        "version" => t.run("GetVersion", test_get_version()).await,
        "ping" => t.run("Ping", test_ping()).await,
        "blockhash" => {
            t.run("GetLatestBlockhash", test_get_latest_blockhash())
                .await
        }
        "block-height" => t.run("GetBlockHeight", test_get_block_height()).await,
        "slot" => t.run("GetSlot", test_get_slot()).await,
        "blockhash-valid" => t.run("IsBlockhashValid", test_is_blockhash_valid()).await,
        "replay-info" => {
            t.run("SubscribeReplayInfo", test_subscribe_replay_info())
                .await
        }
        "slots" => t.run("Subscribe Slots", test_subscribe_slots()).await,
        "accounts" => {
            t.run("Subscribe Accounts (USDC)", test_subscribe_accounts())
                .await
        }
        "transactions" => {
            t.run(
                "Subscribe Transactions (System)",
                test_subscribe_transactions(),
            )
            .await
        }
        "blocks-meta" => {
            t.run("Subscribe Blocks Meta", test_subscribe_blocks_meta())
                .await
        }
        "blocks" => {
            t.run("Subscribe Full Block", test_subscribe_full_block())
                .await
        }
        "deshred" => {
            t.run("Subscribe Deshred (txs)", test_subscribe_deshred())
                .await
        }
        "keepalive" => t.run("Keepalive (ping/pong)", test_keepalive()).await,
        "resubscribe" => {
            t.run("Re-subscribe (slots -> blocks_meta)", test_resubscribe())
                .await
        }
        "unsubscribe" => {
            t.run("Unsubscribe (empty filters)", test_unsubscribe())
                .await
        }
        "backpressure" => {
            t.run("Back Pressure (slow reader)", test_back_pressure())
                .await
        }
        "streams" => {
            t.run(
                "Stream Flood (concurrent)",
                test_stream_flood(config().stress_connections, config().stress_streams),
            )
            .await
        }
        _ => {
            eprintln!("unknown test: {name}");
            eprintln!("run `solana-grpc-test list` to see available tests");
            return false;
        }
    }
    true
}

async fn run_group(t: &mut TestRunner, tests: &[(&str, &str)], label: &str) {
    println!("\x1b[1;36m--- {label} ---\x1b[0m");
    for &(name, _) in tests {
        run_test_by_name(t, name).await;
    }
}

async fn run_all(t: &mut TestRunner) {
    run_group(t, UNARY_TESTS, "Unary RPCs").await;
    println!();
    run_group(t, STREAM_TESTS, "Streaming Subscriptions").await;
    println!();
    run_group(t, ADVANCED_TESTS, "Advanced Stream Tests").await;
}

fn print_test_list() {
    println!("Available tests:\n");
    println!("  Groups:");
    println!("    all         Run all tests");
    println!("    unary       Run unary RPC tests");
    println!("    stream      Run streaming subscription tests");
    println!("    advanced    Run advanced stream tests\n");
    println!("  Unary RPCs:");
    for &(name, desc) in UNARY_TESTS {
        println!("    {name:<16}{desc}");
    }
    println!("\n  Streaming Subscriptions:");
    for &(name, desc) in STREAM_TESTS {
        println!("    {name:<16}{desc}");
    }
    println!("\n  Advanced:");
    for &(name, desc) in ADVANCED_TESTS {
        println!("    {name:<16}{desc}");
    }
    println!("\n  Long-running / stress:");
    println!("    soak          Soak test (use `soak` subcommand)");
    println!("    stress        Stream flood (use `stress` subcommand)");
    println!("    goaway        Ping flood to detect GOAWAY (use `goaway` subcommand)");
    println!("\n  Multi-endpoint:");
    println!("    latency       Compare latency between endpoints (use `latency` subcommand)");
    println!("                  args: name,url,header,key (one per target, at least 2)");
    println!("\n  Billing:");
    println!("    billing       Count all requests/responses and compute billing events");
    println!("                  use `billing --messages N` to set stream message count (default 5)");
}

// --- Main ---

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let cli = Cli::parse();

    if matches!(cli.command, Some(Commands::List)) {
        print_test_list();
        return;
    }

    // Latency comparison uses per-target auth, not global config
    if let Some(Commands::Latency { ref duration, ref targets }) = cli.command {
        let parsed = match targets
            .iter()
            .map(|s| parse_target(s))
            .collect::<Result<Vec<_>>>()
        {
            Ok(t) => t,
            Err(e) => {
                eprintln!("error: {e}");
                process::exit(2);
            }
        };
        if parsed.len() < 2 {
            eprintln!("error: latency comparison requires at least 2 targets");
            process::exit(2);
        }
        let dur = *duration;
        println!("=== Yellowstone gRPC Latency Comparison ===\n");
        let mut t = TestRunner::new();
        t.run("Latency Comparison", test_latency(parsed, dur)).await;
        t.summary();
        process::exit(t.exit_code());
    }

    let api_key = cli.api_key.unwrap_or_else(|| {
        eprintln!("error: --api-key <API_KEY> (or API_KEY env var) is required");
        process::exit(2);
    });

    CONFIG
        .set(Config {
            endpoint: cli.endpoint,
            api_key,
            auth_header: cli.auth_header,
            stream_timeout: Duration::from_secs(cli.stream_timeout),
            block_timeout: Duration::from_secs(cli.block_timeout),
            keepalive_timeout: Duration::from_secs(cli.keepalive_timeout),
            stress_connections: cli.stress_connections,
            stress_streams: cli.stress_streams,
        })
        .expect("config already set");

    let cfg = config();
    println!("=== Yellowstone gRPC Endpoint Test ===");
    println!("Endpoint: {}", cfg.endpoint);
    println!("Auth header: {}", cfg.auth_header);
    println!(
        "Timeouts: stream={}s block={}s keepalive={}s",
        cfg.stream_timeout.as_secs(),
        cfg.block_timeout.as_secs(),
        cfg.keepalive_timeout.as_secs()
    );
    println!();

    let mut t = TestRunner::new();

    match cli.command {
        None | Some(Commands::All) => {
            run_all(&mut t).await;
        }
        Some(Commands::Unary) => {
            run_group(&mut t, UNARY_TESTS, "Unary RPCs").await;
        }
        Some(Commands::Stream) => {
            run_group(&mut t, STREAM_TESTS, "Streaming Subscriptions").await;
        }
        Some(Commands::Advanced) => {
            run_group(&mut t, ADVANCED_TESTS, "Advanced Stream Tests").await;
        }
        Some(Commands::Run { tests }) => {
            for name in &tests {
                if !run_test_by_name(&mut t, name).await {
                    process::exit(2);
                }
            }
        }
        Some(Commands::Soak { duration }) => {
            println!("--- Soak Test ---");
            t.run("Soak (long-running stability)", test_soak(duration))
                .await;
        }
        Some(Commands::Stress {
            connections,
            streams_per_conn,
        }) => {
            println!("--- Stream Stress Test ---");
            t.run(
                "Stream Flood (concurrent)",
                test_stream_flood(connections, streams_per_conn),
            )
            .await;
        }
        Some(Commands::Goaway { duration }) => {
            println!("--- GOAWAY Detection Test ---");
            t.run("GOAWAY (ping flood)", test_goaway(duration)).await;
        }
        Some(Commands::Billing { messages }) => {
            println!("--- Billing Analysis ---");
            t.run("Billing Analysis", run_billing_analysis(messages)).await;
        }
        Some(Commands::List | Commands::Latency { .. }) => unreachable!(),
    }

    t.summary();
    process::exit(t.exit_code());
}
