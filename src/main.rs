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
    /// List all available tests
    List,
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
                println!("[PASS] ({elapsed:.0?})\n         {detail}");
                self.passed += 1;
            }
            Err(e) => {
                let elapsed = start.elapsed();
                println!("[FAIL] ({elapsed:.0?})\n         Error: {e:#}");
            }
        }
    }

    fn summary(&self) {
        println!();
        println!("=== Results: {}/{} passed ===", self.passed, self.total);
        if self.passed == self.total {
            println!("All tests passed!");
        } else {
            println!("{} test(s) failed.", self.total - self.passed);
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
/// Verifies the proxy handles slow consumers gracefully.
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
    println!("--- {label} ---");
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
    println!("=== Yellowstone gRPC Proxy Test ===");
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
        Some(Commands::List) => unreachable!(),
    }

    t.summary();
    process::exit(t.exit_code());
}
