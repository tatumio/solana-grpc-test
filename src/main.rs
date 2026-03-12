use std::collections::HashMap;
use std::env;
use std::process;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use prost::Message as ProstMessage;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use yellowstone_grpc_client::ClientTlsConfig;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
    SubscribeRequestPing, SubscribeUpdate,
    geyser_client::GeyserClient, subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::tonic;
use yellowstone_grpc_proto::tonic::transport::{Channel, Endpoint};

const DEFAULT_ENDPOINT: &str = "https://solana-mainnet-grpc.gateway.tatum.dev";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
const MAX_DECODE_SIZE: usize = 128 * 1024 * 1024;

fn get_endpoint() -> String {
    env::var("GRPC_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_string())
}

fn get_api_key() -> Result<String> {
    env::var("API_KEY").context("API_KEY env var is required for authentication")
}

fn get_auth_header() -> String {
    env::var("AUTH_HEADER").unwrap_or_else(|_| "x-api-key".to_string())
}

fn timeout_secs(env_name: &str, default: u64) -> Duration {
    let secs = env::var(env_name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default);
    Duration::from_secs(secs)
}

fn stream_timeout() -> Duration {
    timeout_secs("STREAM_TIMEOUT_SECS", 30)
}

fn block_timeout() -> Duration {
    timeout_secs("BLOCK_TIMEOUT_SECS", 120)
}

fn keepalive_timeout() -> Duration {
    timeout_secs("KEEPALIVE_TIMEOUT_SECS", 40)
}

async fn connect_channel() -> Result<Channel> {
    let url = get_endpoint();
    let endpoint = Endpoint::from_shared(url)?
        .tls_config(ClientTlsConfig::new().with_native_roots())?;
    let channel = endpoint.connect().await?;
    Ok(channel)
}

macro_rules! connect {
    () => {{
        let channel = connect_channel().await?;
        let api_key = get_api_key()?;
        let header_name = get_auth_header();
        let key: tonic::metadata::AsciiMetadataValue = api_key
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid API key (not valid ASCII metadata)"))?;
        let header: tonic::metadata::AsciiMetadataKey = header_name
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid auth header name: {header_name}"))?;
        GeyserClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
            req.metadata_mut().insert(header.clone(), key.clone());
            Ok(req)
        })
        .max_decoding_message_size(MAX_DECODE_SIZE)
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
        tx.send(request).await.map_err(|_| anyhow::anyhow!("send failed"))?;
        let stream = client
            .subscribe(ReceiverStream::new(rx))
            .await?
            .into_inner();
        Ok(Self { tx, stream, ping_counter: 0, pings_handled: 0, pongs_received: 0 })
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
                    return Ok(Some(UpdateOneof::Ping(yellowstone_grpc_proto::geyser::SubscribeUpdatePing {})));
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
        let _ = self.tx.send(SubscribeRequest {
            ping: Some(SubscribeRequestPing { id: self.ping_counter }),
            ..Default::default()
        }).await;
    }

    /// Send a new SubscribeRequest (for re-subscription or unsubscribe).
    /// This REPLACES all existing filters.
    async fn send(&self, request: SubscribeRequest) -> Result<()> {
        self.tx.send(request).await
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
        Self { passed: 0, total: 0 }
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
        bail!("blockhash {blockhash} reported as invalid (slot={})", resp.slot);
    }
    Ok(format!("blockhash={blockhash} valid=true slot={}", resp.slot))
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
                if count >= 3 { break; }
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out waiting for slot updates")??;

    Ok(format!("received {count} slots: {slots:?} (pings handled: {})", sub.pings_handled))
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
                if count >= 3 { break; }
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out waiting for transaction updates")??;

    let sigs_display: Vec<String> = sigs.iter().map(|s| s[..8.min(s.len())].to_string()).collect();
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
                if count >= 2 { break; }
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out waiting for blocks_meta updates")??;

    Ok(format!("received {count} blocks_meta\n         {}", details.join("\n         ")))
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
                Some(UpdateOneof::Slot(_)) => { slot_count += 1; }
                Some(UpdateOneof::Pong(_)) => { sub.pongs_received += 1; }
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
        bail!("ran for {duration:.0?} but received 0 server pings (expected at least 1 every ~15s)");
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
                if slot_count >= 3 { break; }
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
                    if meta_count >= 2 { break; }
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

// --- Main ---

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let endpoint = get_endpoint();
    let auth_header = get_auth_header();
    println!("=== Yellowstone gRPC Proxy Test ===");
    println!("Endpoint: {endpoint}");
    println!("Auth header: {auth_header}");
    println!("Timeouts: stream={}s block={}s keepalive={}s",
        stream_timeout().as_secs(), block_timeout().as_secs(), keepalive_timeout().as_secs());
    println!();

    let mut t = TestRunner::new();

    println!("--- Unary RPCs ---");
    t.run("GetVersion", test_get_version()).await;
    t.run("Ping", test_ping()).await;
    t.run("GetLatestBlockhash", test_get_latest_blockhash()).await;
    t.run("GetBlockHeight", test_get_block_height()).await;
    t.run("GetSlot", test_get_slot()).await;
    t.run("IsBlockhashValid", test_is_blockhash_valid()).await;

    println!();
    println!("--- Streaming Subscriptions ---");
    t.run("Subscribe Slots", test_subscribe_slots()).await;
    t.run("Subscribe Accounts (USDC)", test_subscribe_accounts()).await;
    t.run("Subscribe Transactions (System)", test_subscribe_transactions()).await;
    t.run("Subscribe Blocks Meta", test_subscribe_blocks_meta()).await;
    t.run("Subscribe Full Block", test_subscribe_full_block()).await;

    println!();
    println!("--- Advanced Stream Tests ---");
    t.run("Keepalive (ping/pong)", test_keepalive()).await;
    t.run("Re-subscribe (slots -> blocks_meta)", test_resubscribe()).await;
    t.run("Unsubscribe (empty filters)", test_unsubscribe()).await;

    t.summary();
    process::exit(t.exit_code());
}
