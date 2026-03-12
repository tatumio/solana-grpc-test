use std::collections::HashMap;
use std::env;
use std::process;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use prost::Message as ProstMessage;
use yellowstone_grpc_client::ClientTlsConfig;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
    geyser_client::GeyserClient, subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::tonic;
use yellowstone_grpc_proto::tonic::transport::{Channel, Endpoint};

const DEFAULT_ENDPOINT: &str = "https://solana-mainnet-grpc.gateway.tatum.dev";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
const STREAM_TIMEOUT: Duration = Duration::from_secs(30);
const FULL_BLOCK_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_DECODE_SIZE: usize = 256 * 1024 * 1024;

fn get_endpoint() -> String {
    env::var("GRPC_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_string())
}

fn get_api_key() -> Result<String> {
    env::var("API_KEY").context("API_KEY env var is required for authentication")
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
        let key: tonic::metadata::AsciiMetadataValue = api_key
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid API key (not valid ASCII metadata)"))?;
        GeyserClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
            req.metadata_mut().insert("x-api-key", key.clone());
            Ok(req)
        })
        .max_decoding_message_size(MAX_DECODE_SIZE)
    }};
}

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
    let mut client = connect!();
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
    let mut stream = client
        .subscribe(tokio_stream::once(request))
        .await?
        .into_inner();
    let mut count = 0u32;
    let mut slots = Vec::new();

    tokio::time::timeout(STREAM_TIMEOUT, async {
        while let Some(msg) = stream.next().await {
            let msg = msg?;
            if let Some(UpdateOneof::Slot(slot)) = msg.update_oneof {
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

    Ok(format!("received {count} slots: {slots:?}"))
}

async fn test_subscribe_accounts() -> Result<String> {
    let mut client = connect!();
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
    let mut stream = client
        .subscribe(tokio_stream::once(request))
        .await?
        .into_inner();

    let detail = tokio::time::timeout(STREAM_TIMEOUT, async {
        while let Some(msg) = stream.next().await {
            let msg = msg?;
            if let Some(UpdateOneof::Account(acct_update)) = msg.update_oneof {
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
    let mut client = connect!();
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
    let mut stream = client
        .subscribe(tokio_stream::once(request))
        .await?
        .into_inner();
    let mut count = 0u32;
    let mut sigs = Vec::new();

    tokio::time::timeout(STREAM_TIMEOUT, async {
        while let Some(msg) = stream.next().await {
            let msg = msg?;
            if let Some(UpdateOneof::Transaction(tx_update)) = msg.update_oneof {
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

    let sigs_display: Vec<String> = sigs.iter().map(|s| s[..8.min(s.len())].to_string()).collect();
    Ok(format!("received {count} txs, sigs: {sigs_display:?}"))
}

async fn test_subscribe_blocks_meta() -> Result<String> {
    let mut client = connect!();
    let request = SubscribeRequest {
        blocks_meta: HashMap::from([(
            "bmeta_sub".to_string(),
            SubscribeRequestFilterBlocksMeta {},
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };
    let mut stream = client
        .subscribe(tokio_stream::once(request))
        .await?
        .into_inner();
    let mut count = 0u32;
    let mut details = Vec::new();

    tokio::time::timeout(STREAM_TIMEOUT, async {
        while let Some(msg) = stream.next().await {
            let msg = msg?;
            if let Some(UpdateOneof::BlockMeta(meta)) = msg.update_oneof {
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

    Ok(format!("received {count} blocks_meta\n         {}", details.join("\n         ")))
}

async fn test_subscribe_full_block() -> Result<String> {
    let mut client = connect!();
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
    let mut stream = client
        .subscribe(tokio_stream::once(request))
        .await?
        .into_inner();

    let detail = tokio::time::timeout(FULL_BLOCK_TIMEOUT, async {
        while let Some(msg) = stream.next().await {
            let msg = msg?;
            if let Some(UpdateOneof::Block(block)) = msg.update_oneof {
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
    .context("timed out waiting for full block (120s)")??;

    Ok(detail)
}

// --- Main ---

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let endpoint = get_endpoint();
    println!("=== Yellowstone gRPC Proxy Test ===");
    println!("Endpoint: {endpoint}");
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

    t.summary();
    process::exit(t.exit_code());
}
