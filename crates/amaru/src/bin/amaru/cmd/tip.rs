// Copyright 2025 PRAGMA
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use amaru::{default_chain_dir, DEFAULT_NETWORK};
use amaru_ouroboros::ReadOnlyChainStore;
use amaru_kernel::NetworkName;
use amaru_kernel::{BlockHeader, EraHistory, IsHeader, Slot};
use amaru_stores::rocksdb::RocksDbConfig;
use amaru_stores::rocksdb::consensus::{ReadOnlyChainDB, RocksDBStore};
use clap::Parser;
use serde::Serialize;
use std::{error::Error, path::PathBuf};

#[derive(Debug, Serialize)]
struct TipInfo {
    slot: u64,
    epoch: u64,
    tip_hash: String,
    network: String,
}

#[derive(Debug, Parser)]
pub struct Args {
    /// Network for which we are querying the tip.
    ///
    /// Should be one of 'mainnet', 'preprod', 'preview' or 'testnet_<magic>' where
    /// `magic` is a 32-bits unsigned value denoting a particular testnet.
    #[arg(
        long,
        value_name = "NETWORK",
        env = "AMARU_NETWORK",
        default_value_t = DEFAULT_NETWORK,
    )]
    network: NetworkName,

    /// The path to the chain database
    #[arg(long, value_name = "DIR", env = "AMARU_CHAIN_DIR")]
    chain_dir: Option<PathBuf>,

    /// Output results as JSON
    #[arg(long)]
    json: bool,
}

pub fn run(args: Args) -> Result<(), Box<dyn Error>> {
    // Determine the chain directory to use and detect the network
    let (chain_dir, detected_network) = if let Some(ref dir) = args.chain_dir {
        // If chain_dir is explicitly provided, use the network from args
        (dir.clone(), args.network)
    } else {
        // Try to auto-detect by checking which chain databases exist
        let mainnet_dir = default_chain_dir(NetworkName::Mainnet);
        let preprod_dir = default_chain_dir(NetworkName::Preprod);
        let preview_dir = default_chain_dir(NetworkName::Preview);
        
        // Try mainnet first (most common for production)
        if std::path::Path::new(&mainnet_dir).exists() {
            if !args.json {
                eprintln!("Note: Auto-detected mainnet chain database at {}", mainnet_dir);
            }
            (mainnet_dir.into(), NetworkName::Mainnet)
        } else if std::path::Path::new(&preprod_dir).exists() {
            if !args.json {
                eprintln!("Note: Auto-detected preprod chain database at {}", preprod_dir);
            }
            (preprod_dir.into(), NetworkName::Preprod)
        } else if std::path::Path::new(&preview_dir).exists() {
            if !args.json {
                eprintln!("Note: Auto-detected preview chain database at {}", preview_dir);
            }
            (preview_dir.into(), NetworkName::Preview)
        } else {
            // Fall back to the default based on network argument
            (default_chain_dir(args.network).into(), args.network)
        }
    };

    let config = RocksDbConfig::new(chain_dir.clone());
    let db: ReadOnlyChainDB = RocksDBStore::open_for_readonly(&config)
        .map_err(|e| format!(
            "Failed to open chain database at {}: {}\n\
            Hint: If you're running mainnet, use: amaru tip --network mainnet\n\
            Or specify the database path with: amaru tip --chain-dir /path/to/chain.db",
            chain_dir.to_string_lossy(),
            e
        ))?;

    // Get the tip hash
    let tip_hash = <ReadOnlyChainDB as ReadOnlyChainStore<BlockHeader>>::get_best_chain_hash(&db);

    // Load the tip header
    let tip_header: BlockHeader = db
        .load_header(&tip_hash)
        .ok_or_else(|| format!("Failed to load tip header: {}", tip_hash))?;

    // Get the slot from the header (already a Slot)
    let slot: Slot = tip_header.slot();
    let slot_u64: u64 = slot.into();

    // Get the era history for the detected network and convert slot to epoch
    let era_history = (*Into::<&'static EraHistory>::into(detected_network)).clone();
    let epoch = era_history
        .slot_to_epoch(slot, slot)
        .map_err(|e| format!("Failed to convert slot to epoch: {:?}", e))?;
    let epoch_u64: u64 = epoch.into();

    // Output the results
    if args.json {
        let tip_info = TipInfo {
            slot: slot_u64,
            epoch: epoch_u64,
            tip_hash: tip_hash.to_string(),
            network: detected_network.to_string(),
        };
        println!("{}", serde_json::to_string(&tip_info)?);
    } else {
        println!("Current Slot: {}", slot_u64);
        println!("Current Epoch: {}", epoch_u64);
        println!("Tip Hash: {}", tip_hash);
    }

    Ok(())
}


