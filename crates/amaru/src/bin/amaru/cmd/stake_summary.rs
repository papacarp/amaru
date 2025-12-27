// Copyright 2025
//
// Command to calculate and display live stake by pool ID

use amaru::live_stake_tracker::PoolStakeData;
use amaru::live_stake_tracker;
use amaru_kernel::network::NetworkName;
use amaru_ledger::summary::serde::encode_pool_id;
use clap::Parser;
use hex;
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::cmd::default_ledger_dir;

#[derive(Debug, Parser)]
pub struct Args {
    /// Network for which to calculate stake.
    ///
    /// Should be one of 'mainnet', 'preprod', 'preview' or 'testnet_<magic>' where
    /// `magic` is a 32-bits unsigned value denoting a particular testnet.
    #[arg(
        long,
        value_name = "NETWORK",
        env = "AMARU_NETWORK",
        default_value_t = super::DEFAULT_NETWORK,
    )]
    network: NetworkName,

    /// Path of the ledger on-disk storage.
    #[arg(long, value_name = "DIR", env = "AMARU_LEDGER_DIR")]
    ledger_dir: Option<PathBuf>,

    /// Output results as JSON instead of human-readable text.
    #[arg(long)]
    json: bool,
}

pub fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let ledger_dir = args
        .ledger_dir
        .unwrap_or_else(|| default_ledger_dir(args.network).into());

    if !args.json {
        println!("Calculating stake by pool from ledger at: {}", ledger_dir.display());
        println!("(This may take a moment if the database is large...)\n");
        println!("Note: Using read-only access. Data may be slightly inconsistent if the node is actively syncing.\n");
    }

    match live_stake_tracker::calculate_stake_by_pool(&ledger_dir) {
        Ok(stake_by_pool) => {
            if args.json {
                output_json(&stake_by_pool)?;
            } else {
                output_text(&stake_by_pool)?;
            }
            Ok(())
        }
        Err(e) => {
            let error_msg = format!("{}", e);
            eprintln!("\nError calculating stake: {}", error_msg);
            eprintln!("\nThis may occur if:");
            eprintln!("  - The database is locked by the running node (try again in a moment)");
            eprintln!("  - The database path is incorrect");
            eprintln!("  - The database is corrupted or incomplete");
            eprintln!("\nThe command uses read-only access and should not interfere with a running node.");
            Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, error_msg)))
        }
    }
}

fn output_text(stake_by_pool: &BTreeMap<amaru_kernel::PoolId, PoolStakeData>) -> Result<(), Box<dyn std::error::Error>> {
    let total_stake: u64 = stake_by_pool.values().map(|data| data.stake).sum();
    let total_pools = stake_by_pool.len();

    println!("Stake Distribution Summary:");
    println!("  Total Pools: {}", total_pools);
    println!("  Total Stake: {} lovelace ({} ADA)", total_stake, total_stake as f64 / 1_000_000.0);
    println!("\nStake by Pool ID:");
    println!("{:-<80}", "");
    
    // Sort by stake (descending) for easier reading
    let mut sorted: Vec<_> = stake_by_pool.iter().collect();
    sorted.sort_by(|a, b| b.1.stake.cmp(&a.1.stake));

    for (pool_id, pool_data) in sorted {
        let ada = pool_data.stake as f64 / 1_000_000.0;
        let pledge_ada = pool_data.current_pledge as f64 / 1_000_000.0;
        let pool_id_bech32 = encode_pool_id(pool_id);
        println!("  {}: {:>20} lovelace ({:>15.2} ADA), pledge: {:>20} lovelace ({:>15.2} ADA)", 
            pool_id_bech32,
            pool_data.stake,
            ada,
            pool_data.current_pledge,
            pledge_ada
        );
    }

    Ok(())
}

fn output_json(stake_by_pool: &BTreeMap<amaru_kernel::PoolId, PoolStakeData>) -> Result<(), Box<dyn std::error::Error>> {
    use serde_json::json;
    
    let total_stake: u64 = stake_by_pool.values().map(|data| data.stake).sum();
    
    // Convert pool IDs to bech32 strings and sort by stake (descending)
    let mut pools: Vec<_> = stake_by_pool
        .iter()
        .map(|(pool_id, pool_data)| {
            json!({
                "pool_id": encode_pool_id(pool_id),
                "pool_id_hex": hex::encode(pool_id.as_slice()),
                "stake_lovelace": pool_data.stake,
                "stake_ada": pool_data.stake as f64 / 1_000_000.0,
                "current_pledge_lovelace": pool_data.current_pledge,
                "current_pledge_ada": pool_data.current_pledge as f64 / 1_000_000.0
            })
        })
        .collect();
    pools.sort_by(|a, b| {
        let stake_a = a["stake_lovelace"].as_u64().unwrap_or(0);
        let stake_b = b["stake_lovelace"].as_u64().unwrap_or(0);
        stake_b.cmp(&stake_a)
    });

    let output = json!({
        "total_pools": stake_by_pool.len(),
        "total_stake_lovelace": total_stake,
        "total_stake_ada": total_stake as f64 / 1_000_000.0,
        "pools": pools
    });

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

