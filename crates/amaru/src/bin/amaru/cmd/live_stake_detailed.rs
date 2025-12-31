// Copyright 2025
//
// Command to output detailed live stake data including delegator lists

use amaru::live_stake_tracker;
use amaru_kernel::network::NetworkName;
use clap::Parser;
use hex;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::cmd::default_ledger_dir;

#[derive(Debug, Parser)]
pub struct Args {
    /// Network for which to calculate stake.
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
}

pub fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let ledger_dir = args
        .ledger_dir
        .unwrap_or_else(|| default_ledger_dir(args.network).into());

    match live_stake_tracker::calculate_detailed_pool_data(&ledger_dir) {
        Ok((pool_data, treasury, reserves)) => {
            // Convert to JSON format
            // Pool IDs and stake credentials are stored as hex strings in JSON
            // Delegator lists map hex strings to lovelace values
            let mut pools_json: BTreeMap<String, serde_json::Value> = BTreeMap::new();
            
            for (pool_id, data) in &pool_data {
                let pool_id_hex = hex::encode(pool_id.as_slice());
                
                // Convert delegator list to JSON (hex string -> lovelace)
                let mut delegator_list_json = BTreeMap::new();
                for (credential_bytes, stake) in &data.delegator_list {
                    let credential_hex = hex::encode(credential_bytes);
                    delegator_list_json.insert(credential_hex, json!(stake));
                }
                
                pools_json.insert(
                    pool_id_hex,
                    json!({
                        "cp": data.pledge,
                        "ap": data.actual_pledge,
                        "ls": data.live_stake,
                        "dl": delegator_list_json,
                    }),
                );
            }
            
            let output = json!({
                "pool_data": pools_json,
                "treasury": treasury,
                "reserves": reserves,
            });
            
            println!("{}", serde_json::to_string(&output)?);
            Ok(())
        }
        Err(e) => {
            let error_msg = format!("{}", e);
            eprintln!("\nError calculating detailed stake: {}", error_msg);
            eprintln!("\nThis may occur if:");
            eprintln!("  - The database is locked by the running node (try again in a moment)");
            eprintln!("  - The database path is incorrect");
            eprintln!("  - The database is corrupted or incomplete");
            Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, error_msg)))
        }
    }
}


