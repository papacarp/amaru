// Copyright 2025
//
// Live stake tracking module that calculates stake by pool ID

use amaru_kernel::{output_stake_credential, HasLovelace, Lovelace, PoolId, StakeCredential};
use amaru_ledger::store::{ReadStore, StoreError};
use amaru_stores::rocksdb::{ReadOnlyRocksDB, RocksDbConfig};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Sum stake by pool ID from the UTxO set
/// 
/// This function opens a read-only connection to the store, iterates over all UTxOs,
/// extracts stake credentials, looks up account delegations, and sums stake by pool ID.
/// 
/// **Note:** This function uses read-only database access and should not interfere with
/// a running node. However, if the node is actively syncing, the data may be slightly
/// inconsistent as it reflects a point-in-time snapshot of the database state.
pub fn calculate_stake_by_pool(
    store_path: &PathBuf,
) -> Result<BTreeMap<PoolId, Lovelace>, StoreError> {
    let mut stake_by_pool: BTreeMap<PoolId, Lovelace> = BTreeMap::new();
    let mut stake_by_credential: BTreeMap<StakeCredential, Lovelace> = BTreeMap::new();

    // Open read-only connection to the store
    // This uses RocksDB's open_for_read_only which uses shared locks and should not
    // conflict with a running node, though data may be slightly inconsistent during sync
    let config = RocksDbConfig::new(store_path.clone());
    let db = ReadOnlyRocksDB::new(config)
        .map_err(|e| {
            StoreError::Internal(format!(
                "Failed to open read-only database connection. \
                 If the node is running, ensure it's not holding exclusive locks. \
                 Original error: {}",
                e
            ).into())
        })?;

    // First pass: sum stake by credential from UTxO
    let utxos = db.iter_utxos()?;
    for (_, output) in utxos {
        if let Some(credential) = output_stake_credential(&output) {
            let value = output.lovelace();
            *stake_by_credential.entry(credential).or_insert(0) += value;
        }
    }

    // Second pass: map credentials to pools via accounts
    for (credential, total_stake) in stake_by_credential {
        if let Ok(Some(account)) = db.account(&credential) {
            if let Some((pool_id, _)) = account.pool {
                *stake_by_pool.entry(pool_id).or_insert(0) += total_stake;
            }
        }
    }

    Ok(stake_by_pool)
}

