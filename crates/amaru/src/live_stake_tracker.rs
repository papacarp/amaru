// Copyright 2025
//
// Live stake tracking module that calculates stake by pool ID

use amaru_kernel::{output_stake_credential, HasLovelace, Lovelace, PoolId, StakeCredential};
use amaru_ledger::store::{ReadStore, StoreError};
use amaru_stores::rocksdb::{ReadOnlyRocksDB, RocksDbConfig};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Pool stake data including total stake and current pledge
#[derive(Debug, Clone)]
pub struct PoolStakeData {
    pub stake: Lovelace,
    pub current_pledge: Lovelace,
}

/// Sum stake by pool ID from accounts and UTxO set, and calculate current pledge
/// 
/// This function opens a read-only connection to the store, iterates over all accounts,
/// initializes stake with account rewards, adds UTxO values, and sums stake by pool ID.
/// It also calculates the current pledge for each pool (sum of owner addresses' stake).
/// This matches the approach used in stake distribution snapshots.
/// 
/// **Note:** This function uses read-only database access and should not interfere with
/// a running node. However, if the node is actively syncing, the data may be slightly
/// inconsistent as it reflects a point-in-time snapshot of the database state.
pub fn calculate_stake_by_pool(
    store_path: &PathBuf,
) -> Result<BTreeMap<PoolId, PoolStakeData>, StoreError> {
    let mut stake_by_pool: BTreeMap<PoolId, PoolStakeData> = BTreeMap::new();
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

    // First pass: initialize stake from accounts (includes rewards that haven't been withdrawn)
    // This matches the stake distribution approach which starts with accounts, not UTxO
    for (credential, account) in db.iter_accounts()? {
        // Initialize with account rewards (rewards that haven't been withdrawn yet)
        stake_by_credential.insert(credential, account.rewards);
    }

    // Second pass: add UTxO values to existing accounts (or create new entries)
    let utxos = db.iter_utxos()?;
    for (_, output) in utxos {
        if let Some(credential) = output_stake_credential(&output) {
            let value = output.lovelace();
            *stake_by_credential.entry(credential).or_insert(0) += value;
        }
    }

    // Third pass: map credentials to pools via accounts
    for (credential, total_stake) in &stake_by_credential {
        if let Ok(Some(account)) = db.account(credential) {
            if let Some((pool_id, _)) = account.pool {
                stake_by_pool
                    .entry(pool_id)
                    .or_insert_with(|| PoolStakeData {
                        stake: 0,
                        current_pledge: 0,
                    })
                    .stake += total_stake;
            }
        }
    }

    // Fourth pass: calculate current pledge for each pool
    // Current pledge = sum of stake for all owner addresses delegated to the pool
    for (pool_id, pool_data) in stake_by_pool.iter_mut() {
        if let Ok(Some(pool_row)) = db.pool(pool_id) {
            let pool_params = &pool_row.current_params;
            
            // Sum stake for all owner addresses
            let mut owner_stake_total = 0u64;
            for owner_hash in &pool_params.owners {
                let owner_credential = StakeCredential::AddrKeyhash(*owner_hash);
                // Get stake for this owner from our already-calculated stake_by_credential map
                if let Some(owner_stake) = stake_by_credential.get(&owner_credential) {
                    // Verify owner is delegated to this pool
                    if let Ok(Some(account)) = db.account(&owner_credential) {
                        if let Some((delegated_pool_id, _)) = account.pool {
                            if delegated_pool_id == *pool_id {
                                owner_stake_total += owner_stake;
                            }
                        }
                    }
                }
            }
            
            pool_data.current_pledge = owner_stake_total;
        }
    }

    Ok(stake_by_pool)
}

/// Detailed pool data including delegator lists
#[derive(Debug, Clone)]
pub struct DetailedPoolData {
    pub pledge: Lovelace,  // cp: current pledge requirement from pool params
    pub actual_pledge: Lovelace,  // ap: actual pledge (sum of owner stake)
    pub live_stake: Lovelace,  // ls: total live stake
    pub delegator_list: BTreeMap<Vec<u8>, Lovelace>,  // dl: delegator stake credential bytes -> stake
}

/// Calculate detailed stake data including delegator lists for each pool
/// 
/// This function calculates:
/// - cp: pledge requirement from pool parameters
/// - ap: actual pledge (sum of stake from pool owners)
/// - ls: live stake (total stake delegated to pool)
/// - dl: delegator list (map of stake credential bytes to their stake)
/// 
/// Also returns treasury and reserves values.
pub fn calculate_detailed_pool_data(
    store_path: &PathBuf,
) -> Result<(BTreeMap<PoolId, DetailedPoolData>, Lovelace, Lovelace), StoreError> {
    let mut pool_data: BTreeMap<PoolId, DetailedPoolData> = BTreeMap::new();
    let mut stake_by_credential: BTreeMap<StakeCredential, Lovelace> = BTreeMap::new();
    let mut pool_delegator_stake: BTreeMap<PoolId, BTreeMap<StakeCredential, Lovelace>> = BTreeMap::new();

    // Open read-only connection to the store
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

    // First pass: initialize stake from accounts (includes rewards)
    for (credential, account) in db.iter_accounts()? {
        stake_by_credential.insert(credential, account.rewards);
    }

    // Second pass: add UTxO values to existing accounts
    let utxos = db.iter_utxos()?;
    for (_, output) in utxos {
        if let Some(credential) = output_stake_credential(&output) {
            let value = output.lovelace();
            *stake_by_credential.entry(credential).or_insert(0) += value;
        }
    }

    // Third pass: map credentials to pools and build delegator lists
    for (credential, total_stake) in &stake_by_credential {
        if let Ok(Some(account)) = db.account(credential) {
            if let Some((pool_id, _)) = account.pool {
                pool_delegator_stake
                    .entry(pool_id)
                    .or_insert_with(BTreeMap::new)
                    .insert(credential.clone(), *total_stake);
            }
        }
    }

    // Fourth pass: build pool data with pledge info for all registered pools
    let pots = db.pots()?;
    
    // Iterate over all pools, not just those with delegators
    for (pool_id, pool_row) in db.iter_pools()? {
        let pool_params = &pool_row.current_params;
        let delegators = pool_delegator_stake.get(&pool_id).cloned().unwrap_or_default();
        
        // Calculate actual pledge (sum of owner stake)
        let mut actual_pledge = 0u64;
        for owner_hash in &pool_params.owners {
            let owner_credential = StakeCredential::AddrKeyhash(*owner_hash);
            if let Some(owner_stake) = delegators.get(&owner_credential) {
                actual_pledge += owner_stake;
            }
        }
        
        // Calculate total live stake
        let live_stake: Lovelace = delegators.values().sum();
        
        // Convert delegator list to bytes
        let mut delegator_list_bytes = BTreeMap::new();
        for (credential, stake) in &delegators {
            let credential_bytes = match credential {
                StakeCredential::AddrKeyhash(hash) => hash.as_slice().to_vec(),
                StakeCredential::ScriptHash(hash) => hash.as_slice().to_vec(),
            };
            delegator_list_bytes.insert(credential_bytes, *stake);
        }
        
        pool_data.insert(
            pool_id,
            DetailedPoolData {
                pledge: pool_params.pledge,
                actual_pledge,
                live_stake,
                delegator_list: delegator_list_bytes,
            },
        );
    }

    Ok((pool_data, pots.treasury, pots.reserves))
}

