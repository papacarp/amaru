// Copyright 2025
//
// File-based logger for stake distribution snapshot events
// Saves stake distribution data to CBOR files when snapshots are created

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::{Event, Subscriber};
use tracing_subscriber::{
    layer::Context,
    Layer,
};
use serde::Serialize;
use cbor4ii::serde::to_writer;

/// File logger that captures stake distribution snapshot events and writes to CBOR files
/// Creates a separate file per epoch in subdirectory: `{base_parent}/{epoch}/snapshot.cbor`
/// 
/// Uses buffered writes (1MB buffer) for efficient disk I/O when handling large snapshots.
/// CBOR format provides ~50-60% size reduction compared to JSON.
pub struct SnapshotFileLogger {
    base_path: PathBuf,
    current_epoch: Arc<Mutex<Option<u64>>>,
    writer: Arc<Mutex<Option<BufWriter<File>>>>,
}

impl SnapshotFileLogger {
    /// Create a new file logger with a base path
    /// Files will be created as `{base_path}_snapshot_{epoch}.json` for each epoch
    pub fn new(base_path: PathBuf) -> Result<Self, std::io::Error> {
        Ok(Self {
            base_path,
            current_epoch: Arc::new(Mutex::new(None)),
            writer: Arc::new(Mutex::new(None)),
        })
    }
    
    fn get_snapshot_file_path(&self, epoch: u64) -> PathBuf {
        let parent = self.base_path.parent().unwrap_or(Path::new("."));
        let name = self.base_path.file_stem()
            .and_then(|n| n.to_str())
            .unwrap_or("snapshot");
        parent.join(epoch.to_string()).join(format!("{}.cbor", name))
    }
    
    fn open_snapshot_file(&self, epoch: u64) -> Result<BufWriter<File>, std::io::Error> {
        let file_path = self.get_snapshot_file_path(epoch);
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true) // Start fresh for each epoch
            .open(file_path)?;
        Ok(BufWriter::with_capacity(1024 * 1024, file)) // 1MB buffer
    }
    
    fn write_snapshot_data(&self, data: &StakeDistributionSnapshotData) -> Result<(), std::io::Error> {
        let mut current_epoch = self.current_epoch.lock().unwrap();
        let mut writer_opt = self.writer.lock().unwrap();
        
        // Check if we need to open a new file for this epoch
        if current_epoch.map(|e| e != data.epoch).unwrap_or(true) {
            // Close previous file if it exists
            if let Some(mut writer) = writer_opt.take() {
                let _ = writer.flush();
            }
            
            // Open new file for this epoch
            match self.open_snapshot_file(data.epoch) {
                Ok(writer) => {
                    *writer_opt = Some(writer);
                    *current_epoch = Some(data.epoch);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        
        // Write CBOR
        if let Some(ref mut writer) = *writer_opt {
            to_writer(&mut *writer, data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("CBOR encoding error: {}", e)))?;
            writer.flush()?;
        }
        
        Ok(())
    }
}

impl<S: Subscriber> Layer<S> for SnapshotFileLogger {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // Handle stake distribution snapshot events
        if event.metadata().target() == "amaru::ledger::state::stake_distribution" {
            // Extract the message to determine event type
            let mut message_visitor = MessageVisitor::default();
            event.record(&mut message_visitor);
            let message = message_visitor.message.as_deref();
            
            // Handle stake_distribution.snapshot event
            if message == Some("stake_distribution.snapshot") {
                // Extract all snapshot fields
                let mut visitor = StakeDistributionVisitor::default();
                event.record(&mut visitor);
                
                // Write snapshot data if we have all the required data
                if let Some(snapshot_data) = visitor.snapshot_data {
                    if let Err(e) = self.write_snapshot_data(&snapshot_data) {
                        eprintln!("Error writing stake distribution snapshot: {}", e);
                    }
                }
            }
        }
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: Option<String>,
}

impl tracing::field::Visit for MessageVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let formatted = format!("{:?}", value);
        if field.name() == "message" {
            let cleaned = formatted.trim_matches('"');
            self.message = Some(cleaned.to_string());
        }
    }
}

#[derive(Default)]
struct StakeDistributionVisitor {
    snapshot_data: Option<StakeDistributionSnapshotData>,
    epoch: Option<u64>,
    active_stake: Option<u64>,
    pools_voting_stake: Option<u64>,
    dreps_voting_stake: Option<u64>,
    accounts_json: Option<String>,
    pools_json: Option<String>,
}

#[derive(Debug, Serialize)]
struct StakeDistributionSnapshotData {
    epoch: u64,
    active_stake: u64,
    pools_voting_stake: u64,
    dreps_voting_stake: u64,
    accounts: serde_json::Value, // Will be parsed from JSON string
    pools: serde_json::Value,   // Will be parsed from JSON string
}

impl StakeDistributionVisitor {
    fn try_create_snapshot_data(&mut self) {
        if self.snapshot_data.is_some() {
            return;
        }
        
        if let (Some(epoch), Some(active_stake), Some(pools_voting_stake), 
                Some(dreps_voting_stake), Some(accounts_json), Some(pools_json)) = (
            self.epoch,
            self.active_stake,
            self.pools_voting_stake,
            self.dreps_voting_stake,
            self.accounts_json.as_ref(),
            self.pools_json.as_ref(),
        ) {
            // Parse JSON strings into serde_json::Value
            let accounts: serde_json::Value = match serde_json::from_str(accounts_json) {
                Ok(v) => v,
                Err(_) => return,
            };
            
            let pools: serde_json::Value = match serde_json::from_str(pools_json) {
                Ok(v) => v,
                Err(_) => return,
            };
            
            self.snapshot_data = Some(StakeDistributionSnapshotData {
                epoch,
                active_stake,
                pools_voting_stake,
                dreps_voting_stake,
                accounts,
                pools,
            });
        }
    }
}

impl tracing::field::Visit for StakeDistributionVisitor {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "epoch" => {
                self.epoch = Some(value);
            }
            "active_stake" => {
                self.active_stake = Some(value);
            }
            "pools_voting_stake" => {
                self.pools_voting_stake = Some(value);
            }
            "dreps_voting_stake" => {
                self.dreps_voting_stake = Some(value);
            }
            _ => {}
        }
        self.try_create_snapshot_data();
    }
    
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let formatted = format!("{:?}", value);
        let cleaned = formatted.trim_matches('"');
        
        match field.name() {
            "epoch" => {
                if let Ok(epoch) = cleaned.parse::<u64>() {
                    self.epoch = Some(epoch);
                }
            }
            "active_stake" => {
                if let Ok(val) = cleaned.parse::<u64>() {
                    self.active_stake = Some(val);
                }
            }
            "pools_voting_stake" => {
                if let Ok(val) = cleaned.parse::<u64>() {
                    self.pools_voting_stake = Some(val);
                }
            }
            "dreps_voting_stake" => {
                if let Ok(val) = cleaned.parse::<u64>() {
                    self.dreps_voting_stake = Some(val);
                }
            }
            "accounts_json" => {
                // This will be a JSON string representation
                self.accounts_json = Some(cleaned.to_string());
            }
            "pools_json" => {
                // This will be a JSON string representation
                self.pools_json = Some(cleaned.to_string());
            }
            _ => {}
        }
        self.try_create_snapshot_data();
    }
    
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "accounts_json" => {
                self.accounts_json = Some(value.to_string());
            }
            "pools_json" => {
                self.pools_json = Some(value.to_string());
            }
            _ => {}
        }
        self.try_create_snapshot_data();
    }
}

