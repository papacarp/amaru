// Copyright 2025
//
// File-based logger for rewards breakdown events
// Efficiently logs all stake key rewards to a file with buffered writes
// Creates a separate file per epoch with a terminal marker when complete

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::{Event, Subscriber};
use tracing_subscriber::{
    layer::Context,
    Layer,
};
use serde::Serialize;

/// File logger that captures rewards breakdown events and writes to CSV files
/// Format: stake_credential_hex,leader_reward,stake_reward\n
/// 
/// Creates a separate file per epoch: `{base}_{epoch}.csv`
/// Writes a terminal marker `0,0,0` when rewards calculation for an epoch completes.
/// 
/// Also creates a summary file `{base}_summary_{epoch}.json` for each epoch containing
/// aggregate rewards statistics in JSON format.
/// 
/// Uses buffered writes (1MB buffer) for efficient disk I/O when handling ~1.3M entries per epoch.
pub struct RewardsFileLogger {
    base_path: PathBuf,
    current_epoch: Arc<Mutex<Option<u64>>>,
    writer: Arc<Mutex<Option<BufWriter<File>>>>,
    line_count: Arc<Mutex<usize>>,
    summary_writer: Arc<Mutex<Option<BufWriter<File>>>>,
    summary_epoch: Arc<Mutex<Option<u64>>>,
}

impl RewardsFileLogger {
    /// Create a new file logger with a base path
    /// Files will be created as `{base_path}_{epoch}.csv` for each epoch
    /// Summary files will be created as `{base_path}_summary_{epoch}.csv` for each epoch
    pub fn new(base_path: PathBuf) -> Result<Self, std::io::Error> {
        Ok(Self {
            base_path,
            current_epoch: Arc::new(Mutex::new(None)),
            writer: Arc::new(Mutex::new(None)),
            line_count: Arc::new(Mutex::new(0)),
            summary_writer: Arc::new(Mutex::new(None)),
            summary_epoch: Arc::new(Mutex::new(None)),
        })
    }
    
    fn get_summary_file_path(&self, epoch: u64) -> PathBuf {
        let base = self.base_path.with_extension("");
        let mut path = base.to_path_buf();
        path.set_file_name(format!("{}_summary_{}.json",
            path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("rewards_breakdown"),
            epoch
        ));
        path
    }
    
    fn open_summary_file(&self, epoch: u64) -> Result<BufWriter<File>, std::io::Error> {
        let file_path = self.get_summary_file_path(epoch);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true) // Start fresh for each epoch
            .open(file_path)?;
        Ok(BufWriter::with_capacity(8192, file)) // 8KB buffer for summary
    }
    
    fn write_summary_line(&self, data: &RewardsSummaryData) -> Result<(), std::io::Error> {
        let mut summary_epoch = self.summary_epoch.lock().unwrap();
        let mut writer_opt = self.summary_writer.lock().unwrap();
        
        // Check if we need to open a new file for this epoch
        if summary_epoch.map(|e| e != data.epoch).unwrap_or(true) {
            // Close previous file if it exists
            if let Some(mut writer) = writer_opt.take() {
                let _ = writer.flush();
            }
            
            // Open new file for this epoch
            match self.open_summary_file(data.epoch) {
                Ok(writer) => {
                    *writer_opt = Some(writer);
                    *summary_epoch = Some(data.epoch);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        
        // Write JSON
        if let Some(ref mut writer) = *writer_opt {
            let json = serde_json::to_string_pretty(data)?;
            writer.write_all(json.as_bytes())?;
            writer.write_all(b"\n")?;
            writer.flush()?;
        }
        Ok(())
    }

    fn get_epoch_file_path(&self, epoch: u64) -> PathBuf {
        // Remove .csv extension if present, then add epoch and .csv
        let base = self.base_path.with_extension("");
        let mut path = base.to_path_buf();
        path.set_file_name(format!("{}_{}.csv", 
            path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("rewards_breakdown"),
            epoch
        ));
        path
    }

    fn open_epoch_file(&self, epoch: u64) -> Result<BufWriter<File>, std::io::Error> {
        let file_path = self.get_epoch_file_path(epoch);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true) // Start fresh for each epoch
            .open(file_path)?;
        
        // Use a large buffer (1MB) to minimize disk writes for ~1.3M entries
        // Each line is approximately: 56 chars (hex) + 20 (leader) + 20 (stake) + 2 (commas/newline) = ~98 bytes
        // 1MB buffer can hold ~10,000 lines before flushing
        Ok(BufWriter::with_capacity(1_048_576, file))
    }

    fn write_line(&self, line: &str) -> Result<(), std::io::Error> {
        if let Ok(mut writer_opt) = self.writer.lock() {
            if let Some(ref mut writer) = *writer_opt {
                writer.write_all(line.as_bytes())?;
                
                // Auto-flush every 10,000 lines to prevent memory issues
                let mut count = self.line_count.lock().unwrap();
                *count += 1;
                if *count >= 10_000 {
                    writer.flush()?;
                    *count = 0;
                }
            }
        }
        Ok(())
    }

    fn write_terminal_marker(&self) -> Result<(), std::io::Error> {
        self.write_line("0,0,0\n")?;
        // Final flush to ensure terminal marker is written
        if let Ok(mut writer_opt) = self.writer.lock() {
            if let Some(ref mut writer) = *writer_opt {
                writer.flush()?;
            }
        }
        Ok(())
    }
}

impl<S: Subscriber> Layer<S> for RewardsFileLogger {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // Handle rewards events - match on target and extract message field to identify event type
        if event.metadata().target() == "amaru::ledger::state::rewards" {
            // First, extract the message to determine event type
            let mut message_visitor = EpochVisitor::default();
            event.record(&mut message_visitor);
            let message = message_visitor.message.as_deref();
            
            // Handle rewards.summary event to detect epoch and completion
            if message == Some("rewards.summary") {
                // Extract all summary fields
                let mut summary_visitor = RewardsSummaryVisitor::default();
                event.record(&mut summary_visitor);
                
                // Write summary line if we have all the data
                if let Some(summary_data) = summary_visitor.summary_data {
                    if let Err(e) = self.write_summary_line(&summary_data) {
                        eprintln!("Error writing rewards summary: {}", e);
                    }
                }
            
                if let Some(epoch) = message_visitor.epoch {
                    let mut current_epoch = self.current_epoch.lock().unwrap();
                    
                    // Check if this is a new epoch
                    let is_new_epoch = current_epoch.map(|e| e != epoch).unwrap_or(true);
                    
                    if is_new_epoch {
                        // Close previous file if it exists and write terminal marker
                        if let Ok(mut writer_opt) = self.writer.lock() {
                            if let Some(mut writer) = writer_opt.take() {
                                // Write terminal marker for previous epoch
                                let _ = writer.write_all(b"0,0,0\n");
                                let _ = writer.flush();
                            }
                        }
                        
                        // Close previous summary file if it exists
                        if let Ok(mut summary_writer_opt) = self.summary_writer.lock() {
                            if let Some(mut summary_writer) = summary_writer_opt.take() {
                                let _ = summary_writer.flush();
                            }
                        }
                        *self.summary_epoch.lock().unwrap() = None;
                        
                        // Open new file for this epoch
                        match self.open_epoch_file(epoch) {
                            Ok(writer) => {
                                *self.writer.lock().unwrap() = Some(writer);
                                *current_epoch = Some(epoch);
                                *self.line_count.lock().unwrap() = 0;
                            }
                            Err(e) => {
                                eprintln!("Error opening rewards file for epoch {}: {}", epoch, e);
                            }
                        }
                    }
                    
                    if let Err(e) = self.write_terminal_marker() {
                        eprintln!("Error writing terminal marker for epoch {}: {}", epoch, e);
                    }
                }
                return;
            }
            
            // Handle account breakdown events - check message field
            if message == Some("rewards.account_breakdown") {
                let mut visitor = RewardsAccountVisitor::default();
                event.record(&mut visitor);

                // Check if this event includes an epoch (leader breakdowns do, member breakdowns don't)
                let epoch_from_event = visitor.epoch;
            
                // Open file if we have an epoch from this event and don't have a file open yet
                if let Some(epoch) = epoch_from_event {
                    let mut current_epoch = self.current_epoch.lock().unwrap();
                    if current_epoch.map(|e| e != epoch).unwrap_or(true) {
                        // Close previous file if it exists
                        if let Ok(mut writer_opt) = self.writer.lock() {
                            if let Some(mut writer) = writer_opt.take() {
                                let _ = writer.write_all(b"0,0,0\n");
                                let _ = writer.flush();
                            }
                        }
                        
                    match self.open_epoch_file(epoch) {
                        Ok(writer) => {
                            *self.writer.lock().unwrap() = Some(writer);
                            *current_epoch = Some(epoch);
                            *self.line_count.lock().unwrap() = 0;
                        }
                        Err(e) => {
                            eprintln!("Error opening rewards file for epoch {}: {}", epoch, e);
                            return;
                        }
                    }
                    }
                    drop(current_epoch); // Release lock after opening file
                }
                
            let current_epoch = self.current_epoch.lock().unwrap();
            if current_epoch.is_none() {
                return;
            }
                drop(current_epoch); // Release lock before processing

                if let Some(account_data) = visitor.account_data {
                    // Format: stake_credential_hex,leader_reward,stake_reward\n
                    let line = format!(
                        "{},{},{}\n",
                        account_data.stake_credential_hex,
                        account_data.leader_reward,
                        account_data.stake_reward
                    );

                    if let Err(e) = self.write_line(&line) {
                        eprintln!("Error writing to rewards file: {}", e);
                    }
                }
            }
        }
    }
}

#[derive(Default)]
struct EpochVisitor {
    epoch: Option<u64>,
    message: Option<String>,
}

impl tracing::field::Visit for EpochVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "epoch" {
            self.epoch = Some(value);
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let formatted = format!("{:?}", value);
        if field.name() == "message" {
            let cleaned = formatted.trim_matches('"');
            self.message = Some(cleaned.to_string());
        } else if field.name() == "epoch" {
            // Try to parse Debug-formatted epoch
            if let Ok(epoch) = formatted.parse::<u64>() {
                self.epoch = Some(epoch);
            }
        }
    }
}

#[derive(Default)]
struct RewardsAccountVisitor {
    account_data: Option<RewardsAccountData>,
    epoch: Option<u64>,
    stake_credential_hex: Option<String>,
    leader_reward: Option<u64>,
    stake_reward: Option<u64>,
    message: Option<String>,
}

#[derive(Debug)]
struct RewardsAccountData {
    stake_credential_hex: String,
    leader_reward: u64,
    stake_reward: u64,
}

impl RewardsAccountVisitor {
    fn try_create_account_data(&mut self) {
        if self.account_data.is_some() {
            return;
        }

        if let (Some(stake_credential_hex), Some(leader_reward), Some(stake_reward)) = (
            self.stake_credential_hex.clone(),
            self.leader_reward,
            self.stake_reward,
        ) {
            self.account_data = Some(RewardsAccountData {
                stake_credential_hex,
                leader_reward,
                stake_reward,
            });
        }
    }
}

impl tracing::field::Visit for RewardsAccountVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "stake_credential_hex" {
            self.stake_credential_hex = Some(value.to_string());
            self.try_create_account_data();
        } else if field.name() == "message" {
            self.message = Some(value.to_string());
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "epoch" => {
                self.epoch = Some(value);
            }
            "leader_reward" => {
                self.leader_reward = Some(value);
                self.try_create_account_data();
            }
            "stake_reward" => {
                self.stake_reward = Some(value);
                self.try_create_account_data();
            }
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let formatted = format!("{:?}", value);
        if field.name() == "stake_credential_hex" {
            let cleaned = formatted.trim_matches('"');
            self.stake_credential_hex = Some(cleaned.to_string());
            self.try_create_account_data();
        } else if field.name() == "message" {
            let cleaned = formatted.trim_matches('"');
            self.message = Some(cleaned.to_string());
        } else if field.name() == "epoch" {
            if let Ok(epoch) = formatted.parse::<u64>() {
                self.epoch = Some(epoch);
            }
        } else if field.name() == "leader_reward" || field.name() == "stake_reward" {
            // These come through as Display-formatted, so they're strings in Debug format
            // Try to parse as u64
            if let Ok(val) = formatted.parse::<u64>() {
                if field.name() == "leader_reward" {
                    self.leader_reward = Some(val);
                } else {
                    self.stake_reward = Some(val);
                }
                self.try_create_account_data();
            }
        }
    }
}

#[derive(Default)]
struct RewardsSummaryVisitor {
    summary_data: Option<RewardsSummaryData>,
    epoch: Option<u64>,
    efficiency: Option<String>,
    incentives: Option<u64>,
    treasury_tax: Option<u64>,
    total_rewards: Option<u64>,
    available_rewards: Option<u64>,
    effective_rewards: Option<u64>,
    pots_reserves: Option<u64>,
    pots_treasury: Option<u64>,
    pots_fees: Option<u64>,
}

#[derive(Debug, Serialize)]
struct RewardsSummaryData {
    epoch: u64,
    efficiency_numerator: u64,
    efficiency_denominator: u64,
    incentives: u64,
    treasury_tax: u64,
    total_rewards: u64,
    available_rewards: u64,
    effective_rewards: u64,
    pots_reserves: u64,
    pots_treasury: u64,
    pots_fees: u64,
}

impl RewardsSummaryVisitor {
    fn try_create_summary_data(&mut self) {
        if self.summary_data.is_some() {
            return;
        }
        
        if let (Some(epoch), Some(efficiency), Some(incentives), Some(treasury_tax),
                Some(total_rewards), Some(available_rewards), Some(effective_rewards),
                Some(pots_reserves), Some(pots_treasury), Some(pots_fees)) = (
            self.epoch,
            self.efficiency.as_ref(),
            self.incentives,
            self.treasury_tax,
            self.total_rewards,
            self.available_rewards,
            self.effective_rewards,
            self.pots_reserves,
            self.pots_treasury,
            self.pots_fees,
        ) {
            // Parse efficiency fraction (e.g., "5293/5400")
            let (num, den) = if let Some((n, d)) = efficiency.split_once('/') {
                if let (Ok(numerator), Ok(denominator)) = (n.parse::<u64>(), d.parse::<u64>()) {
                    (numerator, denominator)
                } else {
                    return;
                }
            } else {
                return;
            };
            
            self.summary_data = Some(RewardsSummaryData {
                epoch,
                efficiency_numerator: num,
                efficiency_denominator: den,
                incentives,
                treasury_tax,
                total_rewards,
                available_rewards,
                effective_rewards,
                pots_reserves,
                pots_treasury,
                pots_fees,
            });
        }
    }
}

impl tracing::field::Visit for RewardsSummaryVisitor {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "epoch" => {
                self.epoch = Some(value);
            }
            "incentives" => {
                self.incentives = Some(value);
            }
            "treasury_tax" => {
                self.treasury_tax = Some(value);
            }
            "total_rewards" => {
                self.total_rewards = Some(value);
            }
            "available_rewards" => {
                self.available_rewards = Some(value);
            }
            "effective_rewards" => {
                self.effective_rewards = Some(value);
            }
            _ => {}
        }
        self.try_create_summary_data();
    }
    
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let formatted = format!("{:?}", value);
        let cleaned = formatted.trim_matches('"');
        
        // Handle nested field names like "pots.reserves"
        let field_name = field.name();
        
        match field_name {
            "epoch" => {
                if let Ok(epoch) = cleaned.parse::<u64>() {
                    self.epoch = Some(epoch);
                }
            }
            "efficiency" => {
                self.efficiency = Some(cleaned.to_string());
            }
            "incentives" => {
                if let Ok(val) = cleaned.parse::<u64>() {
                    self.incentives = Some(val);
                }
            }
            "treasury_tax" => {
                if let Ok(val) = cleaned.parse::<u64>() {
                    self.treasury_tax = Some(val);
                }
            }
            "total_rewards" => {
                if let Ok(val) = cleaned.parse::<u64>() {
                    self.total_rewards = Some(val);
                }
            }
            "available_rewards" => {
                if let Ok(val) = cleaned.parse::<u64>() {
                    self.available_rewards = Some(val);
                }
            }
            "effective_rewards" => {
                if let Ok(val) = cleaned.parse::<u64>() {
                    self.effective_rewards = Some(val);
                }
            }
            _ => {
                // Handle nested field names like "pots.reserves"
                if field_name == "pots.reserves" || field_name.ends_with(".reserves") {
                    if let Ok(val) = cleaned.parse::<u64>() {
                        self.pots_reserves = Some(val);
                    }
                } else if field_name == "pots.treasury" || field_name.ends_with(".treasury") {
                    if let Ok(val) = cleaned.parse::<u64>() {
                        self.pots_treasury = Some(val);
                    }
                } else if field_name == "pots.fees" || field_name.ends_with(".fees") {
                    if let Ok(val) = cleaned.parse::<u64>() {
                        self.pots_fees = Some(val);
                    }
                }
            }
        }
        self.try_create_summary_data();
    }
}
