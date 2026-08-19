// Copyright 2025
//
// Custom tracing layer to capture rewards breakdown events
// This is a minimal hook that doesn't modify core ledger structures

use std::sync::Arc;
use tracing::{Event, Subscriber};
use tracing_subscriber::{
    layer::Context,
    Layer,
};

/// Callback function type for processing rewards breakdown data
pub type RewardsBreakdownCallback = Box<dyn Fn(RewardsBreakdown) + Send + Sync>;

/// Rewards breakdown data extracted from trace events
#[derive(Debug, Clone)]
pub struct RewardsBreakdown {
    pub pool_id: String,
    pub pool_id_hex: String,
    pub leader_total: u64,
    pub leader_operator: u64,
    pub leader_staking: u64,
    pub owner_stake: u64,
    pub total_stake: u64,
}

/// A tracing layer that captures `rewards.leader_breakdown` events
/// and calls a callback with the extracted data.
pub struct RewardsBreakdownLayer {
    callback: Arc<RewardsBreakdownCallback>,
}

impl RewardsBreakdownLayer {
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn(RewardsBreakdown) + Send + Sync + 'static,
    {
        Self {
            callback: Arc::new(Box::new(callback)),
        }
    }
}

impl<S: Subscriber> Layer<S> for RewardsBreakdownLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // Only process events from our target with the specific message
        if event.metadata().target() == "amaru::ledger::state::rewards"
            && event.metadata().name() == "rewards.leader_breakdown"
        {
            let mut visitor = RewardsFieldVisitor::default();
            event.record(&mut visitor);

            if let Some(breakdown) = visitor.breakdown {
                (self.callback)(breakdown);
            }
        }
    }
}

#[derive(Default)]
struct RewardsFieldVisitor {
    breakdown: Option<RewardsBreakdown>,
    pool_id: Option<String>,
    pool_id_hex: Option<String>,
    leader_total: Option<u64>,
    leader_operator: Option<u64>,
    leader_staking: Option<u64>,
    owner_stake: Option<u64>,
    total_stake: Option<u64>,
}

impl RewardsFieldVisitor {
    fn try_create_breakdown(&mut self) {
        if self.breakdown.is_some() {
            return; // Already created
        }

        if let (Some(pool_id), Some(pool_id_hex), Some(leader_total), 
                Some(leader_operator), Some(leader_staking), 
                Some(owner_stake), Some(total_stake)) = (
            self.pool_id.clone(),
            self.pool_id_hex.clone(),
            self.leader_total,
            self.leader_operator,
            self.leader_staking,
            self.owner_stake,
            self.total_stake,
        ) {
            self.breakdown = Some(RewardsBreakdown {
                pool_id,
                pool_id_hex,
                leader_total,
                leader_operator,
                leader_staking,
                owner_stake,
                total_stake,
            });
        }
    }
}

impl tracing::field::Visit for RewardsFieldVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "pool_id" => self.pool_id = Some(value.to_string()),
            "pool_id_hex" => self.pool_id_hex = Some(value.to_string()),
            _ => {}
        }
        self.try_create_breakdown();
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "leader_total" => self.leader_total = Some(value),
            "leader_operator" => self.leader_operator = Some(value),
            "leader_staking" => self.leader_staking = Some(value),
            "owner_stake" => self.owner_stake = Some(value),
            "total_stake" => self.total_stake = Some(value),
            _ => {}
        }
        self.try_create_breakdown();
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        // Handle Debug-formatted values (Display types are formatted via record_debug)
        if field.name() == "pool_id" || field.name() == "pool_id_hex" {
            let formatted = format!("{:?}", value);
            let cleaned = formatted.trim_matches('"');
            if field.name() == "pool_id" {
                self.pool_id = Some(cleaned.to_string());
            } else if field.name() == "pool_id_hex" {
                self.pool_id_hex = Some(cleaned.to_string());
            }
            self.try_create_breakdown();
        }
    }
}
