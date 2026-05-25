use std::path::Path;

use anyhow::Result;
use serde_json::json;
use tokio::io::AsyncWriteExt;

use crate::services::HedgeEvent;

const DEFAULT_HEDGE_LIFECYCLE_PATH: &str = "data/hedge_lifecycle.jsonl";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HedgeIntentStatus {
    Created,
    Queued,
    QueueFull,
    QueueClosed,
    Submitted,
    Filled,
    PartiallyFilled,
    Complete,
    Error,
    Unknown,
}

impl HedgeIntentStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Queued => "queued",
            Self::QueueFull => "queue_full",
            Self::QueueClosed => "queue_closed",
            Self::Submitted => "submitted",
            Self::Filled => "filled",
            Self::PartiallyFilled => "partially_filled",
            Self::Complete => "complete",
            Self::Error => "error",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone)]
pub struct HedgeLifecycleUpdate<'a> {
    pub intent: &'a HedgeEvent,
    pub status: HedgeIntentStatus,
    pub cloid: Option<&'a str>,
    pub target_qty: Option<f64>,
    pub filled_qty: Option<f64>,
    pub residual_qty: Option<f64>,
    pub attempt: Option<u64>,
    pub reason: Option<String>,
}

impl<'a> HedgeLifecycleUpdate<'a> {
    pub fn new(intent: &'a HedgeEvent, status: HedgeIntentStatus) -> Self {
        Self {
            intent,
            status,
            cloid: None,
            target_qty: None,
            filled_qty: None,
            residual_qty: None,
            attempt: None,
            reason: None,
        }
    }
}

pub fn intent_id(intent: &HedgeEvent) -> String {
    format!("{}-{}", intent.source_order_id, intent.hedge_seq)
}

pub async fn append_lifecycle_update(update: HedgeLifecycleUpdate<'_>) -> Result<()> {
    append_lifecycle_update_to_path(DEFAULT_HEDGE_LIFECYCLE_PATH, update).await
}

async fn append_lifecycle_update_to_path(
    path: impl AsRef<Path>,
    update: HedgeLifecycleUpdate<'_>,
) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;

    let intent = update.intent;
    let record = json!({
        "ts_ms": chrono::Utc::now().timestamp_millis(),
        "intent_id": intent_id(intent),
        "status": update.status.as_str(),
        "source_order_id": intent.source_order_id,
        "hedge_seq": intent.hedge_seq,
        "side": intent.side.as_str(),
        "size": intent.size,
        "avg_price": intent.avg_price,
        "terminal": intent.terminal,
        "cloid": update.cloid,
        "target_qty": update.target_qty,
        "filled_qty": update.filled_qty,
        "residual_qty": update.residual_qty,
        "attempt": update.attempt,
        "reason": update.reason,
    });

    file.write_all(record.to_string().as_bytes()).await?;
    file.write_all(b"\n").await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::OrderSide;
    use std::time::Instant;

    #[tokio::test]
    async fn lifecycle_update_writes_json_line() {
        let path = std::env::temp_dir().join(format!(
            "hedge_lifecycle_test_{}.jsonl",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let intent = HedgeEvent {
            source_order_id: 7,
            hedge_seq: 2,
            side: OrderSide::Buy,
            size: 0.5,
            avg_price: 100.0,
            detected_at: Instant::now(),
            terminal: true,
        };
        let mut update = HedgeLifecycleUpdate::new(&intent, HedgeIntentStatus::Submitted);
        update.cloid = Some("0xabc");
        update.target_qty = Some(0.5);
        append_lifecycle_update_to_path(&path, update)
            .await
            .unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("\"status\":\"submitted\""));
        assert!(content.contains("\"intent_id\":\"7-2\""));
        let _ = tokio::fs::remove_file(&path).await;
    }
}
