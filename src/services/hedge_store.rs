use std::path::Path;

use anyhow::Result;
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use crate::services::HedgeIntent;

const DEFAULT_HEDGE_LIFECYCLE_PATH: &str = "data/hedge_lifecycle.jsonl";
static LIFECYCLE_LOGGER: std::sync::OnceLock<mpsc::Sender<String>> = std::sync::OnceLock::new();

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
    pub intent: &'a HedgeIntent,
    pub status: HedgeIntentStatus,
    pub cloid: Option<&'a str>,
    pub target_qty: Option<f64>,
    pub filled_qty: Option<f64>,
    pub residual_qty: Option<f64>,
    pub attempt: Option<u64>,
    pub reason: Option<String>,
}

impl<'a> HedgeLifecycleUpdate<'a> {
    pub fn new(intent: &'a HedgeIntent, status: HedgeIntentStatus) -> Self {
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

pub fn intent_id(intent: &HedgeIntent) -> String {
    format!("{}-{}", intent.source_order_id, intent.hedge_seq)
}

pub fn start_lifecycle_logger(capacity: usize) {
    let (tx, mut rx) = mpsc::channel::<String>(capacity);
    if LIFECYCLE_LOGGER.set(tx).is_err() {
        return;
    }

    tokio::spawn(async move {
        let path = Path::new(DEFAULT_HEDGE_LIFECYCLE_PATH);
        if let Some(parent) = path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                tracing::warn!("failed to create hedge lifecycle log dir: {}", e);
                return;
            }
        }

        let mut file = match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
        {
            Ok(file) => file,
            Err(e) => {
                tracing::warn!("failed to open hedge lifecycle log: {}", e);
                return;
            }
        };

        while let Some(line) = rx.recv().await {
            if let Err(e) = file.write_all(line.as_bytes()).await {
                tracing::warn!("failed to write hedge lifecycle update: {}", e);
                continue;
            }
            if let Err(e) = file.write_all(b"\n").await {
                tracing::warn!("failed to write hedge lifecycle newline: {}", e);
            }
        }
        let _ = file.flush().await;
    });
}

pub async fn append_lifecycle_update(update: HedgeLifecycleUpdate<'_>) -> Result<()> {
    let line = lifecycle_record(update).to_string();
    if let Some(tx) = LIFECYCLE_LOGGER.get() {
        tx.try_send(line)?;
        return Ok(());
    }
    append_lifecycle_line_to_path(DEFAULT_HEDGE_LIFECYCLE_PATH, line).await
}

/// Best-effort lifecycle write that NEVER fails the caller. Journaling is a
/// non-critical observability concern; a saturated/closed log channel or a
/// stalled disk must not abort a hedge decision (which would let a non-trading
/// subsystem wedge the trading subsystem). Drops are logged.
pub async fn try_append_lifecycle_update(update: HedgeLifecycleUpdate<'_>) {
    if let Err(e) = append_lifecycle_update(update).await {
        tracing::warn!("hedge lifecycle journal write dropped (non-fatal): {}", e);
    }
}

#[cfg(test)]
async fn append_lifecycle_update_to_path(
    path: impl AsRef<Path>,
    update: HedgeLifecycleUpdate<'_>,
) -> Result<()> {
    append_lifecycle_line_to_path(path, lifecycle_record(update).to_string()).await
}

async fn append_lifecycle_line_to_path(path: impl AsRef<Path>, line: String) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;

    file.write_all(line.as_bytes()).await?;
    file.write_all(b"\n").await?;
    Ok(())
}

fn lifecycle_record(update: HedgeLifecycleUpdate<'_>) -> serde_json::Value {
    let intent = update.intent;
    json!({
        "ts_ms": chrono::Utc::now().timestamp_millis(),
        "intent_id": intent_id(intent),
        "status": update.status.as_str(),
        "source_order_id": intent.source_order_id,
        "hedge_seq": intent.hedge_seq,
        "source": intent.source.as_str(),
        "maker_side": intent.maker_side.map(|side| side.as_str()),
        "hedge_side": intent.hedge_side.as_str(),
        "size": intent.size,
        "avg_price": intent.avg_price,
        "terminal": intent.terminal,
        "cloid": update.cloid,
        "target_qty": update.target_qty,
        "filled_qty": update.filled_qty,
        "residual_qty": update.residual_qty,
        "attempt": update.attempt,
        "reason": update.reason,
    })
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
        let intent =
            HedgeIntent::from_maker_fill(7, 2, OrderSide::Buy, 0.5, 100.0, Instant::now(), true);
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
