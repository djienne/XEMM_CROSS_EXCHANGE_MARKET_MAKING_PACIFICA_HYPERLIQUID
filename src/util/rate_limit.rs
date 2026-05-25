use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// API endpoint group for per-endpoint rate-limit tracking.
///
/// One 429 on `Info` must not back off `PlaceOrder` - each exchange endpoint
/// has its own bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EndpointGroup {
    PlaceOrder,
    CancelOrder,
    Info,
    Trades,
    Positions,
}

/// Thread-safe book of `RateLimitTracker`s keyed by endpoint group. Shared
/// across services via `Arc`.
#[derive(Debug, Default)]
pub struct RateLimitBook {
    inner: Mutex<HashMap<EndpointGroup, RateLimitTracker>>,
}

impl RateLimitBook {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record a rate-limit error for a specific endpoint group.
    pub fn record_error(&self, group: EndpointGroup) {
        self.inner.lock().entry(group).or_default().record_error();
    }

    /// Record a successful call - resets the per-group backoff.
    pub fn record_success(&self, group: EndpointGroup) {
        self.inner.lock().entry(group).or_default().record_success();
    }

    /// Should this endpoint skip work because of an active backoff?
    pub fn should_skip(&self, group: EndpointGroup) -> bool {
        self.inner
            .lock()
            .get(&group)
            .map(|t| t.should_skip())
            .unwrap_or(false)
    }

    /// Backoff in seconds for the current failure streak.
    pub fn get_backoff_secs(&self, group: EndpointGroup) -> u64 {
        self.inner
            .lock()
            .get(&group)
            .map(|t| t.get_backoff_secs())
            .unwrap_or(0)
    }

    /// Remaining backoff in fractional seconds.
    pub fn remaining_backoff_secs(&self, group: EndpointGroup) -> f64 {
        self.inner
            .lock()
            .get(&group)
            .map(|t| t.remaining_backoff_secs())
            .unwrap_or(0.0)
    }

    /// Current consecutive error count for the group.
    pub fn consecutive_errors(&self, group: EndpointGroup) -> u32 {
        self.inner
            .lock()
            .get(&group)
            .map(|t| t.consecutive_errors())
            .unwrap_or(0)
    }
}

/// Rate limit tracker for exponential backoff
#[derive(Debug)]
pub struct RateLimitTracker {
    last_error_time: Option<Instant>,
    consecutive_errors: u32,
}

impl RateLimitTracker {
    /// Create a new rate limit tracker
    pub fn new() -> Self {
        Self {
            last_error_time: None,
            consecutive_errors: 0,
        }
    }

    /// Record a rate limit error
    pub fn record_error(&mut self) {
        self.last_error_time = Some(Instant::now());
        self.consecutive_errors += 1;
    }

    /// Record a successful API call
    pub fn record_success(&mut self) {
        self.last_error_time = None;
        self.consecutive_errors = 0;
    }

    /// Get current backoff duration in seconds (exponential: 1, 2, 4, 8, 16, 32 max)
    pub fn get_backoff_secs(&self) -> u64 {
        if self.consecutive_errors == 0 {
            return 0;
        }
        std::cmp::min(2u64.pow(self.consecutive_errors - 1), 32)
    }

    /// Check if we should skip this operation due to active backoff
    pub fn should_skip(&self) -> bool {
        if let Some(last_error) = self.last_error_time {
            let backoff_duration = Duration::from_secs(self.get_backoff_secs());
            last_error.elapsed() < backoff_duration
        } else {
            false
        }
    }

    /// Get remaining backoff time in seconds
    pub fn remaining_backoff_secs(&self) -> f64 {
        if let Some(last_error) = self.last_error_time {
            let backoff_duration = Duration::from_secs(self.get_backoff_secs());
            let elapsed = last_error.elapsed();
            if elapsed < backoff_duration {
                (backoff_duration - elapsed).as_secs_f64()
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    /// Get the number of consecutive errors
    pub fn consecutive_errors(&self) -> u32 {
        self.consecutive_errors
    }
}

impl Default for RateLimitTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if an error is a rate limit error
pub fn is_rate_limit_error(error: &anyhow::Error) -> bool {
    let error_string = error.to_string().to_lowercase();
    error_string.contains("rate limit")
        || error_string.contains("too many requests")
        || error_string.contains("429")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_tracker_has_no_backoff() {
        let t = RateLimitTracker::new();
        assert_eq!(t.consecutive_errors(), 0);
        assert_eq!(t.get_backoff_secs(), 0);
        assert!(!t.should_skip());
        assert_eq!(t.remaining_backoff_secs(), 0.0);
    }

    #[test]
    fn backoff_doubles_capped_at_32() {
        let mut t = RateLimitTracker::new();
        let expected = [1u64, 2, 4, 8, 16, 32, 32, 32];
        for (i, want) in expected.iter().enumerate() {
            t.record_error();
            assert_eq!(
                t.get_backoff_secs(),
                *want,
                "after {} errors expected {}s",
                i + 1,
                want
            );
        }
    }

    #[test]
    fn record_success_clears_backoff() {
        let mut t = RateLimitTracker::new();
        t.record_error();
        t.record_error();
        assert_eq!(t.consecutive_errors(), 2);
        t.record_success();
        assert_eq!(t.consecutive_errors(), 0);
        assert_eq!(t.get_backoff_secs(), 0);
        assert!(!t.should_skip());
    }

    #[test]
    fn should_skip_true_immediately_after_error() {
        let mut t = RateLimitTracker::new();
        t.record_error();
        assert!(t.should_skip());
        assert!(t.remaining_backoff_secs() > 0.0);
    }

    #[test]
    fn is_rate_limit_error_matches_common_strings() {
        assert!(is_rate_limit_error(&anyhow::anyhow!(
            "HTTP 429 Too Many Requests"
        )));
        assert!(is_rate_limit_error(&anyhow::anyhow!("Rate limit exceeded")));
        assert!(is_rate_limit_error(&anyhow::anyhow!("TOO MANY REQUESTS")));
        assert!(!is_rate_limit_error(&anyhow::anyhow!(
            "Internal server error"
        )));
    }

    #[test]
    fn book_isolates_endpoints() {
        let book = RateLimitBook::new();
        book.record_error(EndpointGroup::PlaceOrder);
        book.record_error(EndpointGroup::PlaceOrder);
        assert_eq!(book.consecutive_errors(EndpointGroup::PlaceOrder), 2);
        assert_eq!(book.consecutive_errors(EndpointGroup::Info), 0);
        assert!(book.should_skip(EndpointGroup::PlaceOrder));
        assert!(!book.should_skip(EndpointGroup::Info));
        book.record_success(EndpointGroup::PlaceOrder);
        assert_eq!(book.consecutive_errors(EndpointGroup::PlaceOrder), 0);
    }
}
