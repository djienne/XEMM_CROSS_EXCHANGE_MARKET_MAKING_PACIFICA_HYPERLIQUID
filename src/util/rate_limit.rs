use std::time::{Duration, Instant};

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
