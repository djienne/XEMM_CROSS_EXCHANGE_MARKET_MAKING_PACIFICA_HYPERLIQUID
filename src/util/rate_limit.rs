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

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_new_tracker() {
        let tracker = RateLimitTracker::new();

        assert_eq!(tracker.consecutive_errors(), 0);
        assert_eq!(tracker.get_backoff_secs(), 0);
        assert!(!tracker.should_skip());
        assert_eq!(tracker.remaining_backoff_secs(), 0.0);
    }

    #[test]
    fn test_default_tracker() {
        let tracker = RateLimitTracker::default();
        assert_eq!(tracker.consecutive_errors(), 0);
    }

    #[test]
    fn test_record_error_increments() {
        let mut tracker = RateLimitTracker::new();

        tracker.record_error();
        assert_eq!(tracker.consecutive_errors(), 1);

        tracker.record_error();
        assert_eq!(tracker.consecutive_errors(), 2);

        tracker.record_error();
        assert_eq!(tracker.consecutive_errors(), 3);
    }

    #[test]
    fn test_record_success_resets() {
        let mut tracker = RateLimitTracker::new();

        // Build up some errors
        tracker.record_error();
        tracker.record_error();
        tracker.record_error();
        assert_eq!(tracker.consecutive_errors(), 3);

        // Success resets
        tracker.record_success();
        assert_eq!(tracker.consecutive_errors(), 0);
        assert_eq!(tracker.get_backoff_secs(), 0);
        assert!(!tracker.should_skip());
    }

    #[test]
    fn test_exponential_backoff() {
        let mut tracker = RateLimitTracker::new();

        // 0 errors = 0 backoff
        assert_eq!(tracker.get_backoff_secs(), 0);

        // 1 error = 1 sec (2^0)
        tracker.record_error();
        assert_eq!(tracker.get_backoff_secs(), 1);

        // 2 errors = 2 sec (2^1)
        tracker.record_error();
        assert_eq!(tracker.get_backoff_secs(), 2);

        // 3 errors = 4 sec (2^2)
        tracker.record_error();
        assert_eq!(tracker.get_backoff_secs(), 4);

        // 4 errors = 8 sec (2^3)
        tracker.record_error();
        assert_eq!(tracker.get_backoff_secs(), 8);

        // 5 errors = 16 sec (2^4)
        tracker.record_error();
        assert_eq!(tracker.get_backoff_secs(), 16);

        // 6 errors = 32 sec (2^5, max)
        tracker.record_error();
        assert_eq!(tracker.get_backoff_secs(), 32);

        // 7+ errors still = 32 sec (max)
        tracker.record_error();
        assert_eq!(tracker.get_backoff_secs(), 32);

        tracker.record_error();
        assert_eq!(tracker.get_backoff_secs(), 32);
    }

    #[test]
    fn test_should_skip_during_backoff() {
        let mut tracker = RateLimitTracker::new();

        // No errors - should not skip
        assert!(!tracker.should_skip());

        // Record error - should skip (within 1 second backoff)
        tracker.record_error();
        assert!(tracker.should_skip());
    }

    #[test]
    fn test_should_skip_after_backoff_expires() {
        let mut tracker = RateLimitTracker::new();

        // Record error
        tracker.record_error();
        assert!(tracker.should_skip());

        // Wait for backoff to expire (1 second)
        sleep(Duration::from_millis(1100));  // Wait slightly more than 1 sec

        // Should no longer skip
        assert!(!tracker.should_skip());
    }

    #[test]
    fn test_remaining_backoff_secs() {
        let mut tracker = RateLimitTracker::new();

        // No errors - no backoff
        assert_eq!(tracker.remaining_backoff_secs(), 0.0);

        // Record error (1 sec backoff)
        tracker.record_error();
        let remaining = tracker.remaining_backoff_secs();
        assert!(remaining > 0.0 && remaining <= 1.0);

        // Wait a bit
        sleep(Duration::from_millis(200));
        let remaining2 = tracker.remaining_backoff_secs();
        assert!(remaining2 < remaining);
    }

    #[test]
    fn test_is_rate_limit_error_detection() {
        // Test various rate limit error messages
        let rate_limit_errors = [
            "Rate limit exceeded",
            "rate limit reached",
            "Too many requests",
            "too many requests per second",
            "HTTP 429: Too Many Requests",
            "Error 429",
        ];

        for msg in rate_limit_errors {
            let error = anyhow::anyhow!(msg);
            assert!(
                is_rate_limit_error(&error),
                "Should detect '{}' as rate limit error",
                msg
            );
        }
    }

    #[test]
    fn test_is_rate_limit_error_non_rate_limit() {
        // Test non-rate-limit errors
        let other_errors = [
            "Connection refused",
            "Internal server error",
            "Invalid request",
            "Unauthorized",
            "Insufficient margin",
        ];

        for msg in other_errors {
            let error = anyhow::anyhow!(msg);
            assert!(
                !is_rate_limit_error(&error),
                "Should NOT detect '{}' as rate limit error",
                msg
            );
        }
    }

    #[test]
    fn test_backoff_reset_after_success_mid_errors() {
        let mut tracker = RateLimitTracker::new();

        // Build up errors
        tracker.record_error();
        tracker.record_error();
        tracker.record_error();
        assert_eq!(tracker.get_backoff_secs(), 4);

        // Success resets everything
        tracker.record_success();
        assert_eq!(tracker.get_backoff_secs(), 0);

        // New error starts from 1 again
        tracker.record_error();
        assert_eq!(tracker.get_backoff_secs(), 1);
    }
}
