/**
 * Token bucket rate limiter
 * Implements a token bucket algorithm for rate limiting
 */
export class RateLimiter {
  constructor(options = {}) {
    this.maxTokens = options.maxTokens || 100;
    this.refillRate = options.refillRate || 10; // tokens per second
    this.tokens = this.maxTokens;
    this.lastRefill = Date.now();

    // Queue for pending requests
    this.queue = [];
    this.processing = false;
  }

  /**
   * Refill tokens based on time elapsed
   */
  refill() {
    const now = Date.now();
    const elapsed = (now - this.lastRefill) / 1000; // seconds
    const tokensToAdd = elapsed * this.refillRate;

    this.tokens = Math.min(this.maxTokens, this.tokens + tokensToAdd);
    this.lastRefill = now;
  }

  /**
   * Try to consume tokens
   * @param {number} count - Number of tokens to consume
   * @returns {boolean} - True if tokens were consumed, false otherwise
   */
  tryConsume(count = 1) {
    this.refill();

    if (this.tokens >= count) {
      this.tokens -= count;
      return true;
    }

    return false;
  }

  /**
   * Wait until tokens are available and consume them
   * @param {number} count - Number of tokens to consume
   * @returns {Promise<void>}
   */
  async consume(count = 1) {
    return new Promise((resolve) => {
      const attempt = () => {
        if (this.tryConsume(count)) {
          resolve();
        } else {
          // Wait and try again
          const waitTime = Math.max(50, (count - this.tokens) / this.refillRate * 1000);
          setTimeout(attempt, waitTime);
        }
      };

      attempt();
    });
  }

  /**
   * Execute a function with rate limiting
   * @param {Function} fn - Function to execute
   * @param {number} cost - Token cost (default: 1)
   * @returns {Promise<any>}
   */
  async execute(fn, cost = 1) {
    await this.consume(cost);
    return fn();
  }

  /**
   * Get current token count
   */
  getTokens() {
    this.refill();
    return this.tokens;
  }

  /**
   * Reset the rate limiter
   */
  reset() {
    this.tokens = this.maxTokens;
    this.lastRefill = Date.now();
  }
}

/**
 * Sliding window rate limiter
 * Tracks requests in a sliding time window
 */
export class SlidingWindowRateLimiter {
  constructor(options = {}) {
    this.maxRequests = options.maxRequests || 100;
    this.windowMs = options.windowMs || 60000; // 1 minute default
    this.requests = [];
  }

  /**
   * Clean up old requests outside the window
   */
  cleanup() {
    const now = Date.now();
    const cutoff = now - this.windowMs;
    this.requests = this.requests.filter(time => time > cutoff);
  }

  /**
   * Check if a request can be made
   */
  canRequest() {
    this.cleanup();
    return this.requests.length < this.maxRequests;
  }

  /**
   * Try to make a request
   */
  tryRequest() {
    this.cleanup();

    if (this.requests.length < this.maxRequests) {
      this.requests.push(Date.now());
      return true;
    }

    return false;
  }

  /**
   * Wait until a request can be made
   */
  async waitForSlot() {
    return new Promise((resolve) => {
      const attempt = () => {
        if (this.tryRequest()) {
          resolve();
        } else {
          // Calculate wait time based on oldest request
          const oldest = this.requests[0];
          const waitTime = Math.max(100, oldest + this.windowMs - Date.now());
          setTimeout(attempt, waitTime);
        }
      };

      attempt();
    });
  }

  /**
   * Execute a function with rate limiting
   */
  async execute(fn) {
    await this.waitForSlot();
    return fn();
  }

  /**
   * Get current request count in window
   */
  getRequestCount() {
    this.cleanup();
    return this.requests.length;
  }

  /**
   * Reset the rate limiter
   */
  reset() {
    this.requests = [];
  }
}

export default {
  RateLimiter,
  SlidingWindowRateLimiter
};
