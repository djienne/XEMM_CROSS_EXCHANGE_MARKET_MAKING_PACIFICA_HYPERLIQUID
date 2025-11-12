import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Load configuration from config.json
 */
export function loadConfig() {
  try {
    const configPath = path.join(__dirname, '..', 'config.json');
    const configData = fs.readFileSync(configPath, 'utf8');
    return JSON.parse(configData);
  } catch (error) {
    console.error('Error loading config.json:', error.message);
    throw error;
  }
}

/**
 * Get list of trading symbols
 */
export function getSymbols() {
  const config = loadConfig();
  return config.symbols || [];
}

/**
 * Get exchange configuration
 */
export function getExchangeConfig(exchangeName) {
  const config = loadConfig();
  return config.exchanges?.[exchangeName] || null;
}

/**
 * Get trading configuration
 */
export function getTradingConfig() {
  const config = loadConfig();
  return config.trading || {};
}

/**
 * Get risk configuration
 */
export function getRiskConfig() {
  const config = loadConfig();
  return config.risk || {};
}

/**
 * Get logging configuration
 */
export function getLoggingConfig() {
  const config = loadConfig();
  return config.logging || {};
}

export default {
  loadConfig,
  getSymbols,
  getExchangeConfig,
  getTradingConfig,
  getRiskConfig,
  getLoggingConfig
};
