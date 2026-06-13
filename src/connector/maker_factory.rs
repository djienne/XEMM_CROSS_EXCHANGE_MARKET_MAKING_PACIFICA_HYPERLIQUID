//! Maker-venue factory.
//!
//! Builds everything the bot needs from its (swappable) maker venue behind the
//! `MakerExchange` / `MakerFillStream` traits, plus the restartable data-plane
//! builders (orderbook stream + REST poll). Hyperliquid is the permanent taker
//! and is constructed separately in `app.rs`; it never passes through here.
//!
//! Adding a second maker venue = a new `src/connector/<venue>/` module that
//! implements the two maker traits + one new arm in [`build_maker`]. No service
//! edits, no hot-path changes.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::config::Config;
use crate::connector::pacifica::trading::PacificaPoller;
use crate::connector::pacifica::{
    FillDetectionClient, FillDetectionConfig, OrderbookClient as PacOrderbookClient,
    OrderbookConfig as PacOrderbookConfig, PacificaCredentials, PacificaFillStream, PacificaMaker,
    PacificaTrading, PacificaWsTrading,
};
use crate::services::maker::{MakerExchange, MakerFillStream};
use crate::services::price_source::{PricePoll, PriceStream};

/// Builds a fresh orderbook price stream on each (re)connect. The orderbook
/// client is non-`Clone`, so the supervised stream service rebuilds it from
/// config on every restart; this closure encapsulates that rebuild. Boxing is
/// per-connection, never per book frame, so the hot path is untouched.
pub type PriceStreamFactory = Arc<dyn Fn() -> Result<Box<dyn PriceStream>> + Send + Sync>;

/// Builds a fresh REST poller on each restart (price redundancy). Cheap - clones
/// a shared REST handle - and never on the hot path.
pub type PricePollFactory = Arc<dyn Fn() -> Box<dyn PricePoll> + Send + Sync>;

/// The maker venue to construct, parsed from [`Config::maker_venue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MakerVenue {
    Pacifica,
}

impl MakerVenue {
    /// Parse the configured venue. An unknown value is a hard config error
    /// rather than a silent fallback, so a typo cannot quietly run the wrong
    /// venue.
    pub fn from_config(config: &Config) -> Result<Self> {
        match config.maker_venue.to_ascii_lowercase().as_str() {
            "pacifica" => Ok(MakerVenue::Pacifica),
            other => anyhow::bail!(
                "Unknown maker_venue '{}' in config; supported: pacifica",
                other
            ),
        }
    }
}

/// Everything the app needs from the maker venue:
/// - `maker`: the control-plane handle (shared, cloned into every service),
/// - `fill_stream`: the fail-closed fill-event stream (built once),
/// - `price_stream_factory` / `price_poll_factory`: restartable data-plane builders.
pub struct MakerStack {
    pub maker: Arc<dyn MakerExchange>,
    pub fill_stream: Box<dyn MakerFillStream>,
    pub price_stream_factory: PriceStreamFactory,
    pub price_poll_factory: PricePollFactory,
}

/// Construct the maker stack for `venue`. This is the single swap point: a new
/// maker venue is one new arm here.
pub fn build_maker(
    venue: MakerVenue,
    config: &Config,
    credentials: &PacificaCredentials,
) -> Result<MakerStack> {
    match venue {
        MakerVenue::Pacifica => build_pacifica(config, credentials),
    }
}

fn build_pacifica(config: &Config, credentials: &PacificaCredentials) -> Result<MakerStack> {
    // The maker venue's own wire symbol for this asset. `config.symbol` is the
    // canonical identifier (also the Hyperliquid taker symbol); the adapter maps
    // canonical <-> wire internally, and the maker price feeds below subscribe to
    // the wire symbol. Defaults to identity when `maker_symbol` is unset.
    let wire_symbol = config.maker_symbol.as_deref().unwrap_or(&config.symbol);

    // Control plane: one shared REST client + one WS client, wrapped by the
    // `MakerExchange` adapter. Mirrors the construction previously inlined in
    // `XemmBot::new` (single shared instances across all services - the previous
    // per-service instances each kept their own pool/cache with no benefit).
    let pacifica_trading = Arc::new(
        PacificaTrading::new(credentials.clone())
            .context("Failed to create Pacifica trading client")?,
    );
    let pacifica_ws_trading = Arc::new(
        PacificaWsTrading::new(credentials.clone(), false) // false = mainnet
            .with_request_timeout(Duration::from_millis(config.pacifica_ws_request_timeout_ms)),
    );
    let maker: Arc<dyn MakerExchange> = Arc::new(PacificaMaker::new(
        pacifica_trading.clone(),
        pacifica_ws_trading.clone(),
        config.symbol.clone(),
        wire_symbol.to_string(),
    ));

    // Fail-closed fill stream (built once; the client is non-Clone and not
    // restartable). The constructor is pure - no I/O until `run_with` connects -
    // so building it here (rather than later in `run`) is behavior-preserving.
    let fill_config = FillDetectionConfig {
        account: credentials.account.clone(),
        // Unbounded: the fill stream is fail-closed (a permanent exit latches
        // ServiceDown and halts quoting until manual restart), so it must keep
        // reconnecting; FillWsDown gates quoting during gaps and the reconcile
        // hook replays fills missed while disconnected.
        max_attempts: None,
        ping_interval_secs: config.ping_interval_secs,
        enable_position_fill_detection: true,
    };
    let fill_client = FillDetectionClient::new(fill_config, false)
        .context("Failed to create fill detection client")?;
    let fill_stream: Box<dyn MakerFillStream> = Box::new(PacificaFillStream::new(fill_client));

    // Data plane: orderbook stream (restartable). Build once up-front to fail
    // fast on an invalid config at startup, exactly as the old startup path did;
    // the factory then rebuilds the (non-Clone) client from this Clone config on
    // every restart.
    let ob_cfg = PacOrderbookConfig {
        symbol: wire_symbol.to_string(),
        agg_level: config.agg_level,
        reconnect_attempts: config.reconnect_attempts,
        ping_interval_secs: config.ping_interval_secs,
    };
    PacOrderbookClient::new(ob_cfg.clone())
        .context("Failed to create Pacifica orderbook client")?;
    let price_stream_factory: PriceStreamFactory = {
        let ob_cfg = ob_cfg.clone();
        Arc::new(move || {
            let client = PacOrderbookClient::new(ob_cfg.clone())?;
            Ok(Box::new(client) as Box<dyn PriceStream>)
        })
    };

    // Data plane: REST poll (restartable, price redundancy).
    let price_poll_factory: PricePollFactory = {
        let trading = pacifica_trading.clone();
        let symbol = wire_symbol.to_string();
        let agg_level = config.agg_level;
        Arc::new(move || {
            Box::new(PacificaPoller {
                trading: trading.clone(),
                symbol: symbol.clone(),
                agg_level,
            }) as Box<dyn PricePoll>
        })
    };

    Ok(MakerStack {
        maker,
        fill_stream,
        price_stream_factory,
        price_poll_factory,
    })
}
