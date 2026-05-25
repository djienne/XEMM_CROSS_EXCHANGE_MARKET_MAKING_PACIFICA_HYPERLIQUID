//! Small logging helpers. Scope is deliberately narrow - we don't wrap the
//! whole `tracing`/`colored` API, only the repetitive `[SYMBOL TAG]` prefix
//! pattern that otherwise appears in ~70 places across the codebase.

use colored::{ColoredString, Colorize};

/// Build the canonical tag prefix `"[{SYMBOL} {NAME}]"` as a bold coloured
/// string. Intended for the left-hand label in log lines:
///
/// ```text
/// [SOL MAIN] <message>
/// ```
///
/// The colour argument matches `colored::Colorize` method names and maps to
/// the common palette used in the existing code.
pub fn tag(symbol: &str, name: &str, color: Color) -> ColoredString {
    colorize(format!("[{} {}]", symbol, name), color)
}

/// Tag without a leading symbol, e.g. `[PACIFICA]`, `[INIT]`.
pub fn tag_static(name: &str, color: Color) -> ColoredString {
    colorize(format!("[{}]", name), color)
}

fn colorize(s: String, color: Color) -> ColoredString {
    match color {
        Color::BrightWhite => s.bright_white().bold(),
        Color::BrightYellow => s.bright_yellow().bold(),
        Color::BrightGreen => s.bright_green().bold(),
        Color::BrightMagenta => s.bright_magenta().bold(),
        Color::BrightCyan => s.bright_cyan().bold(),
        Color::Blue => s.blue().bold(),
        Color::Cyan => s.cyan().bold(),
        Color::Green => s.green().bold(),
        Color::Yellow => s.yellow().bold(),
        Color::Red => s.red().bold(),
        Color::Magenta => s.magenta().bold(),
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Color {
    BrightWhite,
    BrightYellow,
    BrightGreen,
    BrightMagenta,
    BrightCyan,
    Blue,
    Cyan,
    Green,
    Yellow,
    Red,
    Magenta,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_contains_symbol_and_name() {
        let t = tag("SOL", "MAIN", Color::BrightWhite);
        // ColoredString wraps the inner &str; its Display includes ANSI codes
        // around it. Check the clear text is present by formatting.
        let rendered = format!("{}", t);
        assert!(rendered.contains("SOL"));
        assert!(rendered.contains("MAIN"));
    }
}
