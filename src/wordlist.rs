use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

use crate::checkpoint::{CheckpointState, DomainStatus};

pub const MIN_NAME_LENGTH: usize = 1;
pub const MAX_NAME_LENGTH: usize = 30;
pub const CHECKPOINT_SAVE_INTERVAL: usize = 10;

pub const WORDLIST_PRESETS: &[(&str, &str, &str)] = &[
    (
        "eff",
        "EFF Large Wordlist (7,776 words)",
        "https://www.eff.org/files/2016/07/18/eff_large_wordlist.txt",
    ),
    (
        "common-10k",
        "Google 10000 English (no swears)",
        "https://raw.githubusercontent.com/first20hours/google-10000-english/master/google-10000-english-no-swears.txt",
    ),
    (
        "common-3k",
        "Most common 3000 English words",
        "https://raw.githubusercontent.com/first20hours/google-10000-english/master/google-10000-english-usa-no-swears-medium.txt",
    ),
    (
        "bip39",
        "BIP39 English wordlist (2048 words)",
        "https://raw.githubusercontent.com/bitcoin/bips/master/bip-0039/english.txt",
    ),
];

pub async fn fetch_wordlist(url: &str, output: &Path) -> Result<usize> {
    let response = reqwest::get(url)
        .await
        .with_context(|| format!("Failed to fetch wordlist from {}", url))?;

    if !response.status().is_success() {
        anyhow::bail!("Failed to fetch wordlist: HTTP {}", response.status());
    }

    let text = response
        .text()
        .await
        .with_context(|| "Failed to read response body")?;

    let words = parse_wordlist_content(&text);

    if words.is_empty() {
        anyhow::bail!("No valid words found in the wordlist");
    }

    let content = words.join("\n");
    fs::write(output, &content)
        .await
        .with_context(|| format!("Failed to write wordlist to {}", output.display()))?;

    Ok(words.len())
}

pub fn parse_wordlist_content(content: &str) -> Vec<String> {
    content
        .lines()
        .filter_map(|line| {
            let line = line.trim();

            if line.is_empty() || line.starts_with('#') {
                return None;
            }

            // Handle EFF format (dice number + tab + word)
            let word = if line.contains('\t') {
                line.split('\t').last().unwrap_or(line)
            } else {
                line
            };

            let word = word.trim().to_lowercase();

            if word.chars().all(|c| c.is_ascii_alphabetic())
                && word.len() >= MIN_NAME_LENGTH
                && word.len() <= MAX_NAME_LENGTH
            {
                Some(word)
            } else {
                None
            }
        })
        .collect()
}

pub async fn load_wordlist(path: &Path) -> Result<Vec<String>> {
    let content = fs::read_to_string(path)
        .await
        .with_context(|| format!("Failed to read wordlist from {}", path.display()))?;

    let words = parse_wordlist_content(&content);

    if words.is_empty() {
        anyhow::bail!("No valid words found in {}", path.display());
    }

    Ok(words)
}

pub fn calculate_wordlist_hash(words: &[String]) -> String {
    let mut hasher = Sha256::new();
    for word in words {
        hasher.update(word.as_bytes());
        hasher.update(b"\n");
    }
    format!("{:x}", hasher.finalize())
}

pub fn get_preset(name: &str) -> Option<(&'static str, &'static str, &'static str)> {
    WORDLIST_PRESETS.iter().find(|(n, _, _)| *n == name).copied()
}

pub fn list_presets() -> Vec<(&'static str, &'static str)> {
    WORDLIST_PRESETS
        .iter()
        .map(|(name, desc, _)| (*name, *desc))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFilter {
    All,
    Available,
    Taken,
    Error,
}

impl ExportFilter {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "all" => Ok(Self::All),
            "available" => Ok(Self::Available),
            "taken" => Ok(Self::Taken),
            "error" => Ok(Self::Error),
            _ => anyhow::bail!("Invalid filter: {}. Use: all, available, taken, error", s),
        }
    }

    fn matches(&self, status: Option<DomainStatus>) -> bool {
        match (self, status) {
            (Self::All, _) => true,
            (Self::Available, Some(DomainStatus::Available)) => true,
            (Self::Taken, Some(DomainStatus::Taken)) => true,
            (Self::Error, Some(DomainStatus::Error)) => true,
            _ => false,
        }
    }
}

pub async fn export_results(
    state: &CheckpointState,
    output: &Path,
    filter: ExportFilter,
) -> Result<usize> {
    let mut file = File::create(output)
        .await
        .with_context(|| format!("Failed to create output file: {}", output.display()))?;

    file.write_all(b"word,eth_domain,eth_status,box_domain,box_status,id_domain,id_status\n")
        .await?;

    let mut count = 0;

    for (word, result) in &state.results {
        let eth_matches = filter.matches(result.eth_status);
        let box_matches = filter.matches(result.box_status);
        let id_matches = filter.matches(result.id_status);

        if !eth_matches && !box_matches && !id_matches {
            continue;
        }

        let eth_status = result
            .eth_status
            .map(|s| format!("{:?}", s).to_lowercase())
            .unwrap_or_else(|| "pending".to_string());

        let box_status = result
            .box_status
            .map(|s| format!("{:?}", s).to_lowercase())
            .unwrap_or_else(|| "pending".to_string());

        let id_status = result
            .id_status
            .map(|s| format!("{:?}", s).to_lowercase())
            .unwrap_or_else(|| "pending".to_string());

        let line = format!(
            "{},{}.eth,{},{}.box,{},{}.id,{}\n",
            word, word, eth_status, word, box_status, word, id_status
        );
        file.write_all(line.as_bytes()).await?;
        count += 1;
    }

    file.flush().await?;
    Ok(count)
}

pub fn print_status(state: &CheckpointState) {
    let stats = state.get_stats();

    println!("\n=== Domain Checker Status ===\n");
    println!(
        "Progress: {}/{} words checked",
        stats.checked_words, stats.total_words
    );

    if stats.total_words > 0 {
        let progress = (stats.checked_words as f64 / stats.total_words as f64) * 100.0;
        println!("         {:.1}% complete", progress);
    }

    if state.check_eth {
        println!("\n.eth domains:");
        println!("  Available: {}", stats.eth_available);
        println!("  Taken:     {}", stats.eth_taken);
        println!("  Errors:    {}", stats.eth_error);
    }

    if state.check_box {
        println!("\n.box domains:");
        println!("  Available: {}", stats.box_available);
        println!("  Taken:     {}", stats.box_taken);
        println!("  Errors:    {}", stats.box_error);
    }

    if state.check_id {
        println!("\n.id domains:");
        println!("  Available: {}", stats.id_available);
        println!("  Taken:     {}", stats.id_taken);
        println!("  Errors:    {}", stats.id_error);
    }

    if stats.retry_queue_size > 0 {
        println!("\nRetry queue: {} items pending", stats.retry_queue_size);
    }

    println!(
        "\nStarted:  {}",
        stats.started_at.format("%Y-%m-%d %H:%M:%S UTC")
    );
    println!(
        "Updated:  {}",
        stats.last_updated.format("%Y-%m-%d %H:%M:%S UTC")
    );
    println!();
}

mod colors {
    pub const GREEN: &str = "\x1b[92m";
    pub const CYAN: &str = "\x1b[96m";
    pub const YELLOW: &str = "\x1b[93m";
    pub const BOLD: &str = "\x1b[1m";
    pub const RESET: &str = "\x1b[0m";
}

pub fn print_results(state: &CheckpointState, both_only: bool, show_eth: bool, show_box: bool, show_id: bool) {
    use colors::*;

    let mut eth_available: Vec<&String> = Vec::new();
    let mut box_available: Vec<&String> = Vec::new();
    let mut id_available: Vec<&String> = Vec::new();
    let mut all_available: Vec<&String> = Vec::new();

    for (word, result) in &state.results {
        let eth_avail = matches!(result.eth_status, Some(DomainStatus::Available));
        let box_avail = matches!(result.box_status, Some(DomainStatus::Available));
        let id_avail = matches!(result.id_status, Some(DomainStatus::Available));

        if eth_avail {
            eth_available.push(word);
        }
        if box_avail {
            box_available.push(word);
        }
        if id_avail {
            id_available.push(word);
        }
        if eth_avail && box_avail && id_avail {
            all_available.push(word);
        }
    }

    eth_available.sort();
    box_available.sort();
    id_available.sort();
    all_available.sort();

    if both_only {
        println!("\n{BOLD}{CYAN}═══ Available on ALL domains (.eth, .box, .id) ═══{RESET}\n");
        if all_available.is_empty() {
            println!("  {YELLOW}No domains available on all three{RESET}");
        } else {
            print_all_columns(&all_available, GREEN);
            println!("\n{BOLD}Total: {} domains{RESET}", all_available.len());
        }
        println!();
        return;
    }

    let any_specified = show_eth || show_box || show_id;
    let display_eth = show_eth || !any_specified;
    let display_box = show_box || !any_specified;
    let display_id = show_id || !any_specified;

    if display_eth {
        println!("\n{BOLD}{CYAN}═══ Available .eth Domains ({}) ═══{RESET}\n", eth_available.len());
        if eth_available.is_empty() {
            println!("  {YELLOW}None available{RESET}");
        } else {
            print_word_columns(&eth_available, "eth", GREEN);
        }
    }

    if display_box {
        println!("\n{BOLD}{CYAN}═══ Available .box Domains ({}) ═══{RESET}\n", box_available.len());
        if box_available.is_empty() {
            println!("  {YELLOW}None available{RESET}");
        } else {
            print_word_columns(&box_available, "box", GREEN);
        }
    }

    if display_id {
        println!("\n{BOLD}{CYAN}═══ Available .id Domains ({}) ═══{RESET}\n", id_available.len());
        if id_available.is_empty() {
            println!("  {YELLOW}None available{RESET}");
        } else {
            print_word_columns(&id_available, "id", GREEN);
        }
    }

    println!("\n{BOLD}═══ Summary ═══{RESET}");
    println!("  .eth available: {GREEN}{}{RESET}", eth_available.len());
    println!("  .box available: {GREEN}{}{RESET}", box_available.len());
    println!("  .id available:  {GREEN}{}{RESET}", id_available.len());
    println!("  All available:  {GREEN}{}{RESET}", all_available.len());
    println!();
}

fn print_word_columns(words: &[&String], suffix: &str, color: &str) {
    use colors::RESET;

    let term_width = 100;
    let max_word_len = words.iter().map(|w| w.len()).max().unwrap_or(10);
    let col_width = max_word_len + suffix.len() + 4;
    let cols = (term_width / col_width).max(1);

    for chunk in words.chunks(cols) {
        print!("  ");
        for word in chunk {
            let entry = format!("{}.{}", word, suffix);
            print!("{color}{entry:<width$}{RESET}", width = col_width);
        }
        println!();
    }
}

fn print_all_columns(words: &[&String], color: &str) {
    use colors::RESET;

    let term_width = 120;
    let max_word_len = words.iter().map(|w| w.len()).max().unwrap_or(10);
    let entry_width = (max_word_len + 5) * 3 + 4;
    let cols = (term_width / entry_width).max(1);

    for chunk in words.chunks(cols) {
        print!("  ");
        for word in chunk {
            let eth = format!("{}.eth", word);
            let box_d = format!("{}.box", word);
            let id_d = format!("{}.id", word);
            print!("{color}{:<w$}{RESET}  {color}{:<w$}{RESET}  {color}{:<w$}{RESET}   ", eth, box_d, id_d, w = max_word_len + 4);
        }
        println!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_wordlist() {
        let content = "hello\nworld\ntest";
        let words = parse_wordlist_content(content);
        assert_eq!(words, vec!["hello", "world", "test"]);
    }

    #[test]
    fn test_parse_eff_format() {
        let content = "11111\tabacus\n11112\tabdomen\n11113\tabide";
        let words = parse_wordlist_content(content);
        assert_eq!(words, vec!["abacus", "abdomen", "abide"]);
    }

    #[test]
    fn test_parse_filters_invalid() {
        let content = "# comment\nhello\nthis-has-dash\nVALID\n123numeric";
        let words = parse_wordlist_content(content);
        assert_eq!(words, vec!["hello", "valid"]);
    }

    #[test]
    fn test_hash_consistency() {
        let words = vec!["hello".to_string(), "world".to_string()];
        let hash1 = calculate_wordlist_hash(&words);
        let hash2 = calculate_wordlist_hash(&words);
        assert_eq!(hash1, hash2);
    }
}
