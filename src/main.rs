mod checkpoint;
mod checker;
mod wordlist;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::broadcast;

use checkpoint::{CheckpointManager, CheckpointState, DomainType, RetryEntry};
use checker::{
    calculate_backoff, CheckResult, DualRateLimiter, EnsChecker, IdChecker, RdapChecker,
    DEFAULT_INITIAL_BACKOFF_MS, DEFAULT_MAX_BACKOFF_SECS, DEFAULT_MAX_RETRIES,
};
use wordlist::{
    calculate_wordlist_hash, export_results, fetch_wordlist, get_preset, list_presets,
    load_wordlist, print_results, print_status, ExportFilter, CHECKPOINT_SAVE_INTERVAL,
};

#[derive(Parser)]
#[command(name = "pretty-ens")]
#[command(about = "Check ENS (.eth), .box, and .id domain availability from a wordlist")]
struct Args {
    /// Path to wordlist file (one word per line)
    #[arg(short, long)]
    wordlist: Option<PathBuf>,

    /// Path to checkpoint file for resume support
    #[arg(short, long, default_value = "checkpoint.json")]
    checkpoint: PathBuf,

    /// Check .eth domains only
    #[arg(long)]
    eth: bool,

    /// Check .box domains only
    #[arg(long = "box")]
    box_domain: bool,

    /// Check .id domains only
    #[arg(long)]
    id: bool,

    /// Resume from existing checkpoint (same wordlist)
    #[arg(long)]
    resume: bool,

    /// Skip already checked words (allows different wordlist)
    #[arg(long)]
    skip_existing: bool,

    /// Number of concurrent requests
    #[arg(long, default_value = "5")]
    concurrency: usize,

    /// ETH RPC rate limit (requests/sec)
    #[arg(long, default_value = "5")]
    eth_rps: u32,

    /// RDAP rate limit for .box (requests/sec)
    #[arg(long, default_value = "2")]
    box_rps: u32,

    /// RDAP rate limit for .id (requests/sec)
    #[arg(long, default_value = "2")]
    id_rps: u32,

    /// Fetch wordlist from preset (use --list-presets to see options)
    #[arg(long, conflicts_with_all = ["wordlist", "status", "export"])]
    fetch_preset: Option<String>,

    /// Fetch wordlist from URL
    #[arg(long, conflicts_with_all = ["wordlist", "status", "export"])]
    fetch_url: Option<String>,

    /// Output file for --fetch-* or --export
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Show progress status from checkpoint
    #[arg(long, conflicts_with_all = ["wordlist", "fetch_preset", "fetch_url"])]
    status: bool,

    /// Export results to CSV file
    #[arg(long, conflicts_with_all = ["wordlist", "fetch_preset", "fetch_url"])]
    export: bool,

    /// Filter for export: all, available, taken, error
    #[arg(long, default_value = "all")]
    filter: String,

    /// List available wordlist presets
    #[arg(long)]
    list_presets: bool,

    /// Pretty print available domains
    #[arg(long, conflicts_with_all = ["wordlist", "fetch_preset", "fetch_url", "export"])]
    show: bool,

    /// Show only domains available on both .eth and .box
    #[arg(long)]
    both: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let args = Args::parse();

    if args.list_presets {
        println!("Available wordlist presets:\n");
        for (name, desc) in list_presets() {
            println!("  {:<12} - {}", name, desc);
        }
        return Ok(());
    }

    if args.fetch_preset.is_some() || args.fetch_url.is_some() {
        let fetch_url = if let Some(preset_name) = &args.fetch_preset {
            let (_, _, url) = get_preset(preset_name)
                .with_context(|| format!("Unknown preset: {}", preset_name))?;
            url.to_string()
        } else {
            args.fetch_url.unwrap()
        };

        let output = args
            .output
            .with_context(|| "Output path required for fetch (use -o/--output)")?;

        println!("Fetching wordlist from: {}", fetch_url);
        let count = fetch_wordlist(&fetch_url, &output).await?;
        println!("Saved {} words to {}", count, output.display());
        return Ok(());
    }

    if args.status {
        let state = CheckpointState::load(&args.checkpoint).await?;
        print_status(&state);
        return Ok(());
    }

    if args.show {
        let state = CheckpointState::load(&args.checkpoint).await?;
        print_results(&state, args.both, args.eth, args.box_domain, args.id);
        return Ok(());
    }

    if args.export {
        let output = args
            .output
            .with_context(|| "Output path required for export (use -o/--output)")?;

        let state = CheckpointState::load(&args.checkpoint).await?;
        let filter = ExportFilter::from_str(&args.filter)?;

        let count = export_results(&state, &output, filter).await?;
        println!("Exported {} results to {}", count, output.display());
        return Ok(());
    }

    let wordlist = args
        .wordlist
        .with_context(|| "Wordlist path required (use -w/--wordlist)")?;

    let any_specified = args.eth || args.box_domain || args.id;
    let check_eth = args.eth || !any_specified;
    let check_box = args.box_domain || !any_specified;
    let check_id = args.id || !any_specified;

    run_checker(
        &wordlist,
        &args.checkpoint,
        check_eth,
        check_box,
        check_id,
        args.resume,
        args.skip_existing,
        args.concurrency,
        args.eth_rps,
        args.box_rps,
        args.id_rps,
    )
    .await
}

async fn run_checker(
    wordlist_path: &Path,
    checkpoint_path: &Path,
    check_eth: bool,
    check_box: bool,
    check_id: bool,
    resume: bool,
    skip_existing: bool,
    concurrency: usize,
    eth_rps: u32,
    box_rps: u32,
    id_rps: u32,
) -> Result<()> {
    let words = load_wordlist(wordlist_path).await?;
    let wordlist_hash = calculate_wordlist_hash(&words);

    println!(
        "Loaded {} words from {}",
        words.len(),
        wordlist_path.display()
    );

    let state = if (resume || skip_existing) && checkpoint_path.exists() {
        let mut state = CheckpointState::load(checkpoint_path).await?;

        if state.wordlist_hash != wordlist_hash {
            if skip_existing {
                println!("Loading existing results, new wordlist detected.");
            } else {
                println!("Warning: Wordlist has changed since last run.");
            }
            state.wordlist_hash = wordlist_hash;
            state.total_words = words.len();
        }

        let stats = state.get_stats();
        println!(
            "Loaded checkpoint: {} words already checked",
            stats.checked_words
        );

        state
    } else {
        println!("Starting fresh check...");
        CheckpointState::new(wordlist_hash, words.len(), check_eth, check_box, check_id)
    };

    let checkpoint = Arc::new(CheckpointManager::new(
        state,
        checkpoint_path.to_path_buf(),
        CHECKPOINT_SAVE_INTERVAL,
    ));

    let ens_checker = if check_eth {
        Some(Arc::new(EnsChecker::new().await?))
    } else {
        None
    };

    let rdap_checker = if check_box {
        Some(Arc::new(RdapChecker::new()?))
    } else {
        None
    };

    let id_checker = if check_id {
        Some(Arc::new(IdChecker::new()?))
    } else {
        None
    };

    let rate_limiter = Arc::new(DualRateLimiter::new(eth_rps, box_rps, id_rps));

    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let shutdown_tx_clone = shutdown_tx.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        println!("\n\nReceived Ctrl+C, shutting down gracefully...");
        let _ = shutdown_tx_clone.send(());
    });

    let state = checkpoint.get_state().await;
    let words_to_check: Vec<_> = words
        .into_iter()
        .filter(|w| {
            let needs_eth = check_eth && state.needs_check(w, DomainType::Eth);
            let needs_box = check_box && state.needs_check(w, DomainType::Box);
            let needs_id = check_id && state.needs_check(w, DomainType::Id);
            needs_eth || needs_box || needs_id
        })
        .collect();

    if words_to_check.is_empty() {
        println!("All words have been checked!");
        print_status(&checkpoint.get_state().await);
        return Ok(());
    }

    println!("{} words remaining to check", words_to_check.len());

    let progress = ProgressBar::new(words_to_check.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}",
            )
            .unwrap()
            .progress_chars("=>-"),
    );

    let mut shutdown_rx = shutdown_tx.subscribe();

    let process_future = process_words(
        words_to_check,
        checkpoint.clone(),
        ens_checker,
        rdap_checker,
        id_checker,
        rate_limiter,
        concurrency,
        progress.clone(),
        check_eth,
        check_box,
        check_id,
    );

    tokio::select! {
        result = process_future => {
            result?;
        }
        _ = shutdown_rx.recv() => {
            progress.finish_with_message("Interrupted");
        }
    }

    checkpoint.save().await?;

    progress.finish_with_message("Done");
    print_status(&checkpoint.get_state().await);

    Ok(())
}

async fn process_words(
    words: Vec<String>,
    checkpoint: Arc<CheckpointManager>,
    ens_checker: Option<Arc<EnsChecker>>,
    rdap_checker: Option<Arc<RdapChecker>>,
    id_checker: Option<Arc<IdChecker>>,
    rate_limiter: Arc<DualRateLimiter>,
    concurrency: usize,
    progress: ProgressBar,
    check_eth: bool,
    check_box: bool,
    check_id: bool,
) -> Result<()> {
    stream::iter(words)
        .map(|word| {
            let checkpoint = checkpoint.clone();
            let ens_checker = ens_checker.clone();
            let rdap_checker = rdap_checker.clone();
            let id_checker = id_checker.clone();
            let rate_limiter = rate_limiter.clone();
            let progress = progress.clone();

            async move {
                if check_eth {
                    if let Some(ref checker) = ens_checker {
                        if checkpoint.needs_check(&word, DomainType::Eth).await {
                            rate_limiter.wait_eth().await;
                            let result = check_with_retry(
                                || checker.check_available(&word),
                                &word,
                                DomainType::Eth,
                            )
                            .await;

                            handle_result(&checkpoint, &word, DomainType::Eth, result).await;
                        }
                    }
                }

                if check_box {
                    if let Some(ref checker) = rdap_checker {
                        if checkpoint.needs_check(&word, DomainType::Box).await {
                            rate_limiter.wait_box().await;
                            let result = check_with_retry(
                                || checker.check_available(&word),
                                &word,
                                DomainType::Box,
                            )
                            .await;

                            handle_result(&checkpoint, &word, DomainType::Box, result).await;
                        }
                    }
                }

                if check_id {
                    if let Some(ref checker) = id_checker {
                        if checkpoint.needs_check(&word, DomainType::Id).await {
                            rate_limiter.wait_id().await;
                            let result = check_with_retry(
                                || checker.check_available(&word),
                                &word,
                                DomainType::Id,
                            )
                            .await;

                            handle_result(&checkpoint, &word, DomainType::Id, result).await;
                        }
                    }
                }

                progress.inc(1);
                progress.set_message(format!("{}", word));
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    Ok(())
}

async fn check_with_retry<F, Fut>(check_fn: F, word: &str, domain_type: DomainType) -> CheckResult
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = CheckResult>,
{
    let mut attempt = 0;

    loop {
        let result = check_fn().await;

        if !result.is_retryable() || attempt >= DEFAULT_MAX_RETRIES {
            return result;
        }

        attempt += 1;
        let backoff = calculate_backoff(attempt, DEFAULT_INITIAL_BACKOFF_MS, DEFAULT_MAX_BACKOFF_SECS);

        tracing::debug!(
            word = %word,
            domain_type = %domain_type,
            attempt = attempt,
            backoff_ms = backoff.as_millis(),
            "Retrying after error"
        );

        tokio::time::sleep(backoff).await;
    }
}

async fn handle_result(
    checkpoint: &CheckpointManager,
    word: &str,
    domain_type: DomainType,
    result: CheckResult,
) {
    let status = result.to_status();
    let error = result.error_message();

    if let Err(e) = checkpoint
        .record_result(word, domain_type, status, error.clone())
        .await
    {
        tracing::error!(
            word = %word,
            domain_type = %domain_type,
            error = %e,
            "Failed to record result"
        );
    }

    if result.is_retryable() {
        let entry = RetryEntry {
            word: word.to_string(),
            domain_type,
            retry_count: DEFAULT_MAX_RETRIES,
            next_retry_at: Utc::now() + chrono::Duration::minutes(5),
            last_error: error.unwrap_or_default(),
        };
        checkpoint.add_to_retry(entry).await;
    }
}
