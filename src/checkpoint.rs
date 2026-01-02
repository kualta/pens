use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tokio::fs;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DomainStatus {
    Available,
    Taken,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DomainType {
    Eth,
    Box,
    Id,
}

impl std::fmt::Display for DomainType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DomainType::Eth => write!(f, "eth"),
            DomainType::Box => write!(f, "box"),
            DomainType::Id => write!(f, "id"),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WordResult {
    pub eth_status: Option<DomainStatus>,
    pub box_status: Option<DomainStatus>,
    pub id_status: Option<DomainStatus>,
    pub eth_error: Option<String>,
    pub box_error: Option<String>,
    pub id_error: Option<String>,
    #[serde(default)]
    pub checked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryEntry {
    pub word: String,
    pub domain_type: DomainType,
    pub retry_count: u32,
    pub next_retry_at: DateTime<Utc>,
    pub last_error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointState {
    pub version: u32,
    pub wordlist_url: Option<String>,
    pub wordlist_hash: String,
    pub total_words: usize,
    pub checked_words: HashSet<String>,
    pub results: HashMap<String, WordResult>,
    pub retry_queue: Vec<RetryEntry>,
    pub check_eth: bool,
    pub check_box: bool,
    #[serde(default)]
    pub check_id: bool,
    pub started_at: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

impl Default for CheckpointState {
    fn default() -> Self {
        let now = Utc::now();
        Self {
            version: 1,
            wordlist_url: None,
            wordlist_hash: String::new(),
            total_words: 0,
            checked_words: HashSet::new(),
            results: HashMap::new(),
            retry_queue: Vec::new(),
            check_eth: true,
            check_box: true,
            check_id: true,
            started_at: now,
            last_updated: now,
        }
    }
}

impl CheckpointState {
    pub fn new(wordlist_hash: String, total_words: usize, check_eth: bool, check_box: bool, check_id: bool) -> Self {
        let now = Utc::now();
        Self {
            version: 1,
            wordlist_url: None,
            wordlist_hash,
            total_words,
            checked_words: HashSet::new(),
            results: HashMap::new(),
            retry_queue: Vec::new(),
            check_eth,
            check_box,
            check_id,
            started_at: now,
            last_updated: now,
        }
    }

    pub async fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .await
            .with_context(|| format!("Failed to read checkpoint file: {}", path.display()))?;

        let state: Self = serde_json::from_str(&content)
            .with_context(|| "Failed to parse checkpoint JSON")?;

        Ok(state)
    }

    pub async fn save(&self, path: &Path) -> Result<()> {
        let content = serde_json::to_string_pretty(self)
            .with_context(|| "Failed to serialize checkpoint")?;

        let temp_path = path.with_extension("tmp");
        fs::write(&temp_path, &content)
            .await
            .with_context(|| format!("Failed to write temp checkpoint: {}", temp_path.display()))?;

        fs::rename(&temp_path, path)
            .await
            .with_context(|| format!("Failed to rename checkpoint: {}", path.display()))?;

        Ok(())
    }

    pub fn record_result(
        &mut self,
        word: &str,
        domain_type: DomainType,
        status: DomainStatus,
        error: Option<String>,
    ) {
        let result = self.results.entry(word.to_string()).or_default();
        result.checked_at = Some(Utc::now());

        match domain_type {
            DomainType::Eth => {
                result.eth_status = Some(status);
                result.eth_error = error;
            }
            DomainType::Box => {
                result.box_status = Some(status);
                result.box_error = error;
            }
            DomainType::Id => {
                result.id_status = Some(status);
                result.id_error = error;
            }
        }

        let eth_done = !self.check_eth || result.eth_status.is_some();
        let box_done = !self.check_box || result.box_status.is_some();
        let id_done = !self.check_id || result.id_status.is_some();

        if eth_done && box_done && id_done {
            self.checked_words.insert(word.to_string());
        }

        self.last_updated = Utc::now();
    }

    pub fn add_to_retry_queue(&mut self, entry: RetryEntry) {
        self.retry_queue.retain(|e| !(e.word == entry.word && e.domain_type == entry.domain_type));
        self.retry_queue.push(entry);
    }

    pub fn needs_check(&self, word: &str, domain_type: DomainType) -> bool {
        if let Some(result) = self.results.get(word) {
            match domain_type {
                DomainType::Eth => result.eth_status.is_none() || matches!(result.eth_status, Some(DomainStatus::Error)),
                DomainType::Box => result.box_status.is_none() || matches!(result.box_status, Some(DomainStatus::Error)),
                DomainType::Id => result.id_status.is_none() || matches!(result.id_status, Some(DomainStatus::Error)),
            }
        } else {
            true
        }
    }

    pub fn get_stats(&self) -> CheckpointStats {
        let mut eth_available = 0;
        let mut eth_taken = 0;
        let mut eth_error = 0;
        let mut box_available = 0;
        let mut box_taken = 0;
        let mut box_error = 0;
        let mut id_available = 0;
        let mut id_taken = 0;
        let mut id_error = 0;

        for result in self.results.values() {
            match result.eth_status {
                Some(DomainStatus::Available) => eth_available += 1,
                Some(DomainStatus::Taken) => eth_taken += 1,
                Some(DomainStatus::Error) => eth_error += 1,
                None => {}
            }
            match result.box_status {
                Some(DomainStatus::Available) => box_available += 1,
                Some(DomainStatus::Taken) => box_taken += 1,
                Some(DomainStatus::Error) => box_error += 1,
                None => {}
            }
            match result.id_status {
                Some(DomainStatus::Available) => id_available += 1,
                Some(DomainStatus::Taken) => id_taken += 1,
                Some(DomainStatus::Error) => id_error += 1,
                None => {}
            }
        }

        CheckpointStats {
            total_words: self.total_words,
            checked_words: self.checked_words.len(),
            eth_available,
            eth_taken,
            eth_error,
            box_available,
            box_taken,
            box_error,
            id_available,
            id_taken,
            id_error,
            retry_queue_size: self.retry_queue.len(),
            started_at: self.started_at,
            last_updated: self.last_updated,
        }
    }
}

#[derive(Debug)]
pub struct CheckpointStats {
    pub total_words: usize,
    pub checked_words: usize,
    pub eth_available: usize,
    pub eth_taken: usize,
    pub eth_error: usize,
    pub box_available: usize,
    pub box_taken: usize,
    pub box_error: usize,
    pub id_available: usize,
    pub id_taken: usize,
    pub id_error: usize,
    pub retry_queue_size: usize,
    pub started_at: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

pub struct CheckpointManager {
    state: Mutex<CheckpointState>,
    path: std::path::PathBuf,
    save_counter: Mutex<usize>,
    save_interval: usize,
}

impl CheckpointManager {
    pub fn new(state: CheckpointState, path: std::path::PathBuf, save_interval: usize) -> Self {
        Self {
            state: Mutex::new(state),
            path,
            save_counter: Mutex::new(0),
            save_interval,
        }
    }

    pub async fn record_result(
        &self,
        word: &str,
        domain_type: DomainType,
        status: DomainStatus,
        error: Option<String>,
    ) -> Result<()> {
        {
            let mut state = self.state.lock().await;
            state.record_result(word, domain_type, status, error);
        }

        let should_save = {
            let mut counter = self.save_counter.lock().await;
            *counter += 1;
            if *counter >= self.save_interval {
                *counter = 0;
                true
            } else {
                false
            }
        };

        if should_save {
            self.save().await?;
        }

        Ok(())
    }

    pub async fn save(&self) -> Result<()> {
        let state = self.state.lock().await;
        state.save(&self.path).await
    }

    pub async fn get_state(&self) -> CheckpointState {
        self.state.lock().await.clone()
    }

    pub async fn needs_check(&self, word: &str, domain_type: DomainType) -> bool {
        self.state.lock().await.needs_check(word, domain_type)
    }

    pub async fn add_to_retry(&self, entry: RetryEntry) {
        self.state.lock().await.add_to_retry_queue(entry);
    }
}
