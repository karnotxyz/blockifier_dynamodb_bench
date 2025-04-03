use std::sync::Mutex;
use std::time::Duration;

#[derive(Debug, Default)]
pub struct OperationMetrics {
    pub durations: Vec<Duration>,
}

impl OperationMetrics {
    pub fn add_duration(&mut self, duration: Duration) {
        self.durations.push(duration);
    }

    pub fn average_duration(&self) -> Option<Duration> {
        if self.durations.is_empty() {
            return None;
        }
        let total: Duration = self.durations.iter().sum();
        Some(total / self.durations.len() as u32)
    }

    pub fn median_duration(&self) -> Option<Duration> {
        if self.durations.is_empty() {
            return None;
        }
        let mut sorted_durations = self.durations.clone();
        sorted_durations.sort();
        Some(sorted_durations[sorted_durations.len() / 2])
    }
}

#[derive(Debug, Default)]
pub struct StateMetrics {
    pub storage_reads: Mutex<OperationMetrics>,
    pub nonce_reads: Mutex<OperationMetrics>,
    pub class_hash_reads: Mutex<OperationMetrics>,
    pub compiled_class_hash_reads: Mutex<OperationMetrics>,
    pub state_updates: Mutex<OperationMetrics>,
}

impl StateMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}
