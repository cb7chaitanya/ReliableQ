//! p50/p95/p99/max/throughput helpers computed from raw per-request or
//! per-job duration samples. Never estimated from a histogram bucket —
//! every scenario keeps the raw millisecond samples in memory (job counts
//! here are small enough, tens of thousands at most, for that to be
//! cheap) and sorts once.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub unit: String,
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
    pub max: Option<f64>,
    pub count: usize,
}

impl LatencyPercentiles {
    pub fn empty(unit: &str) -> Self {
        Self {
            unit: unit.to_string(),
            p50: None,
            p95: None,
            p99: None,
            max: None,
            count: 0,
        }
    }

    /// `samples_ms` need not be sorted; this sorts a local copy.
    pub fn from_samples_ms(samples_ms: &[f64]) -> Self {
        if samples_ms.is_empty() {
            return Self::empty("ms");
        }
        let mut sorted = samples_ms.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        Self {
            unit: "ms".to_string(),
            p50: Some(percentile(&sorted, 0.50)),
            p95: Some(percentile(&sorted, 0.95)),
            p99: Some(percentile(&sorted, 0.99)),
            max: sorted.last().copied(),
            count: sorted.len(),
        }
    }
}

/// Nearest-rank percentile over an already-sorted slice.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (p * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

pub fn throughput_per_sec(count: usize, elapsed_secs: f64) -> f64 {
    if elapsed_secs <= 0.0 {
        0.0
    } else {
        count as f64 / elapsed_secs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_of_a_known_sequence() {
        let samples: Vec<f64> = (1..=100).map(|n| n as f64).collect();
        let p = LatencyPercentiles::from_samples_ms(&samples);
        assert_eq!(p.p50, Some(51.0));
        assert_eq!(p.max, Some(100.0));
        assert_eq!(p.count, 100);
    }

    #[test]
    fn empty_samples_produce_none() {
        let p = LatencyPercentiles::from_samples_ms(&[]);
        assert!(p.p50.is_none());
        assert_eq!(p.count, 0);
    }
}
