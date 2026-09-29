use parking_lot::Mutex;
use std::time::{Duration, Instant, SystemTime};

/// Server backpressure applies to every endpoint sharing this transport.
#[derive(Default)]
pub(crate) struct RetryDelay(Mutex<Option<Instant>>);

impl RetryDelay {
    pub async fn wait(&self) {
        loop {
            let delay = self.0.lock().map(|deadline| deadline.saturating_duration_since(Instant::now()));
            match delay {
                Some(delay) if !delay.is_zero() => futures_timer::Delay::new(delay).await,
                _ => return,
            }
        }
    }

    pub fn record(&self, value: Option<&str>) {
        let Some(delay) = retry_after(value, SystemTime::now()) else { return };
        if delay.is_zero() { return; }
        let delay = delay.min(Duration::from_secs(86_400))
            + Duration::from_millis(u64::from(rand::random::<u8>()));
        let next = Instant::now() + delay;
        let mut deadline = self.0.lock();
        *deadline = Some(deadline.map_or(next, |current| current.max(next)));
    }
}

pub(crate) fn retry_after(value: Option<&str>, now: SystemTime) -> Option<Duration> {
    let text = value?.trim();
    if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) {
        return text.parse::<f64>().ok()
            .filter(|seconds| seconds.is_finite())
            .map(|seconds| Duration::from_secs_f64(seconds.min(86_400.0)));
    }
    // Optional advice cannot turn an HTTP failure into success. Invalid advice
    // leaves the normal failure policy in force; past dates require no delay.
    httpdate::parse_http_date(text).ok()
        .map(|date| date.duration_since(now).unwrap_or_default())
}
