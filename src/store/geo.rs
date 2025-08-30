use crate::store::core::InMemoryStore;
use rust_decimal::Decimal;

const MIN_LAT: f64 = -85.05112878;
const MAX_LAT: f64 = 85.05112878;

impl InMemoryStore {
    pub async fn geoadd(&self, key: String, long: Decimal, lat: Decimal, member: String) -> i64 {
        self.zadd(key, long, member).await
    }
}

pub fn validate(long: f64, lat: f64) -> Option<(Decimal, Decimal)> {
    if (-180.0..=180.0).contains(&long) && (MIN_LAT..=MAX_LAT).contains(&lat) {
        Some((
            Decimal::from_f64_retain(long)?,
            Decimal::from_f64_retain(lat)?,
        ))
    } else {
        None
    }
}
