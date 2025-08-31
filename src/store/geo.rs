use crate::store::{core::InMemoryStore, geo_score::encode};

impl InMemoryStore {
    pub async fn geoadd(&self, key: String, lat: f64, long: f64, member: String) -> i64 {
        self.zadd(key, encode(lat, long), member).await
    }
}
