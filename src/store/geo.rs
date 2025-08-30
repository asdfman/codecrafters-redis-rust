use crate::store::core::InMemoryStore;
use rust_decimal::Decimal;

impl InMemoryStore {
    pub async fn geoadd(&self, key: String, long: Decimal, lat: Decimal, member: String) -> i64 {
        self.zadd(key, long, member).await
    }
}
