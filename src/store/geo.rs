use crate::store::{
    core::InMemoryStore,
    geo_score::{decode, encode},
};

impl InMemoryStore {
    pub async fn geoadd(&self, key: String, lat: f64, long: f64, member: String) -> i64 {
        self.zadd(key, encode(lat, long), member).await
    }

    pub async fn geopos(&self, key: String, members: Vec<String>) -> Vec<Vec<String>> {
        let mut result = vec![];
        for member in members.into_iter() {
            if let Some(coords) = self
                .zscore(key.clone(), member)
                .await
                .map(decode)
                .map(|(lat, long)| vec![lat.to_string(), long.to_string()])
            {
                result.push(coords);
            }
        }
        result
    }
}
