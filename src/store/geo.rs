use crate::store::{
    coords::{decode, encode, haversine, Point},
    core::InMemoryStore,
};

impl InMemoryStore {
    pub async fn geoadd(&self, key: String, point: Point, member: String) -> i64 {
        self.zadd(key, encode(point), member).await
    }

    pub async fn geopos(&self, key: String, members: Vec<String>) -> Vec<Vec<String>> {
        let mut result = vec![];
        for member in members.into_iter() {
            if let Some(coords) = self
                .zscore(key.clone(), member)
                .await
                .map(decode)
                .map(|point| vec![point.lon.to_string(), point.lat.to_string()])
            {
                result.push(coords);
            } else {
                result.push(vec![]);
            }
        }
        result
    }

    pub async fn geodist(&self, key: String, from: String, to: String) -> Option<String> {
        let from = self.zscore(key.clone(), from).await.map(decode)?;
        let to = self.zscore(key.clone(), to).await.map(decode)?;
        Some(haversine(from, to).to_string())
    }

    pub(crate) async fn geosearch(
        &self,
        key: String,
        point: Point,
        radius: f64,
        unit: String,
    ) -> Vec<String> {
        todo!()
    }
}
