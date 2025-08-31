use crate::store::{
    coords::{decode, encode, haversine, Point},
    core::InMemoryStore,
    sorted_set::get_sorted_set,
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
        Some(haversine(&from, &to).to_string())
    }

    pub async fn geosearch(
        &self,
        key: String,
        point: Point,
        radius: f64,
        unit: String,
    ) -> Vec<String> {
        let mut res = vec![];
        let data = self.data.lock().await;
        let Some(set) = get_sorted_set(&data, &key) else {
            return res;
        };
        for (member, score) in set.set.iter() {
            let member_point = decode(*score);
            let distance = haversine(&point, &member_point);
            let distance_in_unit = match unit.to_lowercase().as_str() {
                "km" => distance / 1000.0,
                "mi" => distance * 0.621371 * 1000.0,
                "ft" => distance * 3280.84 * 1000.0,
                _ => distance,
            };
            if distance_in_unit <= radius {
                res.push(member.clone());
            }
        }
        res
    }
}
