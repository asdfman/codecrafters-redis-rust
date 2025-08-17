use hashbrown::HashMap;
use rust_decimal::Decimal;
use skiplist::OrderedSkipList;

use super::{
    core::InMemoryStore,
    value::{Value, ValueWrapper},
};

pub struct SortedSet {
    pub set: HashMap<String, Decimal>,
    pub scores: OrderedSkipList<(Decimal, String)>,
}

impl Default for SortedSet {
    fn default() -> Self {
        let mut scores = OrderedSkipList::new();
        unsafe {
            scores.sort_by(|a: &(Decimal, String), b| a.cmp(b));
        }
        Self {
            set: HashMap::new(),
            scores,
        }
    }
}

impl SortedSet {
    pub fn insert(&mut self, member: String, score: Decimal) -> i64 {
        let mut updated_count = 1;
        if let Some(cur_score) = self.set.get(&member) {
            self.scores.remove(&(*cur_score, member.clone()));
            updated_count -= 1;
        }
        self.set.insert(member.clone(), score);
        self.scores.insert((score, member));
        updated_count
    }

    pub fn remove(&mut self, member: String) {
        if let Some(score) = self.set.remove(&member) {
            self.scores.remove(&(score, member));
        }
    }

    pub fn get_score(&self, member: &str) -> Option<&Decimal> {
        self.set.get(member)
    }

    pub fn get_rank(&self, member: &str) -> Option<usize> {
        let score = self.set.get(member)?;
        self.scores.index_of(&(*score, member.to_string()))
    }
}

impl InMemoryStore {
    pub async fn add_sorted_set(&self, key: String, score: Decimal, member: String) -> i64 {
        let mut data = self.data.lock().await;
        if let ValueWrapper {
            value: Value::SortedSet(set),
            ..
        } = data.entry(key.clone()).or_insert(ValueWrapper {
            value: Value::SortedSet(SortedSet::default()),
            expiry: None,
        }) {
            return set.insert(member, score);
        }
        0
    }

    pub async fn zrank(&self, key: String, member: String) -> Option<usize> {
        let data = self.data.lock().await;
        if let ValueWrapper {
            value: Value::SortedSet(set),
            ..
        } = data.get(&key)?
        {
            set.get_rank(&member)
        } else {
            None
        }
    }

    // pub async fn zrange(&self, key: String, start: isize, end: isize) ->
}
