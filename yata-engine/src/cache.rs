use std::collections::HashMap;
use std::time::{Duration, Instant};

struct CacheEntry {
    rows: Vec<Vec<(String, String)>>,
    at: Instant,
}

/// LRU query cache with TTL and generation-based invalidation.
pub struct QueryCache {
    m: HashMap<String, CacheEntry>,
    order: Vec<String>,
    generation: u64,
    max_entries: usize,
    ttl: Duration,
}

impl QueryCache {
    pub fn new(max_entries: usize, ttl_secs: u64) -> Self {
        Self {
            m: HashMap::new(),
            order: Vec::new(),
            generation: 0,
            max_entries,
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    pub fn get(&self, key: &str) -> Option<&Vec<Vec<(String, String)>>> {
        let e = self.m.get(key)?;
        if e.at.elapsed() > self.ttl {
            return None;
        }
        Some(&e.rows)
    }

    pub fn put(&mut self, key: String, rows: Vec<Vec<(String, String)>>) {
        if self.m.len() >= self.max_entries {
            if let Some(old) = self.order.first().cloned() {
                self.m.remove(&old);
                self.order.remove(0);
            }
        }
        self.order.retain(|x| x != &key);
        self.order.push(key.clone());
        self.m.insert(
            key,
            CacheEntry {
                rows,
                at: Instant::now(),
            },
        );
    }

    pub fn invalidate(&mut self) {
        self.generation += 1;
        self.m.clear();
        self.order.clear();
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Build cache key from cypher + params + RLS org_id.
pub fn cache_key(cypher: &str, params: &[(String, String)], rls: Option<&str>) -> String {
    let mut k = cypher.to_string();
    for (pk, pv) in params {
        k.push(':');
        k.push_str(pk);
        k.push('=');
        k.push_str(pv);
    }
    if let Some(o) = rls {
        k.push_str(":rls=");
        k.push_str(o);
    }
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_hit_miss() {
        let mut c = QueryCache::new(256, 30);
        c.put("q1".into(), vec![vec![("a".into(), "1".into())]]);
        assert!(c.get("q1").is_some());
        assert!(c.get("q2").is_none());
    }

    #[test]
    fn test_cache_invalidation() {
        let mut c = QueryCache::new(256, 30);
        c.put("q1".into(), vec![vec![("a".into(), "1".into())]]);
        assert!(c.get("q1").is_some());
        c.invalidate();
        assert!(c.get("q1").is_none());
    }

    #[test]
    fn test_cache_lru_eviction() {
        let mut c = QueryCache::new(2, 30);
        c.put("q1".into(), vec![]);
        c.put("q2".into(), vec![]);
        c.put("q3".into(), vec![]);
        assert!(c.get("q1").is_none()); // evicted
        assert!(c.get("q2").is_some());
        assert!(c.get("q3").is_some());
    }

    #[test]
    fn test_cache_key() {
        let k = cache_key(
            "MATCH (n) RETURN n",
            &[("x".into(), "1".into())],
            Some("org_1"),
        );
        assert!(k.contains("MATCH"));
        assert!(k.contains(":x=1"));
        assert!(k.contains(":rls=org_1"));
    }

    #[test]
    fn test_cache_key_no_rls() {
        let k = cache_key("MATCH (n) RETURN n", &[], None);
        assert!(!k.contains(":rls="));
    }

    #[test]
    fn test_cache_lru_single_entry() {
        let mut c = QueryCache::new(1, 30);
        c.put("q1".into(), vec![vec![("a".into(), "1".into())]]);
        assert!(c.get("q1").is_some());
        c.put("q2".into(), vec![vec![("b".into(), "2".into())]]);
        assert!(c.get("q1").is_none()); // evicted
        assert!(c.get("q2").is_some());
    }

    #[test]
    fn test_cache_overwrite_same_key() {
        let mut c = QueryCache::new(256, 30);
        c.put("q1".into(), vec![vec![("a".into(), "old".into())]]);
        c.put("q1".into(), vec![vec![("a".into(), "new".into())]]);
        let rows = c.get("q1").unwrap();
        assert_eq!(rows[0][0].1, "new");
    }

    #[test]
    fn test_cache_generation_increments() {
        let mut c = QueryCache::new(256, 30);
        assert_eq!(c.generation(), 0);
        c.invalidate();
        assert_eq!(c.generation(), 1);
        c.invalidate();
        assert_eq!(c.generation(), 2);
    }

    #[test]
    fn test_cache_ttl_zero_always_misses() {
        let mut c = QueryCache::new(256, 0);
        c.put("q1".into(), vec![]);
        // TTL=0 means entries expire immediately
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(c.get("q1").is_none());
    }

    #[test]
    fn test_cache_empty_rows() {
        let mut c = QueryCache::new(256, 30);
        c.put("q1".into(), vec![]);
        assert_eq!(c.get("q1").unwrap().len(), 0);
    }

    #[test]
    fn test_cache_invalidate_clears_all() {
        let mut c = QueryCache::new(256, 30);
        for i in 0..10 {
            c.put(format!("q{i}"), vec![]);
        }
        c.invalidate();
        for i in 0..10 {
            assert!(c.get(&format!("q{i}")).is_none());
        }
    }
}
