use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;

pub struct RunnerTracker {
    hits: Vec<u64>,
    last_seen: HashMap<i32, u64>,
    timeout: u64,
    debug: bool,
}

impl RunnerTracker {
    pub fn new(timeout: u64, debug: bool) -> Self {
        Self {
            hits: Vec::new(),
            last_seen: HashMap::new(),
            timeout: timeout,
            debug,
        }
    }

    pub fn update(&mut self, runner_id: i32) {
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_seen.insert(runner_id, t);
        self.hits.push(t);
        if self.hits[0] < t - self.timeout {
            self.hits.remove(0);
        }
    }

    pub fn get_active(&self) -> Vec<i32> {
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut active = Vec::new();
        for (k, v) in self.last_seen.iter() {
            if *v + self.timeout > t {
                active.push(*k);
            }
        }
        active
    }

    pub fn get_hit_rate(&self) -> f64 {
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut hits = self.hits.clone();
        while hits[0] < t - self.timeout {
            hits.remove(0);
        }
        hits.len() as f64 / self.timeout as f64
    }



    // pub fn heartbeat(&mut self) {
    //     self.hits += 1;
    //     self.last_heartbeat = SystemTime::now()
    //         .duration_since(UNIX_EPOCH)
    //         .unwrap()
    //         .as_secs();
    // }

    // pub fn is_alive(&self) -> bool {
    //     let now = SystemTime::now()
    //         .duration_since(UNIX_EPOCH)
    //         .unwrap()
    //         .as_secs();

    //     now - self.last_heartbeat < self.timeout
    // }

    // pub fn get_hits(&self) -> u64 {
    //     self.hits
    // }
}
