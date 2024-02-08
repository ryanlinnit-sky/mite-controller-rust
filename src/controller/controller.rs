use serde::Deserialize;
use std::collections::HashMap;
use zmq::{Context, DONTWAIT, REP};

use super::runner_tracker::RunnerTracker;
use super::scenario_manager::ScenarioManager;

pub struct WorkTracker {
    all_work: HashMap<i32, HashMap<i32, i32>>,
    total_work: HashMap<i32, i32>,
}

impl WorkTracker {
    pub fn new() -> Self {
        Self {
            all_work: HashMap::new(),
            total_work: HashMap::new(),
        }
    }

    pub fn set_actual(&mut self, runner_id: i32, work: HashMap<i32, i32>) {
        let runner_work = self.all_work.get(&runner_id);
        // println!("runner_work: {:?}", runner_work);

        // for k, v in self._all_work[runner_id].items():
        //     self._total_work[k] -= v
        if let Some(runner_work) = runner_work {
            for (k, v) in runner_work.iter() {
                match self.total_work.get_mut(&k) {
                    Some(total_work) => *total_work -= v,
                    None => {
                        println!("Key not found in total_work, adding key: {}", k);
                        self.total_work.insert(*k, 0);
                    }
                }
            }
        }

        // for k, v in work.items():
        //     self._total_work[k] += v
        for (k, v) in work.iter() {
            match self.total_work.get_mut(&k) {
                Some(total_work) => *total_work += v,
                None => {
                    println!("Key not found in total_work, adding key: {}", k);
                    self.total_work.insert(*k, *v);
                }
            }

            // println!("add work - k: {}, v: {}", k, v);
            // let total_work = self.total_work.get_mut(&k).unwrap();
            // *total_work += v;
        }
        // self._all_work[runner_id] = defaultdict(int, work)
        // println!("Adding work: {:?} for runner: {}", work, runner_id);
        self.all_work.insert(runner_id, work);
    }

    pub fn add_assumed(&mut self, runner_id: i32, work: HashMap<i32, i32>) {
        let current = self.all_work.get_mut(&runner_id).unwrap();
        // println!("current: {:?}", current);
        for (k, v) in work.iter() {
            if current.contains_key(k) {
                // let volume = scenario_volume_map.get(scenario_id).unwrap();
                // scenario_volume_map.insert(scenario_id.clone(), volume + 1);
                let current_work = current.get_mut(&k).unwrap();
                *current_work += v;
            } else {
                current.insert(k.clone(), v.clone());
            }

            if self.total_work.contains_key(k) {
                let total_work = self.total_work.get_mut(&k).unwrap();
                *total_work += v;
            } else {
                self.total_work.insert(k.clone(), v.clone());
            }
        }
    }
}

pub struct Controller {
    scenario_spec: String,
    message_socket: String,
    controller_socket: String,
    scenario_manager: ScenarioManager,
    work_tracker: WorkTracker,
    runner_tracker: RunnerTracker,
    runner_count: u64,
    debug: bool,
}

#[derive(Deserialize)]
struct MessageData {
    runner_id: i32,
    current_work: HashMap<i32, i32>,
    completed_data_ids: Vec<Option<(i32, i32)>>,
    max_work: Option<i32>,
}

enum MessageType {
    Hello,
    Heartbeat,
    RequestWork,
    Bye,
}
struct Message {
    message_type: MessageType,
    message: Option<MessageData>,
}

// static MESSAGE_TYPES: [MessageType; 4] = [
//     MessageType::Heartbeat,
//     MessageType::Hello,
//     MessageType::RequestWork,
//     MessageType::Bye,
// ];

impl Controller {
    pub fn new(
        scenario_spec: String,
        message_socket: String,
        controller_socket: String,
        scenario_manager: ScenarioManager,
        debug: bool,
    ) -> Self {
        Self {
            scenario_spec,
            message_socket,
            controller_socket,
            scenario_manager,
            work_tracker: WorkTracker::new(),
            runner_tracker: RunnerTracker::new(10, debug),
            runner_count: 0,
            debug,
        }
    }

    pub fn hello(&mut self) -> u64 {
        self.runner_count += 1;
        self.runner_count
    }

    pub fn get_runner_count(&self) -> u64 {
        self.runner_count
    }

    pub fn required_work_for_runner(
        &mut self,
        runner_id: i32,
        max_work: Option<i32>,
    ) -> Vec<(i32, i32, String, String)> {
        let runner_total = match self.work_tracker.total_work.get(&runner_id) {
            Some(runner_total) => *runner_total,
            None => 0,
        };
        let active_runner_ids = self.runner_tracker.get_active();
        let current_work = self.work_tracker.total_work.clone();
        let hit_rate = self.runner_tracker.get_hit_rate();
        let (work, scenario_volume_map) = self.scenario_manager.get_work(
            current_work,
            runner_total,
            active_runner_ids.len() as i32,
            max_work,
            hit_rate,
        );
        self.work_tracker
            .add_assumed(runner_id, scenario_volume_map);
        work
    }

    pub fn request_work(
        &mut self,
        runner_id: i32,
        current_work: HashMap<i32, i32>,
        completed_data_ids: Vec<Option<(i32, i32)>>,
        max_work: Option<i32>,
    ) -> (Vec<(i32, i32, String, String)>, HashMap<&str, &str>, bool) {
        self.work_tracker.set_actual(runner_id, current_work);
        self.runner_tracker.update(runner_id);
        self.scenario_manager
            .checkin_data(completed_data_ids.iter().map(|x| x.unwrap()).collect());

        let work = self.required_work_for_runner(runner_id, max_work);

        // println!("request_work - {} {:?}", runner_id, work);

        // [TODO] correct config
        //     self._config_manager.get_changes_for_runner(runner_id),
        let mut config = HashMap::new();
        config.insert("k", "v");

        let scenario_is_active = self.scenario_manager.is_active();
        (work, config, !scenario_is_active)
    }

    pub fn run_server(&mut self) {
        let zmq_context = Context::new();
        let socket = zmq_context.socket(REP).unwrap();
        match socket.bind(&self.controller_socket) {
            Ok(_) => {
                if self.debug {
                    println!("binding to {}", self.controller_socket);
                }
            }
            Err(e) => panic!("Failed to bind to socket {}: {}", self.controller_socket, e),
        }

        loop {
            let msg: Vec<u8> = match socket.recv_bytes(DONTWAIT) {
                Ok(msg) => msg,
                Err(_) => {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    continue;
                }
            };
            let msg: (i32, Option<MessageData>) = match rmp_serde::from_slice(&msg) {
                Ok(msg) => msg,
                Err(e) => {
                    eprintln!("Failed to parse message: {}", e);
                    println!("Message: {:?}", msg);
                    continue;
                }
            };

            let message = match msg.0 {
                0 => Message {
                    message_type: MessageType::Heartbeat,
                    message: msg.1,
                },
                1 => Message {
                    message_type: MessageType::Hello,
                    message: None,
                },
                2 => Message {
                    message_type: MessageType::RequestWork,
                    message: msg.1,
                },
                3 => Message {
                    message_type: MessageType::Bye,
                    message: msg.1,
                },
                _ => continue,
            };

            match message.message_type {
                MessageType::Hello => {
                    let runner_id = self.hello();
                    if self.debug {
                        println!("Hello received");
                        println!("Adding Runner id: {}", runner_id);
                        println!("Total Runner count: {}\n", self.get_runner_count());
                    }

                    // [TODO] correct config
                    let mut config = HashMap::new();
                    config.insert("k", "v");

                    let hello_resp = (runner_id, self.scenario_spec.to_string(), config);
                    let buf = rmp_serde::to_vec(&hello_resp).unwrap();
                    socket.send(&buf, 0).unwrap();
                }
                MessageType::Heartbeat => {
                    // [TODO] update runner tracker
                    println!("Heartbeat received");
                    // socket.send("Heartbeat received".as_bytes(), 0).unwrap();
                }
                MessageType::RequestWork => {
                    let message_data = message.message.unwrap();
                    let runner_id = message_data.runner_id;
                    let current_work = message_data.current_work;
                    let completed_data_ids = message_data.completed_data_ids;
                    let max_work = message_data.max_work;

                    let work_resp =
                        self.request_work(runner_id, current_work, completed_data_ids, max_work);
                    let buf = rmp_serde::to_vec(&work_resp).unwrap();
                    socket.send(&buf, 0).unwrap();
                }
                MessageType::Bye => {
                    // [TODO] update runner tracker
                    println!("Bye received");
                    // socket.send("Bye received".as_bytes(), 0).unwrap();
                }
            }

            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        // println!("Running controller with message socket: {}, controller socket: {}, scenario spec: {}, debug: {}", self.message_socket, self.controller_socket, self.scenario_spec, self.debug);
    }
}
