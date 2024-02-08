use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::types::PyTuple;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct Scenario {
    journey_spec: Py<PyAny>,
    datapool: Py<PyAny>,
    volumemodel: Py<PyAny>,
}

#[derive(Clone)]
pub struct ScenarioManager {
    in_start: bool,
    period: u64,
    delay: u64,
    start_time: u64,
    spawn_rate: u64,
    current_period_end: u64,
    debug: bool,
    required: HashMap<i32, i32>,
    scenarios: HashMap<i32, Scenario>,
    scenario_id_gen: i32,
}

impl ScenarioManager {
    pub fn new(period: u64, delay: u64, spawn_rate: u64, debug: bool) -> Self {
        let start_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            in_start: delay > 0,
            period,
            delay,
            start_time,
            spawn_rate,
            current_period_end: 0,
            debug,
            required: HashMap::new(),
            scenarios: HashMap::new(),
            scenario_id_gen: 0,
        }
    }

    pub fn _now(&self) -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - self.start_time
    }

    pub fn get_work(
        &mut self,
        current_work: HashMap<i32, i32>,
        num_runner_current_work: i32,
        num_runners: i32,
        runner_self_limit: Option<i32>,
        hit_rate: f64,
    ) -> (Vec<(i32, i32, String, String)>, HashMap<i32, i32>) {
        // println!("Inside get_work");
        let required = self.get_required_work();
        let diff = Self::remove_a_from_b(current_work.clone(), required.clone());
        let total = required.len() as i32;
        let mut runners_share_limit =
            (total as f64) / (num_runners as f64) - (num_runner_current_work as f64);

        // python controller
        // total=60 num_runners=1 num_runner_current_work=0 runners_share_limit=60.0
        // rust controller
        // total=0 num_runners=1 num_runner_current_work=0 runners_share_limit=0.0

        println!(
            "total={:?} num_runners={:?} num_runner_current_work={:?} runners_share_limit={:?}",
            total, num_runners, num_runner_current_work, runners_share_limit
        );

        runners_share_limit = 6.0;

        // let mut limit = max(0, runners_share_limit);
        let mut limit = runners_share_limit.max(0.0);

        // println!("1. limit: {}", limit);

        // if runner_self_limit is set
        if let Some(runner_self_limit) = runner_self_limit {
            // limit = min(limit, runners_share_limit);
            limit = limit.min(runner_self_limit as f64);
        }

        // println!("2. limit: {}", limit);

        let spawn_limit = self.spawn_rate as f64 / hit_rate;
        // limit = min(limit, spawn_limit);
        limit = limit.min(spawn_limit);

        // println!("spawn_rate: {} hit_rate: {}", self.spawn_rate, hit_rate);

        // println!("3. limit: {}", limit);

        if limit % 1.0 != 0.0 && (limit % 1.0 > 0.4) {
            limit = limit + 1.0;
        }

        // println!("4. limit: {}", limit);

        // [TODO] shuffle the keys of diff
        // let mut diff_keys = diff.keys().collect::<Vec<_>>();
        // diff_keys.shuffle(&mut thread_rng());

        let mut work: Vec<(i32, i32, String, String)> = vec![];
        let mut scenario_volume_map: HashMap<i32, i32> = HashMap::new();

        for scenario_id in diff.keys() {
            // if size of work is greater than limit, break
            if work.len() as f64 >= limit {
                // println!("breaking because work.len() as f64 >= limit");
                // println!("work.len(): {}", work.len());
                // println!("limit: {}", limit);
                break;
            }
            // println!("------------");
            // println!("scenario_id: {}", scenario_id);
            // println!("self.scenarios: {:?}", self.scenarios);
            // println!("------------");
            if self.scenarios.contains_key(scenario_id) {
                let scenario = self.scenarios.get(scenario_id).unwrap();
                work.push((
                    scenario_id.clone(),
                    1 as i32,
                    scenario.journey_spec.to_string(),
                    "".to_string(),
                ));
                // work.append((scenario_id, None, scenario.journey_spec, None))
            }

            if scenario_volume_map.contains_key(scenario_id) {
                let volume = scenario_volume_map.get(scenario_id).unwrap();
                scenario_volume_map.insert(scenario_id.clone(), volume + 1);
            } else {
                scenario_volume_map.insert(scenario_id.clone(), 1);
            }

            // let volume = required.get(scenario_id).unwrap();
            // let work_volume = min(*volume, limit as i32);
            // limit -= work_volume as f64;
            // scenario_volume_map.insert(scenario_id, work_volume);
        }

        // println!("required: {:?}", required);
        // println!("current_work: {:?}", current_work);
        // println!("diff: {:?}", diff);
        // println!("total: {:?}", total);
        // println!("work: {:?}", work);

        // let mut work = HashMap::new();
        // work.insert(1, 1);

        // let work: Vec<(i32, i32, String, String)> = vec![(1, 1, "t:j".to_string(), "".to_string())];

        (work, scenario_volume_map)
    }

    pub fn remove_a_from_b(a: HashMap<i32, i32>, b: HashMap<i32, i32>) -> HashMap<i32, i32> {
        let mut c = b.clone();
        for (k, v) in a.iter() {
            let b_v = match b.get(k) {
                Some(b_v) => *b_v,
                None => 0,
            };
            if b_v <= *v {
                c.remove(k);
            } else {
                c.insert(*k, b_v - v);
            }
        }
        c
    }

    pub fn now(&mut self) -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - self.start_time
    }

    pub fn get_required_work(&mut self) -> HashMap<i32, i32> {
        let now = self.now();
        if self.in_start {
            if now <= self.delay {
                return self.required.clone();
            }
            self.in_start = false;
            self.start_time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
        }
        if now >= self.current_period_end {
            // println!("calling - Updating required and period");
            println!(
                "current_period_end: {} now {} period {}",
                self.current_period_end, now, self.period
            );
            self.update_required_and_period(self.current_period_end, (now + self.period) as u64);
        }
        self.required.clone()
    }

    pub fn add_scenario(
        &mut self,
        journey_spec: Py<PyAny>,
        datapool: Py<PyAny>,
        volumemodel: Py<PyAny>,
    ) {
        let scenario_id = self.scenario_id_gen;
        self.scenario_id_gen += 1;
        let journey_spec_debug = journey_spec.clone();
        let datapool_debug = datapool.clone();
        let volumemodel_debug = volumemodel.clone();
        self.scenarios.insert(
            scenario_id,
            Scenario {
                journey_spec,
                datapool,
                volumemodel,
            },
        );
        println!(
            "Added scenario id={} journey_spec={} datapool={} volumemodel={}",
            scenario_id, journey_spec_debug, datapool_debug, volumemodel_debug
        );
    }

    pub fn update_required_and_period(&mut self, start_of_period: u64, end_of_period: u64) {
        let mut required = HashMap::new();
        // let mut scenarios = self.scenarios.clone();
        // for (scenario_id, scenario) in scenarios.iter_mut() {
        for scenerio in self.scenarios.iter_mut() {
            let scenario_id = scenerio.0.clone();
            let volume_model = &scenerio.1.volumemodel;

            // println!("volume_model: {}", volume_model);
            // println!("start_of_period: {}", start_of_period);
            // println!("end_of_period: {}", end_of_period);
            // required.insert(1, 1);
            // println!("scenerio: {:?}", scenerio);

            // let path = Path::new("/home/ryan/dev/mite-controller-rust");
            // let py_app = fs::read_to_string(path.join("t.py"));
            let from_python = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                let vm_result = volume_model.call1(py, (start_of_period, end_of_period))?;
                // [TODO] only insert if vm_result is `Ok`, if a Python exception is raised, we should remove the scenario
                //        from self.scenarios
                // println!("scenario_id: {} vm_result: {}", scenario_id, vm_result);
                required.insert(scenario_id, vm_result.extract::<u64>(py)? as i32);
                Ok(vm_result)
            });

            // match scenario.volumemodel(start_of_period, end_of_period) {
            //     Ok(number) => {
            //         required.insert(*scenario_id, number as u64);
            //     }
            //     Err(_) => {
            //         println!(
            //             "Removed scenario {} because volume model raised StopVolumeModel",
            //             scenario_id
            //         );
            //         self.scenarios.remove(scenario_id);
            //         if self.scenarios.is_empty() {
            //             println!("All scenarios have been removed from scenario tracker");
            //         }
            //     }
            // }
        }
        self.current_period_end = end_of_period;

        // println!("required: {:?}", required);

        self.required = required;
    }

    pub fn get_python_scenario(
        &mut self,
        scenario_spec: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // scenario_spec is in the format module:scenario_function_name
        // first split on the colon into module and function name variables
        let split: Vec<&str> = scenario_spec.split(":").collect();
        let module = split[0];
        let function_name = split[1];

        // [TODO]
        let path = Path::new("/Users/rli14/identity/mite-controller-rust");
        let py_app = fs::read_to_string(path.join("t.py"))?;
        let from_python = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
            // let syspath: &PyList = py.import("sys")?.getattr("path")?.downcast()?;
            // syspath.insert(0, &path)?;
            let app: Py<PyAny> = PyModule::from_code(py, &py_app, "", "")?
                .getattr(function_name)?
                .into();
            let generator = app.call0(py)?;
            loop {
                match generator.call_method0(py, "__next__") {
                    Ok(value) => {
                        let tuple: &PyTuple = value.extract(py)?;
                        let journey_spec: Py<PyAny> = tuple.get_item(0)?.into();
                        let datapool: Py<PyAny> = tuple.get_item(1)?.into();
                        let volumemodel: Py<PyAny> = tuple.get_item(2)?.into();

                        self.add_scenario(journey_spec, datapool, volumemodel);

                        // println!("journey_spec: {}, datapool: {}, volumemodel: {}", journey_spec, datapool, volumemodel);
                    }
                    Err(e) => {
                        // if self.debug {
                        //     eprintln!("Exiting scenario generator: {}", e);
                        // }
                        break;
                    }
                }
            }
            Ok(generator)
        });

        // [TODO] there's no reason we need to return a `Result` here?
        Ok(())
    }

    pub fn checkin_data(&mut self, ids: Vec<(i32, i32)>) {
        for id in ids {
            let scenario_id = id.0;
            let scenario_data_id = id.1;
            if self.scenarios.contains_key(&scenario_id) {
                // self.scenarios[scenario_id].datapool.checkin(scenario_data_id);
            }
        }
    }

    pub fn is_active(&self) -> bool {
        self.in_start || !self.scenarios.is_empty()
    }
}
