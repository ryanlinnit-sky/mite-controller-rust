use clap::Parser;

mod controller;
use controller::controller::Controller;
use controller::scenario_manager::ScenarioManager;



/// The controller dictates the scenario to run.
/// It is responsible for distributing work to the runners
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Message socket
    #[arg(long, default_value = "tcp://127.0.0.1:14302")]
    message_socket: String,

    /// Controller socket
    #[arg(long, default_value = "tcp://0.0.0.0:14301")]
    controller_socket: String,

    /// Scenario spec.
    #[arg()]
    scenario_spec: String,

    // Start delay
    #[arg(long, default_value = "0")]
    delay_start_seconds: u64,

    // period
    #[arg(long, default_value = "1")]
    max_loop_delay: u64,

    // spawn rate
    #[arg(long, default_value = "1000")]
    spawn_rate: u64,

    /// Debug mode
    #[arg(long)]
    debug: bool,
}


fn main() {
    let args = Args::parse();

    let mut scenario_manager = ScenarioManager::new(
        args.max_loop_delay,
        args.delay_start_seconds,
        args.spawn_rate,
        true,
    );
    scenario_manager.get_python_scenario(args.scenario_spec.to_string()).unwrap();

    let mut controller = Controller::new(
        args.scenario_spec.to_string(),
        args.message_socket,
        args.controller_socket,
        scenario_manager,
        args.debug,
    );
    controller.run_server();
}
