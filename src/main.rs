use std::env;
use std::process;

use light_r::Engine;

fn main() {
    let mut engine = Engine::new();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: light-r <command_string>");
        process::exit(1);
    }

    let command_string = &args[1];
    if let Err(err) = engine.process_commands(command_string) {
        eprintln!("Error: {}", err);
        process::exit(1);
    }
}