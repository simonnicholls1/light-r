use std::env;
use std::io::{self, Read};
use std::process;
use light_r::engine::Engine;

fn main() {
    
    let args: Vec<String> = env::args().collect();
    println!("Arguments: {:?}", args); // Debug: Print all arguments

    if args.len() < 2 {
        // Check if `stdin` has input
        let mut buffer = String::new();
        if io::stdin().read_to_string(&mut buffer).is_ok() && !buffer.trim().is_empty() {
            println!("Stdin detected. Use a command to process the input.");
        } else {
            eprintln!("Usage: light-r <command_string>");
            process::exit(1);
        }
    }

    let mut engine = Engine::new();
    let command_string = &args[1];
    println!("Command String: {}", command_string); // Debug: Print command string

    if let Err(err) = engine.process_commands(command_string) {
        eprintln!("Error: {}", err);
        process::exit(1);
    }
}