use std::error::Error;
use crate::dataframe::DataFrame;
use crate::operations::dlog;

pub struct Engine {
}

impl Engine {
    pub fn new() -> Self {
        Engine {
        }
    }

    pub fn execute_command(&mut self, command: &str, args: &[String], df: Option<DataFrame>) -> Result<DataFrame, Box<dyn Error>> {
        match command {
            "load" => {
                let file_path = args.get(0).ok_or("File path missing for load")?;
                let new_df = DataFrame::new_from_csv(file_path, "row")?;
                return Ok(new_df);
            }
            // Pass the DataFrame to the dlog operation
            "dlog" => {
                let input_df = df.ok_or("No current DataFrame to process for dlog")?;
                let df = dlog::dlog(&input_df)?;
                return Ok(df)
            }

            //"after" => self.current_df = Some(after::main(self.current_df.as_ref().ok_or("No current DataFrame")?, &args[0])?),
            //"before" => self.current_df = Some(before::main(self.current_df.as_ref().ok_or("No current DataFrame")?, &args[0])?),
            //"cgrep" => self.current_df = Some(cgrep::main(self.current_df.as_ref().ok_or("No current DataFrame")?, args)?),
            //"signal" => self.current_df = Some(signal::main(self.current_df.as_ref().ok_or("No current DataFrame")?)?),
            //"dlog" => {self.current_df = Some(dlog(df)?);},
            // "unitscale" => {
            //     let window_size: usize = args[0].parse()?;
            //     let target_vol: f64 = args[1].parse()?;
            //     self.current_df = Some(vol_scale::main(self.current_df.as_ref().ok_or("No current DataFrame")?, window_size, target_vol)?);
            // },
            // "mult" => {
            //     let df2 = self.vars.get(&args[0]).ok_or("Variable not found")?;
            //     self.current_df = Some(multiply::main(self.current_df.as_ref().ok_or("No current DataFrame")?, df2)?);
            // },
            //"load" => self.current_df = Some(self.load_csv(&args[0])?),
            // "save" => {
            //     save::main(self.current_df.as_ref().ok_or("No current DataFrame")?, &args[0])?;
            // },
            // "->" => {
            //     self.vars.insert(args[0].clone(), self.current_df.clone().ok_or("No current DataFrame")?);
            // },
            // "ffill" => self.current_df = Some(ffill::main(self.current_df.as_ref().ok_or("No current DataFrame")?)),
            // "ewa" => self.current_df = Some(ewa::main(self.current_df.as_ref().ok_or("No current DataFrame")?)?),
            // "cumsum" => {
            //     let start_number: f64 = args[0].parse()?;
            //     self.current_df = Some(cumsum::main(self.current_df.as_mut().ok_or("No current DataFrame")?, start_number)?);
            // },
            // "shift" => {
            //     let period: i32 = args[0].parse()?;
            //     self.current_df = Some(shift::main(self.current_df.as_ref().ok_or("No current DataFrame")?, period)?);
            // },
            // "plot" => {
            //     plot::main(self.current_df.as_ref().ok_or("No current DataFrame")?)?;
            // },
            // "momentum" => {
            //     let lookback: usize = args[0].parse()?;
            //     let frequency: usize = args[1].parse()?;
            //     self.current_df = Some(momentum::main(self.current_df.as_ref().ok_or("No current DataFrame")?, lookback, frequency)?);
            // },
            _ => return Err(format!("Unknown command: {}", command).into()),
        }
    }

    pub fn process_commands(&mut self, command_string: &str) -> Result<(), Box<dyn Error>> {
        // Step 1: Load initial DataFrame from stdin
        let mut current_df = self.load_from_stdin()?;

        // Step 2: Process the commands
        for cmd in command_string.split('|') {
            let parts: Vec<String> = cmd.trim().split_whitespace().map(String::from).collect();
            if let Some((command, args)) = parts.split_first() {
                current_df = self.execute_command(command, args, Some(current_df))?;
            }
        }
    
        // Step 3: Output the final DataFrame to stdout
        current_df.print();
    
        Ok(())
    }
    
    fn load_from_stdin(&self) -> Result<DataFrame, Box<dyn Error>> {
        DataFrame::from_stdin("column")
    }

}