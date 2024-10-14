use std::collections::HashMap;
use std::error::Error;
use chrono::NaiveDate;
use crate::dataframe::DataFrame;
use crate::operations::*;

pub struct Engine {
    vars: HashMap<String, DataFrame>,
    current_df: Option<DataFrame>,
}

impl Engine {
    pub fn new() -> Self {
        Engine {
            vars: HashMap::new(),
            current_df: None,
        }
    }

    pub fn execute_command(&mut self, command: &str, args: &[String]) -> Result<(), Box<dyn Error>> {
        let df = self.current_df.as_mut().ok_or("No current DataFrame")?;

        self.current_df = Some(match command {
            "after" => after::main(df, &args[0])?,
            "before" => before::main(df, &args[0])?,
            "cgrep" => cgrep::main(df, args)?,
            "signal" => signal::main(df)?,
            "dlog" => dlog::main(df)?,
            "unitscale" => {
                let window_size: usize = args[0].parse()?;
                let target_vol: f64 = args[1].parse()?;
                vol_scale::main(df, window_size, target_vol)?
            },
            "mult" => {
                let df2 = self.vars.get(&args[0]).ok_or("Variable not found")?;
                multiply::main(df, df2)?
            },
            "load" => load::main(&args[0])?,
            "save" => {
                save::main(df, &args[0])?;
                df.clone()
            },
            "->" => {
                self.vars.insert(args[0].clone(), df.clone());
                df.clone()
            },
            "ffill" => ffill::main(df),
            "ewa" => ewa::main(df)?,
            "cumsum" => {
                let start_number: f64 = args[0].parse()?;
                cumsum::main(df, start_number)?
            },
            "shift" => {
                let period: i32 = args[0].parse()?;
                shift::main(df, period)?
            },
            "plot" => {
                plot::main(df)?;
                df.clone()
            },
            "momentum" => {
                let lookback: usize = args[0].parse()?;
                let frequency: usize = args[1].parse()?;
                momentum::main(df, lookback, frequency)?
            },
            _ => return Err(format!("Unknown command: {}", command).into()),
        });

        Ok(())
    }

    pub fn process_commands(&mut self, command_string: &str) -> Result<(), Box<dyn Error>> {
        self.initial_load()?;
        
        for cmd in command_string.split('|') {
            let parts: Vec<String> = cmd.trim().split_whitespace().map(String::from).collect();
            if let Some((command, args)) = parts.split_first() {
                self.execute_command(command, args)?;
            }
        }

        if let Some(df) = &self.current_df {
            self.output_csv(df)?;
        }

        Ok(())
    }

    fn initial_load(&mut self) -> Result<(), Box<dyn Error>> {
        let mut rdr = csv::Reader::from_reader(std::io::stdin());
        let headers = rdr.headers()?.clone();
        
        let mut dates = Vec::new();
        let mut data = Vec::new();
        
        for result in rdr.records() {
            let record = result?;
            let date = NaiveDate::parse_from_str(&record[0], "%Y-%m-%d")?;
            dates.push(date);
            
            let row: Vec<f64> = record.iter().skip(1)
                .map(|val| val.parse().unwrap_or(f64::NAN))
                .collect();
            data.push(row);
        }

        let column_names: Vec<String> = headers.iter().skip(1).map(String::from).collect();

        self.current_df = Some(DataFrame { dates, data, column_names });

        Ok(())
    }

    fn output_csv(&self, df: &DataFrame) -> Result<(), Box<dyn Error>> {
        let mut wtr = csv::Writer::from_writer(std::io::stdout());

        let mut header = vec!["DATE".to_string()];
        header.extend(df.column_names.clone());
        wtr.write_record(&header)?;

        for (date, row) in df.dates.iter().zip(df.data.iter()) {
            let mut record = vec![date.format("%Y-%m-%d").to_string()];
            record.extend(row.iter().map(|&x| x.to_string()));
            wtr.write_record(&record)?;
        }

        wtr.flush()?;
        Ok(())
    }
}
