use std::collections::HashMap;
use std::error::Error;
use chrono::NaiveDate;
use crate::dataframe::DataFrame;
use crate::operations::*;

pub struct EngineOld {
    vars: HashMap<String, DataFrame>,
    current_df: Option<DataFrame>,
}

impl EngineOld {
    pub fn new() -> Self {
        EngineOld {
            vars: HashMap::new(),
            current_df: None,
        }
    }

    pub fn execute_command(&mut self, command: &str, args: &[String]) -> Result<(), Box<dyn Error>> {
        match command {
            "after" => self.current_df = Some(after::main(self.current_df.as_ref().ok_or("No current DataFrame")?, &args[0])?),
            "before" => self.current_df = Some(before::main(self.current_df.as_ref().ok_or("No current DataFrame")?, &args[0])?),
            "cgrep" => self.current_df = Some(cgrep::main(self.current_df.as_ref().ok_or("No current DataFrame")?, args)?),
            "signal" => self.current_df = Some(signal::main(self.current_df.as_ref().ok_or("No current DataFrame")?)?),
            "dlog" => self.current_df = Some(dlog::main(self.current_df.as_ref().ok_or("No current DataFrame")?)?),
            "unitscale" => {
                let window_size: usize = args[0].parse()?;
                let target_vol: f64 = args[1].parse()?;
                self.current_df = Some(vol_scale::main(self.current_df.as_ref().ok_or("No current DataFrame")?, window_size, target_vol)?);
            },
            "mult" => {
                let df2 = self.vars.get(&args[0]).ok_or("Variable not found")?;
                self.current_df = Some(multiply::main(self.current_df.as_ref().ok_or("No current DataFrame")?, df2)?);
            },
            "load" => self.current_df = Some(self.load_csv(&args[0])?),
            "save" => {
                save::main(self.current_df.as_ref().ok_or("No current DataFrame")?, &args[0])?;
            },
            "->" => {
                self.vars.insert(args[0].clone(), self.current_df.clone().ok_or("No current DataFrame")?);
            },
            "ffill" => self.current_df = Some(ffill::main(self.current_df.as_ref().ok_or("No current DataFrame")?)),
            "ewa" => self.current_df = Some(ewa::main(self.current_df.as_ref().ok_or("No current DataFrame")?)?),
            "cumsum" => {
                let start_number: f64 = args[0].parse()?;
                self.current_df = Some(cumsum::main(self.current_df.as_mut().ok_or("No current DataFrame")?, start_number)?);
            },
            "shift" => {
                let period: i32 = args[0].parse()?;
                self.current_df = Some(shift::main(self.current_df.as_ref().ok_or("No current DataFrame")?, period)?);
            },
            "plot" => {
                plot::main(self.current_df.as_ref().ok_or("No current DataFrame")?)?;
            },
            "momentum" => {
                let lookback: usize = args[0].parse()?;
                let frequency: usize = args[1].parse()?;
                self.current_df = Some(momentum::main(self.current_df.as_ref().ok_or("No current DataFrame")?, lookback, frequency)?);
            },
            _ => return Err(format!("Unknown command: {}", command).into()),
        }
    
        Ok(())
    }

    pub fn process_commands(&mut self, command_string: &str) -> Result<(), Box<dyn Error>> {
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

    fn load_csv(&mut self, file_path: &str) -> Result<DataFrame, Box<dyn Error>> {
        let mut rdr = csv::Reader::from_path(file_path)?;
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

        Ok(DataFrame { dates, data, column_names })
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