use std::error::Error;
use std::time::Instant;
use crate::dataframe::DataFrame;
use crate::operations::dlog::{self, dlog_block};
use std::sync::{Arc, Mutex};
use std::thread;
use std::sync::mpsc;
use memmap2::MmapMut;
use tempfile::tempfile;

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

            "dlog_multithread" => {
                let input_df = df.ok_or("No current DataFrame to process for dlog")?;
                let df = Engine::parallel_process(&input_df, Arc::new(dlog_block), 4)?;
                return Ok(df)
            }

            "print" => {
                let input_df = df.ok_or("No current DataFrame to process for dlog")?;
                input_df.print();
                return Ok(input_df)
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
        // Measure execution time
        let start = Instant::now();

        // Step 1: Load initial DataFrame from stdin
        let mut current_df = self.load_from_stdin()?;

        // Step 2: Process the commands
        for cmd in command_string.split('|') {
            let parts: Vec<String> = cmd.trim().split_whitespace().map(String::from).collect();
            if let Some((command, args)) = parts.split_first() {
                current_df = self.execute_command(command, args, Some(current_df))?;
            }
        }

        let duration = start.elapsed();
    
        // Step 3: Output the final DataFrame to stdout
        if current_df.num_rows < 300000
        {current_df.print();}
        else {println!("Skipping printing as above 200k rows")}

        println!("Time taken to calc: {:?}", duration);
        println!("No rows: {:?}", current_df.num_rows);
        println!("No Assets: {:?}", current_df.num_columns);
    
        Ok(())
    }
    
    fn load_from_stdin(&self) -> Result<DataFrame, Box<dyn Error>> {
        DataFrame::from_stdin("column")
    }

   
    pub fn parallel_process<F>(
        input_df: &DataFrame,
        operation: Arc<F>,
        max_threads: usize,
    ) -> Result<DataFrame, Box<dyn Error>>
    where
        F: Fn(&[u8], &mut [u8], &[usize]) + Send + Sync + 'static,
    {
        let num_blocks = input_df.offsets.len();
    
        // Create an output memory map
        let output_size = input_df.num_rows * input_df.num_columns * 8;
        let tmpfile = tempfile()?;
        tmpfile.set_len(output_size as u64)?;
        let output_mmap = unsafe { MmapMut::map_mut(&tmpfile)? };
        let output_mmap = Arc::new(Mutex::new(output_mmap));
    
        // Share input memory map
        let input_mmap = Arc::new(input_df.mmap.clone());
    
        // Create a channel to distribute work
        let (tx, rx) = mpsc::channel();
    
        // Send all blocks of offsets to the channel
        for block_offsets in &input_df.offsets {
            tx.send(block_offsets.clone()).unwrap();
        }
        drop(tx); // Close the sender to signal no more work
    
        // Wrap the receiver in Arc<Mutex> to share among threads
        let rx = Arc::new(Mutex::new(rx));
    
        // Worker pool
        let mut handles = vec![];
        for _ in 0..max_threads.min(num_blocks) {
            let input_mmap = Arc::clone(&input_mmap);
            let output_mmap = Arc::clone(&output_mmap);
            let rx = Arc::clone(&rx);
            let operation = Arc::clone(&operation);
    
            let handle = thread::spawn(move || {
                while let Ok(block_offsets) = rx.lock().unwrap().recv() {
                    let mut output_lock = output_mmap.lock().unwrap();
    
                    // Pass the block of offsets to the operation
                    operation(
                        &input_mmap[..],                  // Entire input mmap
                        &mut output_lock[..],             // Entire output mmap
                        &block_offsets[..],               // Current block of offsets
                    );
                }
            });
    
            handles.push(handle);
        }
    
        // Wait for all threads to finish
        for handle in handles {
            handle.join().unwrap();
        }
    
        // Unwrap the output memory map
        let output_mmap = Arc::try_unwrap(output_mmap)
            .expect("Failed to unwrap Arc")
            .into_inner()
            .expect("Failed to lock Mutex");
    
        Ok(DataFrame {
            mmap: Arc::new(output_mmap.make_read_only()?),
            num_rows: input_df.num_rows,
            num_columns: input_df.num_columns,
            column_names: input_df.column_names.clone(),
            row_names: input_df.row_names.clone(),
            row_or_column: input_df.row_or_column.clone(),
            offsets: input_df.offsets.clone(),
        })
    }
    
}