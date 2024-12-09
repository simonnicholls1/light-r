use memmap2::{Mmap, MmapMut};
use std::{error::Error, fs::File};
use core::f64;
use std::io::{self, Read};

pub struct DataFrame {
    pub mmap: Mmap,                     // Memory-mapped file
    pub num_rows: usize,                // Number of rows in the dataset
    pub num_columns: usize,             // Number of columns in the dataset
    pub column_names: Vec<String>,      // Names of the columns
    pub row_names: Vec<String>,         // Names of the rows (e.g., dates)
    pub row_or_column: String,          // Either "row" or "column"
    pub offsets: Vec<Vec<usize>>, // Byte offsets for column-wise access
}

impl DataFrame {
    /// Create a memory-mapped DataFrame from a CSV file
    pub fn new_from_csv(file_path: &str, row_or_column: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(file_path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        let mut num_rows = 0;
        let mut num_columns = 0;
        let mut column_names = Vec::new();
        let mut row_names = Vec::new(); // For storing dates or row identifiers
        let mut raw_data = Vec::new();

        let mut line_start = 0;

        for (i, &byte) in mmap.iter().enumerate() {
            if byte == b'\n' {
                let line = &mmap[line_start..i];

                if num_rows == 0 {
                    // Parse header row
                    let mut fields = line.split(|&b| b == b',');
                    let first_field = String::from_utf8_lossy(fields.next().unwrap()).to_string();

                    if first_field != "DATE" {
                        return Err("Error: First column must be 'DATE'.".into());
                    }

                    num_columns = fields.clone().count();
                    column_names = fields
                        .map(|col| String::from_utf8_lossy(col).to_string())
                        .collect();
                } else {
                    // Parse data rows
                    let mut fields = line.split(|&b| b == b',');
                    let date = String::from_utf8_lossy(fields.next().unwrap()).to_string();
                    row_names.push(date); // Store the "DATE" value

                    raw_data.extend(fields.map(|field| {
                        let field_str = String::from_utf8_lossy(field).to_string().trim().to_string();
                        if field_str.is_empty() {
                            f64::NAN
                        } else {
                            field_str.parse::<f64>().unwrap_or(f64::NAN)
                        }
                    }));
                }

                num_rows += 1;
                line_start = i + 1;
            }
        }

        if raw_data.len() != num_rows * num_columns {
            return Err("Error: Mismatched row or column count.".into());
        }

        let data_size = num_rows * num_columns * 8;
        let mut mmap_data = { MmapMut::map_anon(data_size)? };

        mmap_data.copy_from_slice(&raw_data.iter().flat_map(|v| v.to_le_bytes()).collect::<Vec<_>>());

        let offsets = Self::calc_offsets(num_rows, num_columns);

        Ok(Self {
            mmap: mmap_data.make_read_only()?,
            num_rows,
            num_columns,
            column_names,
            row_names,
            row_or_column: row_or_column.to_string(),
            offsets: offsets,
        })
    }

    /// Create a memory-mapped DataFrame from stdin
    pub fn from_stdin() -> Result<Self, Box<dyn std::error::Error>> {
        let stdin = io::stdin();
        let mut buffer = String::new();

        stdin.lock().read_to_string(&mut buffer)?;
        if buffer.trim().is_empty() {
            return Err("Error: No input provided via stdin.".into());
        }

        let mut rdr = csv::Reader::from_reader(buffer.as_bytes());
        let headers = rdr.headers()?.clone();

        let mut column_names: Vec<String> = headers.iter().map(String::from).collect();
        if column_names[0] != "DATE" {
            return Err("Error: First column must be 'DATE'.".into());
        }
        column_names.remove(0); // Remove the "DATE" column

        let mut num_rows = 0;
        let num_columns = column_names.len();
        let mut row_names = Vec::new();
        let mut raw_data = Vec::new();

        for result in rdr.records() {
            let record = result?;
            row_names.push(record[0].to_string()); // First column is the "DATE"
            raw_data.extend(record.iter().skip(1).map(|x| {
                if x.trim().is_empty() {
                    f64::NAN
                } else {
                    x.parse::<f64>().unwrap_or(f64::NAN)
                }
            }));
            num_rows += 1;
        }

        if raw_data.len() != num_rows * num_columns {
            return Err("Error: Mismatched row or column count.".into());
        }

        let data_size = num_rows * num_columns * 8;
        let mut mmap_data = { MmapMut::map_anon(data_size)? };

        mmap_data.copy_from_slice(&raw_data.iter().flat_map(|v| v.to_le_bytes()).collect::<Vec<_>>());

        let offsets = Self::calc_offsets(num_rows, num_columns);

        Ok(Self {
            mmap: mmap_data.make_read_only()?,
            num_rows,
            num_columns,
            column_names,
            row_names,
            row_or_column: "column".to_string(),
            offsets: offsets,
        })
    }

    /// Calculate byte offsets for each data element in each row
    pub fn calc_offsets(num_rows: usize, num_columns: usize) -> Vec<Vec<usize>> {
        let mut offsets = Vec::with_capacity(num_rows);

        // For each row, calculate the offsets for all columns
        for row in 0..num_rows {
            let mut row_offsets = Vec::with_capacity(num_columns);
            for col in 0..num_columns {
                let offset = ((row * 2)  + col) * 8; // 8 bytes per float value
                row_offsets.push(offset);
            }
            offsets.push(row_offsets);
        }

        offsets
    }

    pub fn print(&self) {
        let mut wtr = csv::Writer::from_writer(std::io::stdout());
    
        // Write the header row directly
        let mut header = self.column_names.clone();
        header.insert(0, "DATE".to_string());
        if let Err(err) = wtr.write_record(&header) {
            eprintln!("Error writing header: {}", err);
            return;
        }
    
        // Write each row directly
        for row_index in 0..self.num_rows {
            // Start with the date from `row_names`
            let mut record = vec![self.row_names[row_index].clone()];
    
            // Add the numeric data directly
            record.extend((0..self.num_columns).map(|col_index| {
                let offset = self.offsets[row_index][col_index];
                let bytes = &self.mmap[offset..offset + 8];
                let value = f64::from_le_bytes(bytes.try_into().unwrap_or_default());
                value.to_string()
            }));
    
            // Write the record directly to the CSV writer
            if let Err(err) = wtr.write_record(&record) {
                eprintln!("Error writing record for row {}: {}", row_index, err);
                return;
            }
        }
    
        if let Err(err) = wtr.flush() {
            eprintln!("Error flushing CSV writer: {}", err);
        }
    }


}
