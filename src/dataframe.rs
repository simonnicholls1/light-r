use memmap2::{Mmap, MmapMut};
use std::{fs::File, io, sync::Arc};
use core::f64;
use tempfile::tempfile;
use std::io::Read;

pub struct DataFrame {
    pub mmap: Arc<Mmap>,                     // Memory-mapped file
    pub num_rows: usize,                // Number of rows in the dataset
    pub num_columns: usize,             // Number of columns in the dataset
    pub column_names: Vec<String>,      // Names of the columns
    pub row_names: Vec<String>,         // Names of the rows (e.g., dates)
    pub row_or_column: String,          // Either "row" or "column"
    pub offsets: Vec<Vec<usize>>,       // Byte offsets for column-wise access
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
            mmap: Arc::new(mmap_data.make_read_only()?),
            num_rows,
            num_columns,
            column_names,
            row_names,
            row_or_column: row_or_column.to_string(),
            offsets: offsets,
        })
    }

     /// Create a memory-mapped DataFrame from stdin
     pub fn from_stdin(row_or_column: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let stdin = io::stdin();
        let mut buffer = String::new();

        stdin.lock().read_to_string(&mut buffer)?;
        if buffer.trim().is_empty() {
            return Err("Error: No input provided via stdin.".into());
        }

        // Parse headers
        let mut rdr = csv::Reader::from_reader(buffer.as_bytes());
        let headers = rdr.headers()?.clone();
        let mut column_names: Vec<String> = headers.iter().map(String::from).collect();
        if column_names[0] != "DATE" {
            return Err("Error: First column must be 'DATE'.".into());
        }
        column_names.remove(0); // Remove "DATE" column

        let num_columns = column_names.len();
        let mut row_names = Vec::new();
        let mut num_rows = 0;

        // First pass to count rows
        for result in rdr.records() {
            let record = result?;
            row_names.push(record[0].to_string()); // Store the date
            num_rows += 1;
        }

        // Calculate the required file size
        let file_size = num_rows * num_columns * 8;

        // Create a temporary file
        let tmpfile = tempfile()?;
        tmpfile.set_len(file_size as u64)?; // Set the file size

        // Memory-map the file
        let mut mmap = unsafe { MmapMut::map_mut(&tmpfile)? };

        // Second pass to write data directly to memory map
        rdr = csv::Reader::from_reader(buffer.as_bytes());
        for (row_index, result) in rdr.records().enumerate() {
            let record = result?;
            for (col_index, value) in record.iter().skip(1).enumerate() {
                let parsed_value = if value.trim().is_empty() {
                    f64::NAN
                } else {
                    value.parse::<f64>().unwrap_or(f64::NAN)
                };

                let offset = (row_index * num_columns + col_index) * 8;
                mmap[offset..offset + 8].copy_from_slice(&parsed_value.to_le_bytes());
            }
        }

        // Remap to column-major order if needed
        let remapped_mmap = if row_or_column == "column" {
            Self::remap_to_column_major(&mmap, num_rows, num_columns)?
        } else {
            mmap
        };

        // Calculate offsets
        let offsets = if row_or_column == "row" {
            Self::calc_row_offsets(num_rows, num_columns)
        } else {
            Self::calc_column_offsets(num_rows, num_columns)
        };

        Ok(Self {
            mmap: Arc::new(remapped_mmap.make_read_only()?),
            num_rows,
            num_columns,
            column_names,
            row_names,
            row_or_column: row_or_column.to_string(),
            offsets,
        })
    }

    /// Calculate row-major offsets
    pub fn calc_row_offsets(num_rows: usize, num_columns: usize) -> Vec<Vec<usize>> {
        (0..num_rows)
            .map(|row_index| {
                (0..num_columns)
                    .map(|col_index| (row_index * num_columns + col_index) * 8)
                    .collect()
            })
            .collect()
    }

    /// Calculate col-major offsets
    pub fn calc_column_offsets(num_rows: usize, num_columns: usize) -> Vec<Vec<usize>> {
        (0..num_columns)
            .map(|col_index| {
                (0..num_rows)
                    .map(|row_index| (row_index * 8) + (col_index * num_rows * 8))
                    .collect()
            })
            .collect()
    }

    fn remap_to_column_major(
        mmap: &MmapMut,
        num_rows: usize,
        num_columns: usize,
    ) -> Result<MmapMut, Box<dyn std::error::Error>> {
        let data_size = num_rows * num_columns * 8;
        let mut column_major_data = vec![0u8; data_size];
    
        for col_index in 0..num_columns {
            for row_index in 0..num_rows {
                let row_major_offset = (row_index * num_columns + col_index) * 8;
                let column_major_offset = (col_index * num_rows + row_index) * 8;
                column_major_data[column_major_offset..column_major_offset + 8]
                    .copy_from_slice(&mmap[row_major_offset..row_major_offset + 8]);
            }
        }
    
        let mut remapped_mmap = MmapMut::map_anon(data_size)?;
        remapped_mmap.copy_from_slice(&column_major_data);
    
        Ok(remapped_mmap)
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
    
        // Write the header row
        let mut header = self.column_names.clone();
        header.insert(0, "DATE".to_string());
        if let Err(err) = wtr.write_record(&header) {
            eprintln!("Error writing header: {}", err);
            return;
        }
    
        if self.row_or_column == "row" {
            // Process row-major format
            for row_index in 0..self.num_rows {
                let mut record = vec![self.row_names[row_index].clone()];
                record.extend((0..self.num_columns).map(|col_index| {
                    let offset = self.offsets[row_index][col_index];
                    let bytes = &self.mmap[offset..offset + 8];
                    let value = f64::from_le_bytes(bytes.try_into().unwrap_or_default());
                    value.to_string()
                }));
    
                if let Err(err) = wtr.write_record(&record) {
                    eprintln!("Error writing record for row {}: {}", row_index, err);
                    return;
                }
            }
        } else if self.row_or_column == "column" {
            // Process column-major format
            for row_index in 0..self.num_rows {
                let mut record = vec![self.row_names[row_index].clone()];
                record.extend((0..self.num_columns).map(|col_index| {
                    let offset = self.offsets[col_index][row_index];
                    let bytes = &self.mmap[offset..offset + 8];
                    let value = f64::from_le_bytes(bytes.try_into().unwrap_or_default());
                    value.to_string()
                }));
    
                if let Err(err) = wtr.write_record(&record) {
                    eprintln!("Error writing record for row {}: {}", row_index, err);
                    return;
                }
            }
        } else {
            eprintln!("Error: Unknown row_or_column format.");
            return;
        }
    
        if let Err(err) = wtr.flush() {
            eprintln!("Error flushing CSV writer: {}", err);
        }
    }

}
