use memmap2::MmapMut;
use crate::DataFrame;

pub fn dlog(input_df: &DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // Output dimensions remain the same as input dimensions
    let num_rows = input_df.num_rows;
    let num_columns = input_df.num_columns;

    let column_names = input_df.column_names.clone();
    let row_names = input_df.row_names.clone();

    // Calculate size for the new memory map (8 bytes per float value)
    let output_size = num_rows * num_columns * 8;

    // Create an anonymous memory map for the output DataFrame
    let mut mmap_out = { MmapMut::map_anon(output_size)? };

    // Perform log return calculations
    if input_df.row_or_column == "row" {
        for row_index in 0..input_df.num_rows {
            for col_index in 1..input_df.num_columns {
                let prev_offset = (row_index * input_df.num_columns + (col_index - 1)) * 8;
                let curr_offset = (row_index * input_df.num_columns + col_index) * 8;

                let prev_value = f64::from_le_bytes(input_df.mmap[prev_offset..prev_offset + 8].try_into()?);
                let curr_value = f64::from_le_bytes(input_df.mmap[curr_offset..curr_offset + 8].try_into()?);

                let log_return = if prev_value > 0.0 && curr_value > 0.0 {
                    (curr_value / prev_value).ln()
                } else {
                    f64::NAN
                };

                let output_offset = row_index * num_columns * 8 + col_index * 8;
                mmap_out[output_offset..output_offset + 8].copy_from_slice(&log_return.to_le_bytes());
            }

            // Fill first column with NaN (no previous value for log return)
            let first_offset = row_index * num_columns * 8;
            mmap_out[first_offset..first_offset + 8].copy_from_slice(&f64::NAN.to_le_bytes());
        }
    } else if input_df.row_or_column == "column" {
        for col_index in 0..input_df.num_columns {
            for row_index in 1..input_df.num_rows {
                // Calculate offsets for the current and previous rows
                let prev_start_idx = input_df.offsets[row_index - 1][col_index];
                let curr_start_idx = input_df.offsets[row_index][col_index];

                // Extract values from the memory map
                let prev_value = f64::from_le_bytes(input_df.mmap[prev_start_idx..prev_start_idx + 8].try_into()?);
                let curr_value = f64::from_le_bytes(input_df.mmap[curr_start_idx..curr_start_idx + 8].try_into()?);

                let log_return = if prev_value > 0.0 && curr_value > 0.0 {
                    (curr_value / prev_value).ln()
                } else {
                    f64::NAN
                };

                let output_offset = row_index * num_columns * 8 + col_index * 8;
                mmap_out[output_offset..output_offset + 8].copy_from_slice(&log_return.to_le_bytes());
            }

            // Fill first row with NaN (no previous value for log return)
            let first_offset = col_index * 8;
            mmap_out[first_offset..first_offset + 8].copy_from_slice(&f64::NAN.to_le_bytes());
        }
    }

    Ok(DataFrame {
        mmap: mmap_out.make_read_only()?,
        num_rows,
        num_columns,
        column_names,
        row_names,
        row_or_column: input_df.row_or_column.clone(),
        offsets: input_df.offsets.clone(),
    })
}
