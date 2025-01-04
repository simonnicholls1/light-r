use std::sync::Arc;

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
    for block_no in 0..input_df.offsets.len() {
        for block_idx in 1..input_df.offsets[block_no].len() {
            let prev_offset = input_df.offsets[block_no][block_idx-1];
            let curr_offset = input_df.offsets[block_no][block_idx];

            let prev_value = f64::from_le_bytes(input_df.mmap[prev_offset..prev_offset + 8].try_into()?);
            let curr_value = f64::from_le_bytes(input_df.mmap[curr_offset..curr_offset + 8].try_into()?);

            let log_return = if prev_value > 0.0 && curr_value > 0.0 {
                (curr_value / prev_value).ln()
            } else {
                f64::NAN
            };

            //let output_offset = row_index * num_columns * 8 + col_index * 8;
            mmap_out[curr_offset..curr_offset + 8].copy_from_slice(&log_return.to_le_bytes());
        }

        // Fill first column with NaN (no previous value for log return)
        let first_offset = block_no * num_columns * 8;
        mmap_out[first_offset..first_offset + 8].copy_from_slice(&f64::NAN.to_le_bytes());
    }
    Ok(DataFrame {
        mmap: Arc::new(mmap_out.make_read_only()?),
        num_rows,
        num_columns,
        column_names,
        row_names,
        row_or_column: input_df.row_or_column.clone(),
        offsets: input_df.offsets.clone(),
    })
}

pub fn dlog_block(input: &[u8], output: &mut [u8], offsets: &[usize]) {
    for i in 1..offsets.len() {
        let prev_offset = offsets[i - 1];
        let curr_offset = offsets[i];

        // Read previous and current values
        let prev_value = f64::from_le_bytes(input[prev_offset..prev_offset + 8].try_into().unwrap());
        let curr_value = f64::from_le_bytes(input[curr_offset..curr_offset + 8].try_into().unwrap());

        // Compute log return
        let log_return = if prev_value > 0.0 && curr_value > 0.0 {
            (curr_value / prev_value).ln()
        } else {
            f64::NAN
        };

        // Write the result to the output memory map
        output[curr_offset..curr_offset + 8].copy_from_slice(&log_return.to_le_bytes());
    }

    // Set the first value in the block to NaN
    let first_offset = offsets[0];
    output[first_offset..first_offset + 8].copy_from_slice(&f64::NAN.to_le_bytes());
}
