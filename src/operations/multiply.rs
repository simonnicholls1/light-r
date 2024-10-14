use crate::DataFrame;
use std::error::Error;

pub fn multiply(df1: &DataFrame, df2: &DataFrame) -> Result<DataFrame, Box<dyn Error>> {
    if df1.dates != df2.dates {
        return Err("DataFrames must have the same dates".into());
    }
    if df1.data[0].len() != df2.data[0].len() {
        return Err("DataFrames must have the same number of columns".into());
    }

    let mut result_data = Vec::with_capacity(df1.data.len());
    for (row1, row2) in df1.data.iter().zip(df2.data.iter()) {
        let mut result_row = Vec::with_capacity(row1.len());
        for (val1, val2) in row1.iter().zip(row2.iter()) {
            result_row.push(val1 * val2);
        }
        result_data.push(result_row);
    }

    Ok(DataFrame {
        dates: df1.dates.clone(),
        data: result_data,
        column_names: df1.column_names.clone(), // Assuming column names remain the same
    })
}

pub fn main(df1: &DataFrame, df2: &DataFrame) -> Result<DataFrame, Box<dyn Error>> {
    multiply(df1, df2)
}
