use crate::DataFrame;

pub fn equally_weighted_average(df: &DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
    if df.data.is_empty() {
        return Err("No data available for equally weighted average calculation.".into());
    }

    let mut result = DataFrame {
        dates: df.dates.clone(),
        data: vec![Vec::new(); df.dates.len()],
        column_names: df.column_names.clone()
    };

    for (i, row) in df.data.iter().enumerate() {
        let valid_values: Vec<f64> = row.iter().filter(|&&x| !x.is_nan()).cloned().collect();
        if !valid_values.is_empty() {
            let average = valid_values.iter().sum::<f64>() / valid_values.len() as f64;
            result.data[i] = vec![average];
        } else {
            result.data[i] = vec![f64::NAN];
        }
    }

    // Remove rows with NaN values
    let mut filtered_dates = Vec::new();
    let mut filtered_data = Vec::new();
    for (date, row) in result.dates.iter().zip(result.data.iter()) {
        if !row[0].is_nan() {
            filtered_dates.push(*date);
            filtered_data.push(row.clone());
        }
    }

    result.dates = filtered_dates;
    result.data = filtered_data;

    Ok(result)
}

pub fn main(df: &DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
    equally_weighted_average(df)
}
