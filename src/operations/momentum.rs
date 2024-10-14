use crate::DataFrame;
use std::error::Error;

pub fn calculate_momentum(df: &DataFrame, lookback_period: usize, frequency: usize) -> Result<DataFrame, Box<dyn Error>> {
    if df.data.is_empty() {
        return Err("No data available for momentum calculation.".into());
    }

    let frequency = frequency.max(1);
    let mut momentum_data = Vec::new();
    let mut momentum_dates = Vec::new();

    for i in (lookback_period..df.data.len()).step_by(frequency) {
        let mut row = Vec::with_capacity(df.data[0].len());
        for j in 0..df.data[0].len() {
            let current = df.data[i][j];
            let previous = df.data[i - lookback_period][j];
            let momentum = if previous != 0.0 {
                (current - previous) / previous
            } else {
                f64::NAN
            };
            row.push(momentum);
        }
        momentum_data.push(row);
        momentum_dates.push(df.dates[i]);
    }

    Ok(DataFrame {
        dates: momentum_dates,
        data: momentum_data,
        column_names: df.column_names.clone(),
    })
}

pub fn main(df: &DataFrame, lookback: usize, frequency: usize) -> Result<DataFrame, Box<dyn Error>> {
    if df.data.is_empty() {
        return Err("No data available for momentum calculation.".into());
    }
    calculate_momentum(df, lookback, frequency)
}
