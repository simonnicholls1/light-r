use crate::DataFrame;
use std::error::Error;

fn calculate_rolling_volatility(df: &DataFrame, window_size: usize) -> Result<DataFrame, Box<dyn Error>> {
    if df.data.is_empty() || df.data[0].is_empty() {
        return Err("DataFrame is empty".into());
    }

    let mut volatility_data = vec![vec![f64::NAN; df.data[0].len()]; df.data.len()];

    for col in 0..df.data[0].len() {
        for i in window_size-1..df.data.len() {
            let window = &df.data[i-window_size+1..=i];
            let mean: f64 = window.iter().map(|row| row[col]).sum::<f64>() / window_size as f64;
            let variance = window.iter()
                .map(|row| {
                    let diff = row[col] - mean;
                    diff * diff
                })
                .sum::<f64>() / window_size as f64;
            volatility_data[i][col] = (variance * window_size as f64).sqrt();
        }
    }

    Ok(DataFrame {
        dates: df.dates.clone(),
        data: volatility_data,
        column_names: df.column_names.clone(),
    })
}

fn vol_scale_factor(rolling_vol: &DataFrame, target_volatility: f64) -> Result<DataFrame, Box<dyn Error>> {
    let mut scaled_data = vec![vec![f64::NAN; rolling_vol.data[0].len()]; rolling_vol.data.len()];

    for (i, row) in rolling_vol.data.iter().enumerate() {
        for (j, &vol) in row.iter().enumerate() {
            if !vol.is_nan() && vol != 0.0 {
                scaled_data[i][j] = target_volatility / vol;
            }
        }
    }

    Ok(DataFrame {
        dates: rolling_vol.dates.clone(),
        data: scaled_data,
        column_names: rolling_vol.column_names.clone(),
    })
}

pub fn main(df: &DataFrame, window_size: usize, target_vol: f64) -> Result<DataFrame, Box<dyn Error>> {
    let rolling_vol = calculate_rolling_volatility(df, window_size)?;
    let scaled_vol_factors = vol_scale_factor(&rolling_vol, target_vol)?;

    let mut result_data = vec![vec![0.0; df.data[0].len()]; df.data.len()];
    for i in 0..df.data.len() {
        for j in 0..df.data[0].len() {
            result_data[i][j] = df.data[i][j] * scaled_vol_factors.data[i][j];
        }
    }

    Ok(DataFrame {
        dates: df.dates.clone(),
        data: result_data,
        column_names: df.column_names.clone(),
    })
}
