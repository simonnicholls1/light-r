use crate::DataFrame;

pub fn calculate_log_returns(df: &DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
    if df.data.is_empty() {
        return Err("No data available for log return calculation.".into());
    }

    let mut log_returns = DataFrame {
        dates: df.dates[1..].to_vec(),
        data: Vec::with_capacity(df.data.len() - 1),
        column_names: df.column_names.clone()
    };

    for i in 1..df.data.len() {
        let mut row = Vec::with_capacity(df.data[i].len());
        for j in 0..df.data[i].len() {
            if df.data[i][j] <= 0.0 || df.data[i-1][j] <= 0.0 {
                return Err("Non-positive values encountered. Log returns cannot be calculated.".into());
            }
            let log_return = (df.data[i][j] / df.data[i-1][j]).ln();
            row.push(log_return);
        }
        log_returns.data.push(row);
    }

    Ok(log_returns)
}

pub fn main(df: &DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
    calculate_log_returns(df)
}
