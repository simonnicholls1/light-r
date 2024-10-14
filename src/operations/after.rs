use chrono::NaiveDate;
use crate::dataframe::DataFrame;

pub fn after_date(df: &DataFrame, date: NaiveDate) -> DataFrame {
    let start_index = df.dates.partition_point(|&d| d <= date);
    
    DataFrame {
        dates: df.dates[start_index..].to_vec(),
        data: df.data[start_index..].to_vec(),
        column_names: df.column_names.clone()
    }
}

pub fn main(df: &DataFrame, date: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let date = NaiveDate::parse_from_str(date, "%Y-%m-%d")?;
    Ok(after_date(df, date))
}
