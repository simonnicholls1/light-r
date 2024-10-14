use chrono::NaiveDate;
use crate::DataFrame;

pub fn before_date(df: &DataFrame, date: NaiveDate) -> DataFrame {
    let end_index = df.dates.partition_point(|&d| d < date);
    
    DataFrame {
        dates: df.dates[..end_index].to_vec(),
        data: df.data[..end_index].to_vec(),
        column_names: df.column_names.clone()
    }
}

pub fn main(df: &DataFrame, date: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let date = NaiveDate::parse_from_str(date, "%Y-%m-%d")?;
    Ok(before_date(df, date))
}
