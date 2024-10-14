use crate::DataFrame;
use chrono::NaiveDate;
use csv::Reader;
use std::fs::File;
use std::error::Error;

pub fn load(filename: &str) -> Result<DataFrame, Box<dyn Error>> {
    let file = File::open(filename)?;
    let mut rdr = Reader::from_reader(file);
    
    let headers = rdr.headers()?.clone();
    let date_index = headers.iter().position(|h| h == "DATE")
        .ok_or("DATE column not found")?;

    let mut dates = Vec::new();
    let mut data = Vec::new();

    for result in rdr.records() {
        let record = result?;
        let date = NaiveDate::parse_from_str(&record[date_index], "%Y-%m-%d")?;
        dates.push(date);

        let row: Vec<f64> = record.iter()
            .enumerate()
            .filter(|&(i, _)| i != date_index)
            .map(|(_, val)| val.parse::<f64>().unwrap_or(f64::NAN))
            .collect();

        data.push(row);
    }

    let column_names: Vec<String> = headers.iter()
        .enumerate()
        .filter(|&(i, _)| i != date_index)
        .map(|(_, name)| name.to_string())
        .collect();

    Ok(DataFrame { dates, data, column_names })
}

pub fn main(filename: &str) -> Result<DataFrame, Box<dyn Error>> {
    load(filename)
}
